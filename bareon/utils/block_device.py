#
# Copyright 2015 Cray Inc.  All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import functools
import itertools
import logging
import os
import re
import string

from bareon import errors
from bareon.utils import utils

LOG = logging.getLogger(__name__)


class BlockDevicePayload(object):
    def __init__(self, block, guid=None):
        self.block = block
        self.guid = guid

    @property
    def dev(self):
        return self.block.dev

    @property
    def size(self):
        return self.block.size

    @property
    def block_size(self):
        return self.block.block_size

    @property
    def physical_block_size(self):
        return self.block.physical_block_size

    @property
    def is_virtual(self):
        return self.block.is_virtual

    @property
    def is_bootable(self):
        return self.block.is_bootable


class Disk(BlockDevicePayload):
    model = None
    table = None

    @classmethod
    def new_by_device_scan(cls, dev):
        output = utils.execute('sgdisk', '--print', dev)[0]
        disk_info = _GDiskPrint(output)

        disk_block = _BlockDevice(
            dev, disk_info.sectors, disk_info.sector_size)
        disk_args = {'guid': disk_info.guid}
        if disk_info.table_format == 'gpt':
            disk_args['alignment'] = disk_info.align
            disk_args['sector_min'] = disk_info.sector_min
            disk_args['sector_max'] = disk_info.sector_max
        elif disk_info.table_format == 'mbr':
            disk_args['sector_min'] = 64
        disk = cls(disk_block, disk_info.table_format, **disk_args)

        for listing_info in disk_info.partitions:
            output = utils.execute(
                'sgdisk', '--info', '{}'.format(listing_info.index),
                disk_block.dev)[0]
            detailed_info = _GDiskInfo(output)

            dev = disk_block.device_by_index(listing_info.index)
            block = _BlockDevice(
                dev, detailed_info.size, disk_info.sector_size)

            partition = Partition(
                disk, block, detailed_info.begin,
                listing_info.index, listing_info.code,
                attributes=detailed_info.attributes, guid=detailed_info.guid)
            disk.register(partition)

        return disk

    def __init__(self, block, partition_table_format, sector_min=0,
                 sector_max=None, alignment=1, **kwargs):
        super(Disk, self).__init__(block, **kwargs)
        self.partition_table_format = partition_table_format
        self.alignment = alignment

        self._space = [_DiskSpaceDescriptor(0, self.block.size)]

        if sector_min:
            self._mark_reserved(0, max(sector_min - 1, 0))
        if sector_max is not None:
            self._mark_reserved(sector_max + 1, self.block.size)

        self._align_free_blocks()

    @property
    def partitions(self):
        """Iterate over existing partitions.

        Iterate over existing partitions (not free and not reserved segments)
        """
        for segment in self._space:
            if segment.is_free():
                continue
            elif segment.kind == segment.KIND_RESERVED:
                continue
            yield segment.payload

    @property
    def segments(self):
        """Iterate over segments.

        Iterate over all(except reserved) segments. Useful if someone is going
        to analise existing free segments.
        """
        for segment in self._space:
            if segment.kind == segment.KIND_RESERVED:
                continue
            if segment.is_free():
                yield EmptySpace(self, segment.begin, segment.end)
            else:
                yield segment.payload

    def allocate(self, size_bytes):
        """Allocate new patition on device.

        There is no any manipulation on real
        disk. It change only internal disk representation. Use first free block
        big enough to fit requested size. Raise BlockDeviceAllocationError if
        there is no suitable free block.

        :param size_bytes: required partition size in bytes
        :type size_bytes: int
        :return: created partition object
        :rtype: Partition
        """
        size = size_bytes // self.block_size
        size += int(size_bytes != size * self.block_size)

        for idx, segment in enumerate(self._space):
            if not segment.is_free():
                continue
            if segment.kind == segment.KIND_ALIGN:
                continue
            if segment.size < size:
                continue

            replace, tail = segment.split(segment.begin + size)

            block = _BlockDevice(
                None, replace.size, self.block_size, self.physical_block_size)
            partition = Partition(self, block, replace.begin, None, None)
            allocation = _DiskSpaceDescriptor(
                replace.begin, replace.end, _DiskSpaceDescriptor.KIND_BUSY,
                payload=partition)

            self._space[idx:idx + 1] = [
                x for x in (allocation, tail) if not x.is_null()]
            break
        else:
            raise errors.BlockDeviceAllocationError(
                'Unable to allocate {} sectors on {}'.format(size, self.dev))

        self._align_free_blocks()

        return partition

    def register(self, partition):
        """Mark corresponding sectors as busy by received partitions.

        Used during disk scan to build internal disk schema representation.

        :param partition: existing partition
        :type partition: Partition
        """
        allocation = self._reserve(
            partition.begin, partition.end, _DiskSpaceDescriptor.KIND_BUSY)
        allocation.payload = partition

    def _mark_reserved(self, begin, end):
        self._reserve(begin, end, _DiskSpaceDescriptor.KIND_RESERVED)

    def _reserve(self, begin, end, kind):
        intersect = []

        lookup = [begin, end]
        point = lookup.pop(0)
        for idx, segment in enumerate(self._space):
            try:
                while True:
                    if point < segment.begin:
                        break
                    if segment.end < point:
                        break

                    intersect.append(idx)
                    point = lookup.pop(0)
            except IndexError:
                break

        try:
            slice_start, slice_end = intersect
            slice_end += 1
        except ValueError:
            raise ValueError(
                'Provided range {}:{} lies outside disk '
                'boundaries'.format(begin, end))

        intersect = self._space[slice_start:slice_end]
        for segment in intersect:
            if segment.is_free():
                continue
            raise ValueError(
                'Failed to allocate rage {}:{} because of intersection with '
                'existing allocations'.format(begin, end))

        allocation = _DiskSpaceDescriptor(begin, end, kind)
        replace = [
            x for x in (
                intersect[0].split(begin)[0],
                allocation,
                intersect[-1].split(end + 1)[1]) if not x.is_null()
        ]

        self._space[slice_start:slice_end] = replace

        return allocation

    def _align_free_blocks(self):
        if self.alignment < 2:
            return

        replace_batch = []
        for idx, segment in enumerate(self._space):
            if segment.kind != segment.KIND_FREE:
                continue

            offset = self.alignment - segment.begin % self.alignment
            if offset == self.alignment:
                continue

            align, free = segment.split(segment.begin + offset)
            replace = [
                _DiskSpaceDescriptor(align.begin, align.end, align.KIND_ALIGN),
                free]
            replace_batch.append((idx, replace))

        replace_batch.reverse()
        for idx, replace in replace_batch:
            self._space[idx:idx + 1] = [x for x in replace if not x.is_null()]


class Partition(BlockDevicePayload):
    suffix_number = None

    def __init__(self, disk, block, begin, index, code, guid=None,
                 attributes=0):
        super(Partition, self).__init__(block, guid=guid)
        self.disk = disk
        self.begin = begin
        self.end = self.begin + self.block.size - 1  # -1 because ends included
        self.index = index
        self.code = code
        self.attributes = attributes

    @property
    def dev_suffix(self):
        name = str(self.index)

        if self.disk.is_virtual:
            return name
        if self.disk.dev[-1] in string.digits:
            return 'p' + name
        return name


class EmptySpace(object):
    def __init__(self, disk, begin, end):
        self.disk = disk
        self.begin = begin
        self.end = end
        self.size = (self.end - self.begin) + 1


class _BlockDevice(object):
    is_bootable = False

    def __init__(self, dev, size, block_size, physical_block_size=None):
        self.dev = dev
        self.size = size
        self.block_size = block_size
        if physical_block_size is None:
            physical_block_size = block_size
        self.physical_block_size = physical_block_size

        if not self.is_virtual:
            self.is_bootable = self._is_bootable_check()

    def device_by_index(self, index):
        if self.is_virtual:
            return None

        suffix = index
        if self.dev[-1] in string.digits:
            suffix = 'p{}'.format(suffix)

        dev = '{}{}'.format(self.dev, suffix)
        try:
            os.stat(dev)
        except OSError:
            raise ValueError(
                'Unable to determine device name for partition index '
                '{} on {}'.format(index, self.dev))

        return dev

    @property
    def is_virtual(self):
        return self.dev is None

    def _is_bootable_check(self):
        LOG.info('Collect disk structure for %s using file', self.dev)

        output = utils.execute(
            'file', '--brief', '--keep-going', '--special-files', self.dev)[0]
        LOG.debug('file output:')
        LOG.debug('%s', output)

        records = (x.strip() for x in output.split(';'))

        return any('boot sector' in x for x in records)


class _DiskSpaceDescriptor(object):
    _kind_idnr = itertools.count()
    KIND_FREE = next(_kind_idnr)
    KIND_RESERVED = next(_kind_idnr)
    KIND_ALIGN = next(_kind_idnr)
    KIND_BUSY = next(_kind_idnr)
    del _kind_idnr

    _kind_names = {
        KIND_FREE: 'FREE',
        KIND_RESERVED: 'RESERVED',
        KIND_ALIGN: 'ALIGN',
        KIND_BUSY: 'BUSY'
    }

    def __init__(self, begin, end, kind=KIND_FREE, payload=None):
        self.begin = begin
        self.end = end
        self.size = end - begin + 1
        self.kind = kind
        self.payload = payload

        self._anchor = (self.begin, self.end, self.kind)

    def __repr__(self):
        return '<{}({} {}:{})>'.format(
            type(self).__name__, self._kind_names[self.kind],
            self.begin, self.end)

    def __hash__(self):
        return hash(self._anchor)

    def __eq__(self, other):
        if not isinstance(other, _DiskSpaceDescriptor):
            return NotImplemented
        return self._anchor == other._anchor

    def __ne__(self, other):
        if not isinstance(other, _DiskSpaceDescriptor):
            return NotImplemented
        return self._anchor != other._anchor

    def is_free(self):
        return self.kind in (self.KIND_FREE, self.KIND_ALIGN)

    def is_null(self):
        return self.end + 1 == self.begin

    def split(self, boundary):
        if not self.is_free():
            raise TypeError('Unsplittable item {!r}'.format(self))

        cls = type(self)
        left = cls(self.begin, boundary - 1, self.kind, self.payload)
        right = cls(boundary, self.end, self.kind, self.payload)
        return left, right


class _GDiskMessage(object):
    _payload_field_match_rules = ()
    _payload_field_converts = {}
    _payload_mandatory_fields = frozenset()

    def __init__(self, data):
        self._raw = data

        data = data.strip()
        data = data.splitlines()

        self._notice = self._parse_notice(data)
        self._payload = self._parse_payload(data)
        self._payload = self._unpack_payload()
        self._extra = data

    def _parse_notice(self, data):
        notice = []

        for idx, line in enumerate(data):
            is_boundary = self._is_notice_boundary(line)
            if is_boundary:
                if not idx:
                    continue
                data[:idx + 1] = []
                self._ensure_boundary(data)
                break
            elif not idx:
                break
            notice.append(line.rstrip())

        notice = ' '.join(notice)
        if not notice:
            notice = None
        return notice

    def _parse_payload(self, data):
        idx = -1
        for idx, line in enumerate(data):
            if not self._is_boundary(line):
                continue
            break

        payload = data[:idx]
        data[:idx + 1] = []

        return payload

    def _unpack_payload(self):
        payload_fields = {}
        for line in self._payload:
            for match_rule in self._payload_field_match_rules:
                match = match_rule.search(line)
                if match is None:
                    continue

                payload_fields.update(match.groupdict())

        missing_fields = self._payload_mandatory_fields - set(payload_fields)
        if missing_fields:
            missing_fields = sorted(missing_fields)
            missing_fields = '", "'.join(missing_fields)
            raise errors.BlockDeviceSchemeError(
                'Required fields are missing: "{}"'.format(
                    missing_fields),
                self._raw)

        invalid_fields = {}
        for field in payload_fields:
            try:
                conv = self._payload_field_converts[field]
            except KeyError:
                continue

            value = payload_fields[field]
            try:
                payload_fields[field] = conv(payload_fields[field])
            except (ValueError, TypeError) as e:
                invalid_fields[field] = (e, value)

        if invalid_fields:
            message = []
            indent = ' ' * 4
            for field in sorted(invalid_fields):
                e, value = invalid_fields[field]
                message.append('{}{}({}): {}'.format(indent, field, value, e))
            message = '\n'.join(message)
            raise errors.BlockDeviceSchemeError(
                'Unable to convert parsed fields:\n{}'.format(message))

        return payload_fields

    def _ensure_boundary(self, data):
        if not data:
            return
        if self._is_boundary(data[0]):
            data.pop(0)
            return
        raise errors.BlockDeviceSchemeError(
            'Invalid sfdisk output - missing expected boundary("\n\n")',
            self._raw)

    @staticmethod
    def _is_boundary(line):
        return not line

    @staticmethod
    def _is_notice_boundary(line):
        if not line:
            return False

        if len(line) < 8:
            return False

        chars = set(line)
        if 1 < len(chars):
            return False

        return chars.pop() == '*'


class _GDiskPrint(_GDiskMessage):
    _payload_field_match_rules = (
        re.compile(r'^[Dd]isk\s+(?P<disk>/dev/[A-Za-z][A-Za-z0-9]+): '
                   r'(?P<sectors>\d+) sectors'),
        re.compile(r'^[Ll]ogical\s+sector\s+size:\s+(?P<sector_size>\d+)\s+'
                   r'bytes'),
        re.compile(r'^[Dd]isk\s+identifier\s+\(GUID\):\s+(?P<guid>\S+)$'),
        re.compile(r'^[Ff]irst\s+usable\s+sector\s+is\s+(?P<sector_min>\d+),'
                   r'\s*last\s+usable\s+sector\s+is\s+(?P<sector_max>\d+)'),
        re.compile(r'^[Pp]artitions\s+will\s+be\s+aligned\s+on\s*'
                   r'(?P<align>\d+)-sector\s+boundaries'),
        re.compile(r'^[Tt]otal\s+free\s+space\s+is\s+(?P<sectors_free>\d+)\s+'
                   r'sectors')
    )
    _payload_field_converts = {
        'sectors': int,
        'sector_size': int,
        'sector_min': int,
        'sector_max': int,
        'align': int,
        'sectors_free': int
    }
    _payload_mandatory_fields = frozenset((
        'disk', 'sectors', 'sector_size', 'guid'))

    _partitions_header_match = re.compile(
        r'^[Nn]umber\s+[Ss]tart\s*\(sector\)\s+[Ee]nd\s*\(sector\)\s+'
        r'[Ss]ize\s+[Cc]ode')

    # defaults
    disk = sectors = sector_size = guid = sector_max = sectors_free = None
    sector_min = 0
    align = 1

    def __init__(self, data):
        super(_GDiskPrint, self).__init__(data)

        table_format = 'gpt'
        if self._notice:
            # TODO(dbogun): investigate other possible convertions
            match = re.search(
                r'converting (MBR) to GPT format', self._notice, re.I)
            if match:
                table_format = match.group(1).lower()
        self.table_format = table_format

        self._payload.setdefault('sector_min', 0)
        self._payload.setdefault('sector_max', self._payload['sectors'])
        self._payload.setdefault('align', 1)
        self._payload.setdefault('sectors_free', None)
        for field, value in self._payload.items():
            setattr(self, field, value)

        self.partitions = self._unpack_partitions()

    def _unpack_partitions(self):
        try:
            line = self._extra.pop(0)
            match = self._partitions_header_match.search(line)
            if match is None:
                raise ValueError
        except (IndexError, ValueError):
            raise errors.BlockDeviceSchemeError(
                'Invalid partitions table header', self._raw)

        partitions = []
        while self._extra:
            try:
                record = _GDiskPrintPartitionRecord(self._extra.pop(0))
            except errors.BlockDeviceSchemeError as e:
                raise errors.BlockDeviceSchemeError(e.message, self._raw)
            partitions.append(record)

        return partitions


class _GDiskInfo(_GDiskMessage):
    _payload_field_match_rules = (
        re.compile(r'^[Pp]artition\s+unique\s+GUID:\s*(?P<guid>\S+)$'),
        re.compile(r'^[Ff]irst\s+sector:\s+(?P<begin>\d+)\s+\('),
        re.compile(r'^[Ll]ast\s+sector:\s*(?P<end>\d+) \('),
        re.compile(r'^[Pp]artition\s+size:\s*(?P<size>\d+)\s*sectors'),
        re.compile(r'^[Aa]ttribute\s+flags:\s*(?P<attributes>[0-9A-Fa-f]+)')
    )
    _payload_field_converts = {
        'begin': int,
        'end': int,
        'size': int,
        'attributes': functools.partial(int, base=16)
    }
    _payload_mandatory_fields = frozenset((
        'guid', 'begin', 'end', 'size', 'attributes'))

    # defaults
    guid = begin = end = size = attributes = None

    def __init__(self, data):
        super(_GDiskInfo, self).__init__(data)

        for field, value in self._payload.items():
            setattr(self, field, value)


class _GDiskPrintPartitionRecord(object):
    _fields = ('index', 'begin', 'end', None, None, 'code')
    _conv = {
        'index': int,
        'begin': int,
        'end': int,
        'code': functools.partial(int, base=16)
    }

    # defaults
    index = begin = end = code = None

    def __init__(self, line):
        record = line.split()
        record = dict(zip(self._fields, record))
        record.pop(None)

        invalid = {}
        for field in record:
            try:
                conv = self._conv[field]
            except KeyError:
                continue

            value = record[field]
            try:
                value = conv(value)
            except ValueError as e:
                invalid[field] = (e, value)

            setattr(self, field, value)

        if invalid:
            message = []
            indent = ' ' * 4
            for field in sorted(invalid):
                e, value = invalid[field]
                message.append('{}{}({}): {}'.format(
                    indent, field, value, e))
            message = '\n'.join(message)
            raise errors.BlockDeviceSchemeError(
                'Invalid partition table record\n"""{}"""\n{}'.format(
                    line, message))
