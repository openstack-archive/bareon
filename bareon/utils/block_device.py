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

import abc
import collections
import functools
import itertools
import logging
import os
import re
import string

import six

from bareon import errors
from bareon.utils import hardware
from bareon.utils import lvm
from bareon.utils import utils

LOG = logging.getLogger(__name__)


class SGDisk(object):
    def __init__(self, dev):
        self.dev = dev

    def zap(self):
        LOG.info('Erase block device "%s"', self.dev)
        utils.execute('sgdisk', '--zap-all', self.dev)

    def new(self, partition):
        LOG.info(
            'Create new partition %d:%d (0x%04x) on %s',
            partition.begin, partition.end, partition.code, self.dev)
        utils.execute(
            'sgdisk', '--new={}:{}:{}'.format(
                partition.index, partition.begin, partition.end), self.dev)
        utils.execute('sgdisk', '--typecode={}:{:04x}'.format(
            partition.index, partition.code), self.dev)
        if partition.index < 5:
            utils.execute('sgdisk', '--change-name={}:{}'.format(
                partition.index, 'primary'), self.dev)

        if partition.guid is not None:
            guid = partition.guid
            utils.execute('sgdisk', '--disk-guid={}'.format(guid))
        else:
            output = utils.execute(
                'sgdisk', '--info', '{}'.format(partition.index),
                self.dev)[0]
            guid = _SGDiskInfo(output).guid
        return guid


class DeviceFinder(object):
    def __init__(self):
        self.dev_list = []
        self.dev_by_name = {}
        self.dev_by_scsi = {}
        self.dev_by_path = {}

        disks = hardware.get_block_data_from_udev('disk')
        partitions = hardware.get_block_data_from_udev('partition')
        for dev in itertools.chain(disks, partitions):
            record = hardware.get_device_info(dev, False)
            if not record:
                continue
            self._parse(record)

    def _parse(self, record):
        dev = record['uspec']['DEVNAME']
        record['scsi'] = hardware.scsi_address(dev)

        self.dev_list.append(record)

        self.dev_by_name[dev] = record
        self.dev_by_name[self._cut_prefix(dev, '/dev/')] = record
        self.dev_by_path[dev] = record

        scsi_addr = record['scsi']
        if scsi_addr:
            self.dev_by_scsi[scsi_addr] = record

        for p in record['uspec'].get('DEVLINKS', ()):
            match_uuid = re.search(
                r'disk/by-(:?part)?uuid/(?P<uuid>[!/]+)$', p)
            if match_uuid:
                # force lowercase uuids
                uuid = match_uuid.groupdict()['uuid'].lower()
                p = list(p.rpartition('/'))
                p[-1] = uuid
                p = ''.join(p)

            self.dev_by_path[p] = record
            self.dev_by_path[self._cut_prefix(p, '/dev/')] = record

    def __call__(self, kind, needle):
        try:
            index = {
                'name': self.dev_by_name,
                'scsi': self.dev_by_scsi,
                'path': self.dev_by_path
            }[kind]
        except KeyError:
            raise errors.InternalError(
                'Incorrect "kind" argument: {!r}'.format(kind), exc_info=False)

        try:
            result = index[needle]
        except KeyError:
            raise errors.BlockDeviceNotFoundError(kind, needle)
        return result

    @staticmethod
    def _cut_prefix(subject, prefix):
        start, dummy, end = subject.partition(prefix)
        if start:
            return subject
        return end


# TODO(dbogun): this object can/should be removed
# The only reason why it exists - to keep "remaining" size. All other kinds can
# be stored into SizeUnit.
class SpaceClaim(utils.EqualComparisonMixin, object):
    _kind = itertools.count()
    KIND_EXACT = next(_kind)
    KIND_PERCENTAGE = next(_kind)
    KIND_BIGGEST = next(_kind)
    del _kind

    _kind_names = {
        KIND_EXACT: 'EXACT',
        KIND_PERCENTAGE: 'PERCENTAGE',
        KIND_BIGGEST: 'REMAINING'}

    @classmethod
    def new_biggest(cls):
        return cls(None, cls.KIND_BIGGEST)

    @classmethod
    def new_by_sizeunit(cls, size):
        if size.unit == '%':
            return cls.new_percent(size)
        return cls.new_exact(size)

    @classmethod
    def new_percent(cls, value):
        if not isinstance(value, SizeUnit):
            value = SizeUnit.new_by_string('{} %'.format(value))
        if value.unit != '%':
            raise TypeError('Unsuitable value for percentage space claim: '
                            '{!r}'.format(value))
        return cls(value, cls.KIND_PERCENTAGE)

    @classmethod
    def new_exact(cls, value):
        if not isinstance(value, SizeUnit):
            value = SizeUnit.new_by_string('{} B'.format(value))
        if value.bytes is None:
            raise TypeError('Unsuitable value for exact space claim: '
                            '{!r}'.format(value))
        return cls(value, cls.KIND_EXACT)

    def __init__(self, size, kind):
        self.size = size
        self.kind = kind

    def __call__(self, storage, from_tail=False):
        if self.kind == self.KIND_EXACT:
            segment = storage.allocate(self.size, from_tail=from_tail)
        elif self.kind == self.KIND_PERCENTAGE:
            usable_size = storage.usable_size
            usable_size = storage.blocks_to_sizeunit(usable_size)
            percent = usable_size.bytes // 100
            size = self.size.value_int * percent
            size = SizeUnit(size, 'B')
            segment = storage.allocate(size, from_tail=from_tail)
        elif self.kind == self.KIND_BIGGEST:
            usable_size = storage.calc_biggest_unallocated_chunk()
            segment = storage.allocate(usable_size, from_tail=from_tail)
        else:
            raise errors.InternalError(exc_info=False)

        return segment

    def __repr__(self):
        return '<{} {}:{!r}>'.format(
            type(self).__name__, self._kind_names[self.kind], self.size)


class SizeUnit(utils.EqualComparisonMixin, object):
    bytes = None

    _unit_multiplier = {
        '%': None,
        's': 512,
        'B': 1
    }

    m = 1
    for name in ('KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'):
        m *= 1000
        _unit_multiplier[name] = m
    m = 1
    for name in ('KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB'):
        m *= 1024
        _unit_multiplier[name] = m
    del m

    @classmethod
    def new_by_string(cls, raw, default_unit=None):
        suffix_by_length = cls._multiplier_suffixes_by_length()

        raw = raw.strip()
        match = ()
        for split_at in range(len(raw), -1, -1):
            value = raw[:split_at]
            value = value.rstrip()
            try:
                value = cls._value_to_number(value)
            except ValueError:
                continue

            suffix = raw[split_at:]
            try:
                suffix_candidate = suffix_by_length[len(suffix)]
            except KeyError:
                break

            if suffix not in suffix_candidate:
                continue

            match = value, suffix

        if not match:
            if default_unit is None:
                raise ValueError(
                    'Unable to parse size record {}: there is no '
                    'units'.format(raw))
            match = cls._value_to_number(raw), default_unit

        return cls(*match)

    @classmethod
    def new_by_bytes(cls, value_in_bytes, unit):
        multiplier = cls._get_multiplier(unit)
        value = value_in_bytes // multiplier
        if value_in_bytes != value * multiplier:
            value = value_in_bytes / float(multiplier)
        return cls(value, unit)

    @classmethod
    def _multiplier_suffixes_by_length(cls):
        suffix_by_length = {}
        suffixes = set(cls._unit_multiplier)
        for l in itertools.count(0):
            if not suffixes:
                break

            match = set()
            for s in suffixes:
                if len(s) != l:
                    continue
                match.add(s)
            suffix_by_length[l] = match
            suffixes -= match

        return suffix_by_length

    def __init__(self, value, unit):
        multiplier = self._get_multiplier(unit)

        self.unit = unit
        self.value = value
        self.value_int = int(value)
        if multiplier is not None:
            self.bytes = int(value * multiplier)

    def __repr__(self):
        value = str(self)
        if self.bytes is not None and self.unit != 'B':
            value = '{} == {} B'.format(value, self.bytes)
        return '<{}: {}>'.format(type(self).__name__, value)

    def __str__(self):
        if self.value == self.value_int:
            value = self.value_int
        else:
            value = self.value
        return '{} {}'.format(value, self.unit)

    def in_unit(self, unit):
        multiplier = self._get_multiplier(unit)
        if self.bytes is None or multiplier is None:
            raise ValueError('{} can\'t be converted in unit {}'.format(
                self, unit))
        return type(self).new_by_bytes(self.bytes, unit)

    @classmethod
    def _get_multiplier(cls, unit):
        try:
            multiplier = cls._unit_multiplier[unit]
        except KeyError:
            raise ValueError(
                'Invalid size unit: {}'.format(unit))
        return multiplier

    @classmethod
    def _value_to_number(cls, value):
        for conv in (int, float):
            try:
                value = conv(value)
            except ValueError:
                continue
            break
        else:
            raise ValueError(
                'Illegal input for {}.value field: {!r}'.format(cls, value))
        return value

    @classmethod
    def _comparable_shape(cls, target):
        value = super(SizeUnit, cls)._comparable_shape(target)
        if target.bytes is not None:
            fields = ('bytes', )
        else:
            fields = ('value', 'value_int', 'unit')
        value['payload'] = {
            k: v for k, v in value['payload'].items()
            if k in fields}
        return value


class FuzzyMatchSize(object):
    def __init__(self, factor, size):
        self.factor = factor
        self.size = size

    def __eq__(self, other):
        if not isinstance(other, FuzzyMatchSize):
            return NotImplemented
        return self.size - self.factor < other.size < self.size + self.factor

    def __ne__(self, other):
        return not self.__eq__(other)


@six.add_metaclass(abc.ABCMeta)
class AbstractStorage(object):
    block_size = 1
    usable_size = 0
    allocate_accuracy = SizeUnit(0, 'B')

    @abc.abstractmethod
    def calc_biggest_unallocated_chunk(self):
        pass

    @abc.abstractmethod
    def allocate(self, size, from_tail=False):
        pass

    def blocks_to_sizeunit(self, value):
        return SizeUnit(value * self.block_size, 'B')

    def sizeunit_to_blocks(self, value):
        blocks = value.bytes // self.block_size
        blocks += int(value.bytes != blocks * self.block_size)
        return blocks

    def handle_is_service(self, segment):
        usable_size = self.usable_size - segment.size
        self.usable_size = max(usable_size, 0)


class AbstractSegment(object):
    _kind = itertools.count()
    KIND_FREE = next(_kind)
    KIND_RESERVED = next(_kind)
    KIND_ALIGN = next(_kind)
    KIND_BUSY = next(_kind)
    del _kind

    _kind_names = {
        KIND_FREE: 'FREE',
        KIND_RESERVED: 'RESERVED',
        KIND_ALIGN: 'ALIGN',
        KIND_BUSY: 'BUSY'
    }

    is_service = False
    fuzzy_cmp_factor = False

    def __init__(self, owner, kind, size, payload=None):
        self.owner = owner
        self.kind = kind
        self.size = size
        self.payload = payload

        self._cmp_repr = self._make_cmp_repr()

    def set_is_service(self):
        if self.is_service:
            return

        self.is_service = True
        self.owner.handle_is_service(self)

    def set_fuzzy_cmp_factor(self, value):
        self.fuzzy_cmp_factor = value
        self._cmp_repr = self._make_cmp_repr()

    def is_free(self):
        return self.kind in (self.KIND_FREE, self.KIND_ALIGN)

    def _make_cmp_repr(self):
        return FuzzyMatchSize(self.fuzzy_cmp_factor, self.size), self.kind

    def __repr__(self):
        return '<{}({} {})>'.format(
            type(self).__name__, self._kind_names[self.kind],
            self.size)

    def __eq__(self, other):
        if not isinstance(other, AbstractSegment):
            return NotImplemented
        return self._cmp_repr == other._cmp_repr

    def __ne__(self, other):
        if not isinstance(other, AbstractSegment):
            return NotImplemented
        return self._cmp_repr != other._cmp_repr


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


class Disk(BlockDevicePayload, AbstractStorage):
    model = None
    table = None

    @classmethod
    def new_by_scan(cls, dev, partitions=True):
        output = utils.execute('sgdisk', '--print', dev)[0]
        disk_info = _SGDiskPrint(output)

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

        if not partitions:
            return disk

        for listing_info in disk_info.partitions:
            output = utils.execute(
                'sgdisk', '--info', '{}'.format(listing_info.index),
                disk_block.dev)[0]
            detailed_info = _SGDiskInfo(output)

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

        begin, end = 0, self.block.size - 1
        self._space = [DiskSegment(self, begin, end)]
        self.usable_size = self._space[0].size

        if sector_min:
            sector_min = max(sector_min - 1, begin)
            if begin <= sector_min:
                self._mark_reserved(begin, sector_min)
        if sector_max is not None:
            sector_max = min(sector_max + 1, end)
            if sector_max <= end:
                self._mark_reserved(sector_max, end)

        self._align_free_blocks()

    @property
    def segments(self):
        """Iterate over all(except reserved) segments

        Useful if someone is going to analise existing free segments.
        """
        for segment in self._space:
            if segment.kind == segment.KIND_RESERVED:
                continue
            yield segment

    def calc_biggest_unallocated_chunk(self):
        best_segment = None
        for segment in self._space:
            if segment.kind != segment.KIND_FREE:
                continue
            if best_segment is None:
                best_segment = segment
                continue

            if segment.size <= best_segment.size:
                continue

            best_segment = segment

        if best_segment is None:
            raise errors.BlockDeviceAllocationError(
                'There is no free segments on {}'.format(self))

        return self.blocks_to_sizeunit(best_segment.size)

    def allocate(self, size, from_tail=False):
        """Allocate new partition on device.

        There is no any manipulation on real disk. It change only internal disk
        representation. Use first free block big enough to fit requested size.
        Raise BlockDeviceAllocationError if there is no suitable free block.

        :param size: required partition size in bytes
        :type size: SizeUnit
        :param from_tail: allocate from begin or from end of disk
        :type from_tail: bool
        :return: created partition object
        :rtype: Partition
        """

        accuracy = self.sizeunit_to_blocks(self.allocate_accuracy)
        claim = self.sizeunit_to_blocks(size)
        claim_min = max(claim - accuracy, 1)

        space_sequence = enumerate(self._space)
        if from_tail:
            space_sequence = list(space_sequence)
            space_sequence.reverse()

        best_match = None
        best_shortage = None
        for idx, segment in space_sequence:
            if not segment.is_free():
                continue
            if segment.kind == segment.KIND_ALIGN:
                continue
            if segment.size < claim_min:
                continue

            shortage = max(claim - segment.size, 0)
            if best_shortage is None:
                best_match = idx
                best_shortage = shortage
            elif shortage < best_shortage:
                best_match = idx
                best_shortage = shortage

            if not shortage:
                break

        if best_match is None:
            raise errors.BlockDeviceAllocationError(
                'Unable to allocate {} sectors on {}'.format(claim, self.dev))

        segment = self._space[best_match]

        if from_tail:
            split_point = segment.end - claim + 1
            split_point = self._prev_aligned_block(split_point)
            split_point = max(split_point, segment.begin)
        else:
            split_point = segment.begin + claim
            split_point = self._next_aligned_block(split_point)
            split_point = min(split_point, segment.end)

        space = list(segment.split(split_point))
        replace_idx = int(from_tail)

        space[replace_idx] = replace = DiskSegment.new_replacement(
            space[replace_idx], DiskSegment.KIND_BUSY)

        self._space[best_match:best_match + 1] = [
            x for x in space if not x.is_null()]

        self._align_free_blocks()

        return replace

    def register(self, partition):
        """Register existing segment

        Mark corresponding sectors as busy by existing partition. Used during
        disk scan to build internal disk schema representation.

        :param partition: existing partition
        :type partition: Partition
        """
        allocation = self._reserve(
            partition.begin, partition.end, DiskSegment.KIND_BUSY)
        allocation.payload = partition

    def _mark_reserved(self, begin, end):
        self._reserve(begin, end, DiskSegment.KIND_RESERVED).set_is_service()

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

        allocation = DiskSegment(self, begin, end, kind)
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

            must_start_at = self._next_aligned_block(segment.begin)
            if must_start_at == segment.begin:
                continue

            align, free = segment.split(must_start_at)
            replace = [
                DiskSegment.new_replacement(align, align.KIND_ALIGN),
                free]
            replace_batch.append((idx, replace))

        replace_batch.reverse()
        for idx, replace in replace_batch:
            self._space[idx:idx + 1] = [x for x in replace if not x.is_null()]

    def _prev_aligned_block(self, value):
        follow = self._next_aligned_block(value)
        if value != follow:
            return follow - self.alignment
        return value

    def _next_aligned_block(self, value):
        offset = self.alignment - value % self.alignment
        if offset == self.alignment:
            return value
        return value + offset


class DiskSegment(AbstractSegment):
    @classmethod
    def new_replacement(cls, space, kind, payload=None):
        return cls(space.owner, space.begin, space.end, kind, payload=payload)

    def __init__(self, disk, begin, end, kind=AbstractSegment.KIND_FREE,
                 payload=None):
        self.begin = begin
        self.end = end

        super(DiskSegment, self).__init__(disk, kind, end - begin + 1, payload)

    def _make_cmp_repr(self):
        value = [
            FuzzyMatchSize(self.fuzzy_cmp_factor, x)
            for x in (self.begin, self.end)]
        value.append(self.kind)
        return tuple(value)

    def __repr__(self):
        return '<{}({} {}:{})>'.format(
            type(self).__name__, self._kind_names[self.kind],
            self.begin, self.end)

    def is_null(self):
        return self.end + 1 == self.begin

    def split(self, boundary):
        if not self.is_free():
            raise TypeError('Unsplittable item {!r}'.format(self))

        cls = type(self)
        left = cls(
            self.owner, self.begin, boundary - 1, self.kind, self.payload)
        right = cls(self.owner, boundary, self.end, self.kind, self.payload)

        return left, right


class LVM(AbstractStorage):
    block_size = 1024 * 1024

    @classmethod
    def new_by_scan(cls, name, lv=True):
        vg_set = lvm.vgdisplay()
        vg = cls(cls._filter_vg_by_name(vg_set, name))
        if not lv:
            return vg

        lv_by_pv = collections.defaultdict(list)
        for raw in lvm.lvdisplay():
            try:
                lv_by_pv[raw['vg']].append(raw)
            except KeyError:
                raise errors.InternalError()

        for raw in lv_by_pv[vg.name]:
            vg.register(raw)

        return vg

    def __init__(self, vg):
        try:
            self.name = vg['name']
            self.usable_size = self.size = vg['size']
            self.free = vg['free']
            self.uuid = vg['uuid']
        except KeyError:
            raise errors.InternalError()

        self.segments = [LVMSegment(self, LVMSegment.KIND_FREE, self.free)]

    def calc_biggest_unallocated_chunk(self):
        if not self.free:
            raise errors.BlockDeviceAllocationError(
                'There is no free space on {}'.format(self))
        return self.blocks_to_sizeunit(self.free)

    def allocate(self, size, from_tail=False):
        claim_accuracy = self.sizeunit_to_blocks(self.allocate_accuracy)
        claim = self.sizeunit_to_blocks(size)
        claim_min = max(claim - claim_accuracy, 0)

        if self.free < claim_min:
            raise errors.BlockDeviceAllocationError(
                'Unable to allocate {}(accuracy: {}) on LVM vg {}'.format(
                    size, self.allocate_accuracy, self.name))

        claim = min(claim, self.free)
        segment = LVMSegment(self, LVMSegment.KIND_BUSY, claim)

        self.free -= segment.size
        self.segments.append(segment)
        self.segments[0].size -= segment.size
        return segment

    def register(self, lv):
        try:
            size = lv['size']
        except KeyError:
            raise errors.InternalError()

        segment = LVMSegment(
            self, LVMSegment.KIND_BUSY, size, payload=lv)
        self.free -= segment.size
        self.segments.append(segment)
        return segment

    @staticmethod
    def _filter_vg_by_name(vg_set, name):
        for vg in vg_set:
            if vg['name'] != name:
                continue
            break
        else:
            raise errors.VGNotFoundError(
                'There is no LVMvg named "{}"'.format(name))
        return vg


class LVMSegment(AbstractSegment):
    pass


class Partition(BlockDevicePayload):
    suffix_number = None

    @classmethod
    def new_by_disk_segment(cls, space, index, code):
        block = _BlockDevice(
            None, space.size, space.owner.block_size,
            physical_block_size=space.owner.physical_block_size)
        return cls(space.owner, block, space.begin, index, code)

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


class _SGDiskMessage(object):
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


class _SGDiskPrint(_SGDiskMessage):
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
        super(_SGDiskPrint, self).__init__(data)

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


class _SGDiskInfo(_SGDiskMessage):
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
        super(_SGDiskInfo, self).__init__(data)

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
