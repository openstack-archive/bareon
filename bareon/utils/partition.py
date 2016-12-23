# Copyright 2014 Mirantis, Inc.
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

import os
import time

from oslo_log import log as logging

from bareon import errors
from bareon.utils import utils

LOG = logging.getLogger(__name__)
PARTITION_ALIGMENT = ('none', 'cylinder', 'minimal', 'optimal')

KiB = 1024
MiB = KiB * 1024
GiB = MiB * 1024
TiB = GiB * 1024


def info(dev):
    utils.udevadm_settle()

    disk = _disk_dummy(dev)
    _disk_info_by_lsblk(disk)
    _disk_info_by_file(disk)
    if disk['parts']:
        # avoid parted call, because parted exit with failure if there is no
        # partition table on disk
        _disk_info_by_parted(disk)

    # satisfy existing format expectations
    partitions = disk['parts']
    for p in partitions:
        if p['num'] is not None:
            p['num'] = int(p['num'])
        p['flags'] = sorted(p['flags'])

    LOG.debug('Info result: %s' % disk)
    return disk


def wipe(dev):
    # making an empty new table is equivalent to wiping the old one
    LOG.debug('Wiping partition table on %s (we assume it is equal '
              'to creating a new one)' % dev)
    make_label(dev)


def make_label(dev, label='gpt'):
    """Creates partition label on a device.

    :param dev: A device file, e.g. /dev/sda.
    :param label: Partition label type 'gpt' or 'msdos'. Optional.

    :returns: None
    """
    LOG.debug('Trying to create %s partition table on device %s' %
              (label, dev))
    if label not in ('gpt', 'msdos'):
        raise errors.WrongPartitionLabelError(
            'Wrong partition label type: %s' % label)
    utils.udevadm_settle()
    out, err = utils.execute('parted', '-s', dev, 'mklabel', label,
                             check_exit_code=[0, 1])
    LOG.debug('Parted output: \n%s' % out)
    reread_partitions(dev, out=out)


def set_partition_flag(dev, num, flag, state='on'):
    """Sets flag on a partition

    :param dev: A device file, e.g. /dev/sda.
    :param num: Partition number
    :param flag: Flag name. Must be one of 'bios_grub', 'legacy_boot',
    'boot', 'raid', 'lvm'
    :param state: Desiable flag state. 'on' or 'off'. Default is 'on'.

    :returns: None
    """
    LOG.debug('Trying to set partition flag: dev=%s num=%s flag=%s state=%s' %
              (dev, num, flag, state))
    # parted supports more flags but we are interested in
    # setting only this subset of them.
    # not all of these flags are compatible with one another.
    if flag not in ('bios_grub', 'legacy_boot', 'boot', 'raid', 'lvm'):
        raise errors.WrongPartitionSchemeError(
            'Unsupported partition flag: %s' % flag)
    if state not in ('on', 'off'):
        raise errors.WrongPartitionSchemeError(
            'Wrong partition flag state: %s' % state)
    utils.udevadm_settle()
    out, err = utils.execute('parted', '-s', dev, 'set', str(num),
                             flag, state, check_exit_code=[0, 1])
    LOG.debug('Parted output: \n%s' % out)
    reread_partitions(dev, out=out)


def set_gpt_type(dev, num, type_guid):
    """Sets guid on a partition.

    :param dev: A device file, e.g. /dev/sda.
    :param num: Partition number
    :param type_guid: Partition type guid. Must be one of those listed
    on this page http://en.wikipedia.org/wiki/GUID_Partition_Table.
    This method does not check whether type_guid is valid or not.

    :returns: None
    """
    # TODO(kozhukalov): check whether type_guid is valid
    LOG.debug('Setting partition GUID: dev=%s num=%s guid=%s' %
              (dev, num, type_guid))
    utils.udevadm_settle()
    utils.execute('sgdisk', '--typecode=%s:%s' % (num, type_guid),
                  dev, check_exit_code=[0])


def make_partition(dev, begin, end, ptype, alignment='optimal'):
    """Creates a partition on the device.

    :param dev: A device file, e.g. /dev/sda.
    :param begin: Beginning of the partition.
    :param end: Ending of the partition.
    :param ptype: Partition type: primary or logical.
    :param alignment: Set alignment mode for newly created partitions,
    valid alignment types are: none, cylinder, minimal, optimal. For more
    information about this you can find in GNU parted manual.

    :returns: None
    """
    LOG.debug('Trying to create a partition: dev=%s begin=%s end=%s' %
              (dev, begin, end))
    if ptype not in ('primary', 'logical'):
        raise errors.WrongPartitionSchemeError(
            'Wrong partition type: %s' % ptype)
    if alignment not in PARTITION_ALIGMENT:
        raise errors.WrongPartitionSchemeError(
            'Wrong partition alignment requested: %s' % alignment)

    # check begin >= end
    if begin >= end:
        raise errors.WrongPartitionSchemeError(
            'Wrong boundaries: begin >= end')

    # check if begin and end are inside one of free spaces available
    if not any(x['fstype'] == 'free' and begin >= x['begin'] and
               end <= x['end'] for x in info(dev)['parts']):
        raise errors.WrongPartitionSchemeError(
            'Invalid boundaries: begin and end '
            'are not inside available free space')

    utils.udevadm_settle()
    out, err = utils.execute(
        'parted', '-a', alignment, '-s', dev, 'unit', 'MiB',
        'mkpart', ptype, str(begin), str(end), check_exit_code=[0, 1])
    LOG.debug('Parted output: \n%s' % out)
    reread_partitions(dev, out=out)


def remove_partition(dev, num):
    LOG.debug('Trying to remove partition: dev=%s num=%s' % (dev, num))
    if not any(x['fstype'] != 'free' and x['num'] == num
               for x in info(dev)['parts']):
        raise errors.PartitionNotFoundError('Partition %s not found' % num)
    utils.udevadm_settle()
    out, err = utils.execute('parted', '-s', dev, 'rm',
                             str(num), check_exit_code=[0, 1])
    reread_partitions(dev, out=out)


def reread_partitions(dev, out='Device or resource busy', timeout=60):
    # The reason for this method to exist is that old versions of parted
    # use ioctl(fd, BLKRRPART, NULL) to tell Linux to re-read partitions.
    # This system call does not work sometimes. So we try to re-read partition
    # table several times. Besides partprobe uses BLKPG instead, which
    # is better than BLKRRPART for this case. BLKRRPART tells Linux to re-read
    # partitions while BLKPG tells Linux which partitions are available
    # BLKPG is usually used as a fallback system call.
    begin = time.time()
    while 'Device or resource busy' in out:
        if time.time() > begin + timeout:
            raise errors.BaseError('Unable to re-read partition table on'
                                   'device %s' % dev)
        LOG.debug('Last time output contained "Device or resource busy". '
                  'Trying to re-read partition table on device %s' % dev)
        time.sleep(2)
        out, err = utils.execute('partprobe', dev, check_exit_code=[0, 1])
        LOG.debug('Partprobe output: \n%s' % out)
        utils.udevadm_settle()


def get_uuid(device):
    return utils.execute('blkid', '-o', 'value', '-s', 'UUID', device,
                         check_exit_code=[0])[0].strip()


def _disk_dummy(device):
    metadata = {
        'dev': device,
        'size': None,
        'logical_block': None,
        'physical_block': None,
        'table': None,
        'model': None,
        'has_bootloader': None
    }
    return {'generic': metadata, 'parts': []}


def _disk_partition_dummy(device, suffix, disk):
    return {
        'dev': device,
        'master_suffix': suffix,
        'disk_dev': disk['dev'],  # TODO(dbogun): remove, can be calculated
        'name': device,  # TODO(dbogun): use 'dev' key instead
        'num': suffix,   # TODO(dbogun): use 'master_suffix' key instead
        'fstype': None,
        'size': None,
        'begin': None,
        'end': None,
        'type': None,
        'uuid': None,
        'flags': set(),
    }


def _disk_info_by_lsblk(disk):
    meta, partitions = disk['generic'], disk['parts']

    LOG.info('Collect disk structure for %s using lsblk', meta['dev'])

    output = utils.execute(
        'lsblk', '--bytes', '--list', '--noheadings', '--all',
        '--output=NAME,SIZE,PHY-SEC,LOG-SEC,UUID,FSTYPE', meta['dev'])[0]
    output = output.strip('\n')
    LOG.debug('lsblk output:\n%s', output)

    record_field_names = (
        'dev', 'size', 'physical_block', 'logical_block', 'uuid', 'fstype')
    partitions_map = {x['master_suffix']: x for x in partitions}
    dev_name = None
    is_master = True
    for record in output.splitlines():
        record = record.split()
        record = dict((f, v) for f, v in zip(record_field_names, record))
        # FIXME(dbogun): use "rounded" (MiB) values is bad idea
        record['size'] = int(record['size']) // 2**20  # Convert into MiB
        try:
            record['fstype'] = record['fstype'].lower()
        except KeyError:
            pass

        for f in ('physical_block', 'logical_block'):
            record[f] = int(record[f])

        if is_master:
            dev_name = record.pop('dev')
            record.pop('fstype', None)
            meta.update(record)
        else:
            suffix = record['dev'][len(dev_name):]
            record['dev'] = os.path.join('/dev', record['dev'])
            try:
                p = partitions_map[suffix]
            except KeyError:
                p = _disk_partition_dummy(record['dev'], suffix, meta)
                partitions.append(p)

            p['size'] = record['size']
            p['uuid'] = record.get('uuid')
            p['fstype'] = record.get('fstype')

        is_master = False

    partitions.sort(key=lambda x: x['master_suffix'])


def _disk_info_by_parted(disk):
    meta, partitions = disk['generic'], disk['parts']

    LOG.info('Collect disk structure for %s using parted', meta['dev'])
    # FIXME(dbogun): use "rounded" (MiB) values is bad idea
    output = utils.execute(
        'parted', meta['dev'], '--script', '--machine',
        'unit MiB print free')[0]
    LOG.debug('Parted output:\n%s', output)

    disk_field_names = (
        'dev', 'size', None, 'logical_block', 'physical_block', 'table',
        'model')
    partition_field_names = (
        'suffix', 'begin', 'end', 'size', 'fstype', 'type', 'flags')

    partitions_map = {x['master_suffix']: x for x in partitions}
    state = 'intro'
    for record in output.split(';'):
        record = record.strip()

        if not record:
            state = 'intro'

        elif state == 'intro':
            if record == 'BYT':
                state = 'master'

        elif state == 'master':
            state = 'partition'

            record = dict(
                (f, v) for f, v in zip(disk_field_names, record.split(':')))
            record['size'] = utils.parse_unit(record['size'], 'MiB')
            for f in ('physical_block', 'logical_block'):
                record[f] = int(record[f])
            del record[None]
            meta.update(record)

        elif state == 'partition':
            record = dict(
                (f, v) for f, v in zip(
                    partition_field_names, record.split(':')))
            for f in record:
                if record[f]:
                    continue
                record[f] = None
            for f in ('size', 'begin', 'end'):
                record[f] = utils.parse_unit(record[f], 'MiB')

            if record['fstype'] == 'free':
                record['suffix'] = None

            try:
                if record['suffix'] is None:
                    raise KeyError
                p = partitions_map[record['suffix']]
            except KeyError:
                dev = None
                if record['suffix']:
                    dev = '{}{}'.format(meta['dev'], record['suffix'])
                p = _disk_partition_dummy(dev, record['suffix'], meta)
                partitions.append(p)

            flags = record.pop('flags', '')
            if flags:
                flags = flags.split(',')
                p['flags'].update(x.strip().lower() for x in flags)

            del record['suffix']
            p.update(record)

        else:
            raise RuntimeError(
                'Internal error - impossible internal state %r', state)

    partitions.sort(key=lambda x: (x['master_suffix'], x['begin']))


def _disk_info_by_file(disk):
    meta = disk['generic']
    LOG.info('Collect disk structure for %s using file', meta['dev'])

    output = utils.execute(
        'file', '--brief', '--keep-going', '--special-files', meta['dev'])[0]
    LOG.debug('file output: \n%s' % output)

    records = (x.strip() for x in output.split(';'))

    meta['has_bootloader'] = any('boot sector' in x for x in records)
