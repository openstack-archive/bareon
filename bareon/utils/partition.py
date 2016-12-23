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

import time

from oslo_log import log as logging

from bareon import errors
from bareon.utils import block_device
from bareon.utils import utils

LOG = logging.getLogger(__name__)
PARTITION_ALIGMENT = ('none', 'cylinder', 'minimal', 'optimal')

KiB = 1024
MiB = KiB * 1024
GiB = MiB * 1024
TiB = GiB * 1024


def scan_device(dev):
    utils.udevadm_settle()

    disk = block_device.Disk.new_by_device_scan(dev)

    meta = {
        'dev': disk.dev,
        'table': disk.partition_table_format,
        'has_bootloader': disk.is_bootable,
        'block_size': disk.block_size
    }

    partitions = []
    for segment in disk.segments:
        info = {
            'master_dev': disk.dev,
            'begin': segment.begin * disk.block_size,
            'end': segment.end * disk.block_size + disk.block_size - 1,
            'size': segment.size * disk.block_size
        }
        if isinstance(segment, block_device.EmptySpace):
            info['fstype'] = 'free'
        elif isinstance(segment, block_device.Partition):
            info['num'] = segment.index
            info['name'] = segment.dev
            info['guid'] = segment.guid
            info['type'] = 'primary' if segment.index < 5 else 'logical'

            flags = set()
            if segment.code == 0xEF02:
                flags.add('bios_grub')
            elif segment.code == 0xEF00:
                flags.add('boot')
            elif segment.code == 0xFD00:
                flags.add('raid')
            elif segment.code == 0x8E00:
                flags.add('lvm')

            if segment.attributes & 0x04:
                flags.add('legacy_boot')

            info['flags'] = sorted(flags)

            lsblk_info = _partition_info_by_lsblk(segment.dev)

            # check that got correct device
            size = lsblk_info.pop('size')
            if size != segment.size * segment.block_size:
                raise errors.BlockDeviceSchemeError(
                    'Partition schema for {} from gdisk don\'t match info '
                    'from lsblk'.format(segment.dev))

            info.update({'fstype': None, 'uuid': None})  # defaults
            info.update(**lsblk_info)
        else:
            raise TypeError(
                'Unexpected object {!r} into {!r}.segments list'.format(
                    segment, disk))

        partitions.append(info)

    return {
        'generic': meta,
        'parts': partitions
    }


# FIXME(dbogun): unused function
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

    disk = block_device.Disk.new_by_device_scan(dev)
    partition = disk.allocate(end - begin + 1)

    utils.udevadm_settle()
    out, err = utils.execute(
        'parted', '-a', alignment, '-s', dev, 'unit', 's',
        'mkpart', ptype, str(partition.begin), str(partition.end))
    LOG.debug('Parted output: \n%s', out)
    reread_partitions(dev, out=out)


# FIXME(dbogun): unused function
def remove_partition(dev, num):
    LOG.debug('Trying to remove partition: dev=%s num=%s' % (dev, num))
    disk = scan_device(dev)
    if not any(x['fstype'] != 'free' and x['num'] == num
               for x in disk['parts']):
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


def _partition_info_by_lsblk(dev):
    LOG.info('Collect partition info for %s using lsblk', dev)

    output = utils.execute(
        'lsblk', '--bytes', '--list', '--noheadings',
        '--output=SIZE,UUID,FSTYPE', dev)[0]
    output = output.strip('\n')
    LOG.debug('lsblk output:\n%s', output)

    field_names = ('size', 'uuid', 'fstype')
    record = dict((f, v) for f, v in zip(field_names, output.split()))
    record['size'] = int(record['size'])

    return record
