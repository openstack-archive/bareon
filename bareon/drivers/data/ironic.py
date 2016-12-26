#
# Copyright 2016 Cray Inc.  All Rights Reserved.
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

import collections
import fnmatch
import itertools
import json
import math
import os

from oslo_config import cfg
from oslo_log import log as logging

from bareon.drivers.data.generic import GenericDataDriver
from bareon import errors
from bareon import objects
from bareon.utils import block_device
from bareon.utils import hardware as hu
from bareon.utils import partition as pu
from bareon.utils import utils


LOG = logging.getLogger(__name__)

CONF = cfg.CONF

MiB = 2 ** 20
DEFAULT_LVM_META_SIZE = 64 * MiB
DEFAULT_GRUB_SIZE = 24 * MiB


class Ironic(GenericDataDriver):
    data_validation_schema = 'ironic.json'

    _root_on_lvm = None
    _boot_on_lvm = None

    def __init__(self, data):
        super(Ironic, self).__init__(data)
        self._original_data = data
        convert_size(self.data['partitions'])

    @property
    def storage_claim(self):
        return StorageParser(self._original_data, self.image_scheme).claim

    def _get_image_meta(self):
        pass

    def _get_image_scheme(self):
        LOG.debug('--- Preparing image schema ---')
        data = self.data
        image_schema = objects.ImageScheme()
        image_list = data['images']
        deployment_flags = data.get('image_deploy_flags', {})

        image_schema.images = [objects.Image(uri=image['image_pull_url'],
                                             target_device=image['target'],
                                             format='bare',
                                             container='raw',
                                             os_id=image['name'],
                                             os_boot=image.get('boot', False),
                                             image_name=image.get('image_name',
                                                                  ''),
                                             image_uuid=image.get('image_uuid',
                                                                  ''),
                                             deployment_flags=deployment_flags)
                               for image in image_list]
        return image_schema

    def get_os_ids(self):
        images = set([image.os_id for image in self.image_scheme.images])
        partitions = set([id
                          for fs in self.partition_scheme.fss
                          for id in fs.os_id])
        return images & partitions

    def get_image_ids(self):
        return [image.os_id for image in self.image_scheme.images]

    @property
    def is_multiboot(self):
        return True if len(self.get_image_ids()) > 1 else False

    @property
    def is_configdrive_needed(self):
        return False

    @property
    def hw_partition_scheme(self):
        if not hasattr(self, '_hw_partition_scheme'):
            self._hw_partition_scheme = self._get_hw_partition_schema()
        return self._hw_partition_scheme

    @property
    def partitions_policy(self):
        if not hasattr(self, '_partitions_policy'):
            self._partitions_policy = self.data.get('partitions_policy',
                                                    'verify')
        return self._partitions_policy

    @property
    def root_on_lvm(self):
        return self.partition_scheme and self._root_on_lvm

    @property
    def boot_on_lvm(self):
        no_separate_boot = (self.partition_scheme.fs_by_mount('/boot') is None)
        return ((no_separate_boot and self.root_on_lvm) or
                self._boot_on_lvm)

    def _partition_data(self):
        return self.data['partitions']

    def _get_partition_scheme(self):
        """Reads disk/partitions volumes/vgs from given deploy_config

        Translating different ids (name, path, scsi) to name via
        scanning/comparing the underlying node hardware.
        """
        LOG.debug('--- Preparing partition scheme ---')
        LOG.debug('Looping over all disks in provision data')

        multiboot_installed = False

        partition_schema = objects.PartitionScheme()
        for disk in self._ks_disks:
            # # skipping disk if there are no volumes with size >0
            # # to be allocated on it which are not boot partitions
            if all((v["size"] <= 0 for v in disk["volumes"] if
                    v.get("mount") != "/boot")):
                continue

            LOG.debug('Processing disk type:%s id:%s' % (
                disk['id']['type'], disk['id']['value']))
            LOG.debug('Adding gpt table on disk type:%s id:%s' % (
                disk['id']['type'], disk['id']['value']))

            parted = partition_schema.add_parted(
                name=self._disk_dev(disk), label='gpt', disk_size=disk['size'])

            # TODO(lobur): do not add partitions implicitly, they may fail
            # partition verification
            parted.add_partition(size=DEFAULT_GRUB_SIZE, flags=['bios_grub'])

            if self.is_multiboot and not multiboot_installed:
                multiboot_installed = True
                multiboot_partition = parted.add_partition(size=100 * MiB)
                partition_schema.add_fs(device=multiboot_partition.name,
                                        mount='multiboot', fs_type='ext4',
                                        fstab_enabled=False, os_id=[])

            LOG.debug('Looping over all volumes on disk type:%s id:%s' % (
                disk['id']['type'], disk['id']['value']))
            for volume in disk['volumes']:
                LOG.debug('Processing volume: '
                          'name=%s type=%s size=%s mount=%s vg=%s '
                          'keep_data=%s' %
                          (volume.get('name'), volume.get('type'),
                           volume.get('size'), volume.get('mount'),
                           volume.get('vg'), volume.get('keep_data')))

                if volume['size'] <= 0:
                    LOG.debug('Volume size is zero. Skipping.')
                    continue

                FUNC_MAP = {
                    'partition': self._process_partition,
                    'raid': self._process_raid,
                    'pv': self._process_pv
                }
                FUNC_MAP[volume['type']](volume, disk, parted,
                                         partition_schema)

        LOG.debug('Looping over all volume groups in provision data')

        for vg in self._ks_vgs:
            self._process_vg(vg, partition_schema)

        partition_schema.elevate_keep_data()
        return partition_schema

    def _process_partition(self, volume, disk, parted, partition_schema):
        partition = self._add_partition(volume, disk, parted)
        if 'partition_guid' in volume:
            LOG.debug('Setting partition GUID: %s' %
                      volume['partition_guid'])
            partition.set_guid(volume['partition_guid'])

        if 'mount' in volume and volume['mount'] != 'none':
            LOG.debug('Adding file system on partition: '
                      'mount=%s type=%s' %
                      (volume['mount'],
                       volume.get('file_system', 'xfs')))
            partition_schema.add_fs(
                device=partition.name, mount=volume['mount'],
                fs_type=volume.get('file_system', 'xfs'),
                fs_label=self._getlabel(volume.get('disk_label')),
                fstab_options=volume.get('fstab_options', 'defaults'),
                fstab_enabled=volume.get('fstab_enabled', True),
                os_id=volume.get('images', self.get_image_ids()[:1]),
            )
            parted.install_bootloader = True
            if volume['mount'] == '/boot' and not self._boot_done:
                self._boot_done = True

    def _process_pv(self, volume, disk, parted, partition_schema):
        partition = self._add_partition(volume, disk, parted)
        LOG.debug('Creating pv on partition: pv=%s vg=%s' %
                  (partition.name, volume['vg']))
        lvm_meta_size = volume.get('lvm_meta_size', DEFAULT_LVM_META_SIZE)
        lvm_meta_size = utils.B2MiB(lvm_meta_size)
        # The reason for that is to make sure that
        # there will be enough space for creating logical volumes.
        # Default lvm extension size is 4M. Nailgun volume
        # manager does not care of it and if physical volume size
        # is 4M * N + 3M and lvm metadata size is 4M * L then only
        # 4M * (N-L) + 3M of space will be available for
        # creating logical extensions. So only 4M * (N-L) of space
        # will be available for logical volumes, while nailgun
        # volume manager might reguire 4M * (N-L) + 3M
        # logical volume. Besides, parted aligns partitions
        # according to its own algorithm and actual partition might
        # be a bit smaller than integer number of mebibytes.
        if lvm_meta_size < 10:
            raise errors.WrongPartitionSchemeError(
                'Error while creating physical volume: '
                'lvm metadata size is too small')
        metadatasize = int(math.floor((lvm_meta_size - 8) / 2))
        metadatacopies = 2
        partition_schema.vg_attach_by_name(
            pvname=partition.name, vgname=volume['vg'],
            metadatasize=metadatasize,
            metadatacopies=metadatacopies)

    def _process_raid(self, volume, disk, parted, partition_schema):
        partition = self._add_partition(volume, disk, parted)
        if not partition:
            return
        if 'mount' in volume and volume['mount'] not in ('none', '/boot'):
            LOG.debug('Attaching partition to RAID '
                      'by its mount point %s' % volume['mount'])
            partition_schema.md_attach_by_mount(
                device=partition.name, mount=volume['mount'],
                fs_type=volume.get('file_system', 'xfs'),
                fs_label=self._getlabel(volume.get('disk_label')))

        if 'mount' in volume and volume['mount'] == '/boot' and \
                not self._boot_done:
            LOG.debug('Adding file system on partition: '
                      'mount=%s type=%s' %
                      (volume['mount'],
                       volume.get('file_system', 'ext2')))
            partition_schema.add_fs(
                device=partition.name, mount=volume['mount'],
                fs_type=volume.get('file_system', 'ext2'),
                fs_label=self._getlabel(volume.get('disk_label')),
                fstab_options=volume.get('fstab_options', 'defaults'),
                fstab_enabled=volume.get('fstab_enabled', True),
                os_id=volume.get('images', self.get_image_ids()[:1]),
            )
            parted.install_bootloader = True
            self._boot_done = True

    def _process_vg(self, volume_group, partition_schema):
        LOG.debug('Processing vg %s' % volume_group['id'])
        LOG.debug(
            'Looping over all logical volumes in vg %s' % volume_group['id'])
        for volume in volume_group['volumes']:
            LOG.debug('Processing lv %s' % volume['name'])
            if volume['size'] <= 0:
                LOG.debug('LogicalVolume size is zero. Skipping.')
                continue

            if volume['type'] == 'lv':
                LOG.debug('Adding lv to vg %s: name=%s, size=%s' %
                          (volume_group['id'], volume['name'], volume['size']))
                lv = partition_schema.add_lv(name=volume['name'],
                                             vgname=volume_group['id'],
                                             size=volume['size'])

                if 'mount' in volume and volume['mount'] != 'none':
                    LOG.debug('Adding file system on lv: '
                              'mount=%s type=%s' %
                              (volume['mount'],
                               volume.get('file_system', 'xfs')))
                    partition_schema.add_fs(
                        device=lv.device_name,
                        mount=volume['mount'],
                        fs_type=volume.get('file_system', 'xfs'),
                        fs_label=self._getlabel(volume.get('disk_label')),
                        fstab_options=volume.get('fstab_options',
                                                 'defaults'),
                        fstab_enabled=volume.get('fstab_enabled', True),
                        os_id=volume.get('images', self.get_image_ids()[:1]),
                    )

                    lv_path = "%s/%s" % (volume_group['id'], volume['name'])
                    if volume['mount'] == '/':
                        self._root_on_lvm = lv_path
                    elif volume['mount'] == '/boot':
                        self._boot_on_lvm = lv_path

    def _add_partition(self, volume, disk, parted):
        partition = None
        if volume.get('mount') != '/boot':
            LOG.debug('Adding partition on disk %s: size=%s' % (
                disk['id']['value'], volume['size']))
            partition = parted.add_partition(
                size=volume['size'],
                keep_data=self._get_keep_data_flag(volume))
            LOG.debug('Partition name: %s' % partition.name)

        elif volume.get('mount') == '/boot' \
                and not self._boot_partition_done \
                and (disk in self._small_ks_disks or not self._small_ks_disks):
            # NOTE(kozhukalov): On some hardware GRUB is not able
            # to see disks larger than 2T due to firmware bugs,
            # so we'd better avoid placing /boot on such
            # huge disks if it is possible.
            LOG.debug('Adding /boot partition on disk %s: '
                      'size=%s', disk['id']['value'], volume['size'])
            partition = parted.add_partition(
                size=volume['size'],
                keep_data=self._get_keep_data_flag(volume))
            LOG.debug('Partition name: %s', partition.name)
            self._boot_partition_done = True
        else:
            LOG.debug('No need to create partition on disk %s. '
                      'Skipping.', disk['id']['value'])
        return partition

    def _get_keep_data_flag(self, volume):
        # For the new policy-based driver the default is True
        return volume.get('keep_data', True)

    def _get_hw_partition_schema(self):
        """Reads disks/partitions from underlying hardware.

        Does not rely on deploy_config
        """
        # NOTE(lobur): Only disks/partitions currently supported.
        # No vgs/volumes
        LOG.debug('--- Reading HW partition scheme from the node ---')

        fstab = self._find_hw_fstab()

        LOG.debug('Scanning all disks on the node')
        partition_schema = objects.PartitionScheme()
        for dev in self.hu_disks:
            disk_info = pu.scan_device(dev['name'])
            disk_meta = disk_info['generic']

            parted = partition_schema.add_parted(
                name=disk_meta['dev'],
                label=disk_meta['table'],
                install_bootloader=disk_meta['has_bootloader']
            )

            LOG.debug('Scanning all partitions on disk %s '
                      % disk_meta['dev'])

            for part in disk_info['parts']:
                if part['fstype'] == 'free':
                    LOG.debug('Skipping a free partition at:'
                              'begin=%s, end=%s' %
                              (part['begin'], part['end']))
                    continue

                LOG.debug('Adding partition: '
                          'name=%s size=%s to hw schema' %
                          (part['master_dev'], part['size']))

                # NOTE(lobur): avoid use of parted.add_partition to omit
                # counting logic; use real data instead.
                partition = objects.Partition(
                    name=part.get('name'),
                    count=part.get('num'),
                    device=part.get('master_dev'),
                    begin=part.get('begin'),
                    end=part.get('end'),
                    partition_type=part.get('type'),
                    flags=part.get('flags')
                )
                parted.partitions.append(partition)

                mnt_point = self._get_mount_point_from_fstab(fstab,
                                                             part['uuid'])
                if mnt_point:
                    LOG.debug('Adding filesystem: '
                              'device=%s fs_type=%s mount_point=%s '
                              'to hw schema' %
                              (part.get('name'), part.get('fstype'),
                               mnt_point))
                    partition_schema.add_fs(device=part.get('name'),
                                            mount=mnt_point,
                                            fs_type=part.get('fstype', ''))
                else:
                    LOG.warning("Not adding %s filesystem to hw_schema because"
                                " it has no mount point in fstab"
                                % part.get('name'))

        return partition_schema

    def _find_hw_fstab(self):
        mount_dir = '/mnt'
        fstabs = []

        fstab_fss = filter(lambda fss: fss.mount == '/',
                           self.partition_scheme.fss)

        for fss in fstab_fss:
            fss_dev = fss.device
            fstab_path = os.path.join(mount_dir, 'etc', 'fstab')

            try:
                utils.execute('mount', fss_dev, mount_dir, run_as_root=True,
                              check_exit_code=[0])
                fstab, _ = utils.execute('cat', fstab_path,
                                         run_as_root=True,
                                         check_exit_code=[0])
                utils.execute('umount', mount_dir, run_as_root=True)
            except errors.ProcessExecutionError as e:
                raise errors.HardwarePartitionSchemeCannotBeReadError(
                    "Cannot read fstab from %s partition. Error occurred: %s"
                    % (fss_dev, str(e))
                )
            LOG.info("fstab has been found on %s:\n%s" % (fss_dev, fstab))
            fstabs.append(fstab)
        return '\n'.join(fstabs)

    def _get_mount_point_from_fstab(self, fstab, part_uuid):
        res = None
        if not part_uuid:
            return res
        for line in fstab.splitlines():
            # TODO(lobur): handle fstab written using not partition UUID
            if part_uuid in line:
                res = line.split()[1]
                break
        return res

    def _match_device(self, hu_disk, ks_disk):
        """Check if hu_disk and ks_disk are the same device

        Tries to figure out if hu_disk got from hu.list_block_devices
        and ks_spaces_disk given correspond to the same disk device. This
        is the simplified version of hu.match_device

        :param hu_disk: A dict representing disk device how
        it is given by list_block_devices method.
        :param ks_disk: A dict representing disk device according to
         ks_spaces format.

        :returns: True if hu_disk matches ks_spaces_disk else False.
        """

        id_type = ks_disk['id']['type']
        id_value = ks_disk['id']['value']
        if isinstance(hu_disk.get(id_type), (list, tuple)):
            return any((id_value in value for value in hu_disk[id_type]))
        else:
            return id_value in hu_disk[id_type]

    @property
    def hu_disks(self):
        """Actual disks which are available on this node

        It is a list of dicts which are formatted other way than
        ks_spaces disks. To match both of those formats use
        _match_device method.
        """
        if not getattr(self, '_hu_disks', None):
            self._hu_disks = self._get_block_devices()
        return self._hu_disks

    @property
    def hu_vgs(self):
        """Actual disks which are available on this node

        It is a list of dicts which are formatted other way than
        ks_spaces disks. To match both of those formats use
        _match_data_by_pattern method.
        """
        if not getattr(self, '_hu_vgs', None):
            self._hu_vgs = self._get_vg_devices()
        return self._hu_vgs

    def _get_vg_devices(self):
        devices = hu.get_vg_devices_from_udev_db()
        vg_dev_infos = []
        for device in devices:
            vg_dev_infos.append(self._get_block_device_info(device))
        return vg_dev_infos

    def _get_block_devices(self):
        # Extends original result of hu.get_device_info with hu.get_device_ids
        # and add scsi param.
        devices = hu.get_block_devices_from_udev_db()
        block_dev_infos = []
        for device in devices:
            block_dev_infos.append(self._get_block_device_info(device))
        return block_dev_infos

    def _get_block_device_info(self, device):
        device_info = {
            'name': device,
            'scsi': hu.scsi_address(device)
        }
        hu_device_info = hu.get_device_info(device)
        if hu_device_info:
            device_info.update(hu_device_info)

        ids = hu.get_device_ids(device)
        if not ids:
            # DEVLINKS not presented on virtual environment.
            # Let's keep it here for development purpose.
            devpath = device_info.get('uspec', {}).get('DEVPATH')
            if devpath:
                ids = [devpath]
        device_info['path'] = ids

        return {k: v for k, v in device_info.iteritems() if v}

    def _get_grub(self):
        LOG.debug('--- Parse grub settings ---')
        grub = objects.Grub()
        kernel_params = self.data.get('deploy_data', {}).get(
            'kernel_params', '')
        # NOTE(lobur): Emulating ipappend 2 to allow early network-based
        # initialization during tenant image boot.
        bootif = utils.parse_kernel_cmdline().get("BOOTIF")
        if bootif:
            kernel_params += " BOOTIF=%s" % bootif

        if kernel_params:
            LOG.debug('Setting initial kernel parameters: %s',
                      kernel_params)
            grub.kernel_params = kernel_params
        return grub

    def _disk_dev(self, ks_disk):
        # first we try to find a device that matches ks_disk
        # comparing by-id and by-path links
        matched = [hu_disk['name'] for hu_disk in self.hu_disks
                   if self._match_device(hu_disk, ks_disk)]
        # if we can not find a device by its by-id and by-path links
        if not matched or len(matched) > 1:
            raise errors.DiskNotFoundError(
                'Disk not found with %s: %s' % (
                    ks_disk['id']['type'], ks_disk['id']['value']))
        return matched[0]

    def _disk_vg_dev(self, ks_vgs):
        # first we try to find a device that matches ks_disk
        # comparing by-id and by-path links
        matched = [hu_vg['name'] for hu_vg in self.hu_vgs
                   if self._match_data_by_pattern(hu_vg, ks_vgs)]
        # if we can not find a device by its by-id and by-path links
        if not matched or len(matched) > 1:
            raise errors.DiskNotFoundError(
                'Disk not found with %s: %s' % (
                    ks_vgs['id']['type'], ks_vgs['id']['value']))
        return matched[0]

    def _get_device_ids(self, dev_type):
        device_ids = []
        if dev_type == hu.DISK:
            devs = hu.get_block_devices_from_udev_db()
        elif dev_type == hu.PARTITION:
            devs = hu.get_partitions_from_udev_db()

        for dev in devs:
            ids = hu.get_device_ids(dev)
            if ids:
                device_ids.append(ids)
        return device_ids

    @property
    def hu_partitions(self):
        if not getattr(self, '_hu_partitions', None):
            self._hu_partitions = self._get_device_ids(dev_type=hu.PARTITION)
        return self._hu_partitions

    def _disk_partition(self, ks_partition):
        matched = [hu_partition['name'] for hu_partition in self.hu_partitions
                   if self._match_data_by_pattern(hu_partition, ks_partition)]
        if not matched or len(matched) > 1:
            raise errors.DiskNotFoundError(
                'Disk not found with %s: %s' % (
                    ks_partition['id']['type'], ks_partition['id']['value']))
        return matched[0]

    def _match_data_by_pattern(self, hu_data, ks_data):
        id_type = ks_data['id']['type']
        id_value = ks_data['id']['value']
        if isinstance(hu_data.get(id_type), (list, tuple)):
            return any((fnmatch.fnmatch(value, id_value) for value in
                        hu_data.get(id_type, [])))
        else:
            return fnmatch.fnmatch(hu_data.get(id_type, ''), id_value)

    @classmethod
    def validate_data(cls, data):
        super(Ironic, cls).validate_data(data)

        disks = data['partitions']

        # scheme is not valid if the number of disks is 0
        if not [d for d in disks if d['type'] == 'disk']:
            raise errors.WrongInputDataError(
                'Invalid partition schema: You must specify at least one '
                'disk.')


def convert_size(data):
    data = convert_string_sizes(data, target='B')
    data = _resolve_all_sizes(data)
    return data


def _resolve_all_sizes(data):
    # NOTE(oberezovskyi): "disks" should be processed before "vgs"
    disks = filter(lambda space: space['type'] == 'disk', data)
    disks = _resolve_sizes(disks)

    vgs = filter(lambda space: space['type'] == 'vg', data)
    _set_vg_sizes(vgs, disks)
    vgs = _resolve_sizes(vgs, retain_space_size=False)

    return disks + vgs


def _set_vg_sizes(vgs, disks):
    pvs = []
    for disk in disks:
        pvs += [vol for vol in disk['volumes'] if vol['type'] == 'pv']

    vg_sizes = collections.defaultdict(int)
    for pv in pvs:
        vg_sizes[pv['vg']] += pv['size'] - pv.get(
            'lvm_meta_size', DEFAULT_LVM_META_SIZE)

    for vg in vgs:
        vg['size'] = vg_sizes[vg['id']]


def _convert_percentage_sizes(space, size):
    for volume in space['volumes']:
        if isinstance(volume['size'], basestring) and '%' in volume['size']:
            # NOTE(lobur): decimal results of % conversion are floored.
            volume['size'] = size * int(volume['size'].split('%')[0]) // 100


def _get_disk_id(disk):
    if isinstance(disk['id'], dict):
        return '{}: {}'.format(disk['id']['type'],
                               disk['id']['value'])
    return disk['id']


def _get_space_size(space, retain_size):
    if not space.get('size'):
        raise ValueError('Size of {type} "{id}" is not '
                         'specified'.format(type=space['type'],
                                            id=_get_disk_id(space)))
    return space['size'] if retain_size else space.pop('size')


def _process_space_claims(space):
    claimed_space = 0
    unsized_volume = None
    for volume in space['volumes']:
        if (isinstance(volume['size'], basestring) and
                volume['size'] == 'remaining'):
            if not unsized_volume:
                unsized_volume = volume
            else:
                raise ValueError('Detected multiple volumes attempting to '
                                 'claim remaining size {type} "{id}"'
                                 ''.format(type=space['type'],
                                           id=_get_disk_id(space)))
        else:
            claimed_space += volume['size']
    return claimed_space, unsized_volume


def _resolve_sizes(spaces, retain_space_size=True):
    for space in spaces:
        space_size = _get_space_size(space, retain_space_size)
        # NOTE(oberezovskyi): DEFAULT_GRUB_SIZE is size of grub stage 1.5
        # (bios_grub) partition
        taken_space = DEFAULT_GRUB_SIZE if space['type'] == 'disk' else 0
        _convert_percentage_sizes(space, space_size)
        claimed_space, unsized_volume = _process_space_claims(space)
        taken_space += claimed_space
        delta = space_size - taken_space
        delta_MiB = utils.B2MiB(abs(delta))
        if delta < 0:
            raise ValueError('Sum of requested filesystem sizes exceeds space '
                             'available on {type} "{id}" by {delta} '
                             'MiB'.format(delta=delta_MiB, type=space['type'],
                                          id=_get_disk_id(space)))
        elif unsized_volume:
            ref = (unsized_volume['mount'] if unsized_volume.get(
                   'mount') else unsized_volume.get('pv'))
            if delta:
                LOG.info('Claiming remaining {delta} MiB for {ref} '
                         'volume/partition on {type} {id}.'
                         ''.format(delta=delta_MiB,
                                   type=space['type'],
                                   id=_get_disk_id(space),
                                   ref=ref))
                unsized_volume['size'] = delta
            else:
                raise ValueError(
                    'Volume/partition {ref} requested all remaining space, '
                    'but no unclaimed space remains on {type} {id}'.format(
                        type=space['type'],
                        id=_get_disk_id(space),
                        ref=ref))
        else:
            LOG.info('{delta} MiB of unclaimed space remains on {type} "{id}" '
                     'after completing allocations.'.format(delta=delta_MiB,
                                                            type=space['type'],
                                                            id=_get_disk_id(
                                                                space)))
    return spaces


def convert_string_sizes(data, target=None):
    if target is not None:
        conv_args = {'target': target}
    else:
        conv_args = {}

    if isinstance(data, (list, tuple)):
        return [convert_string_sizes(el, target=target) for el in data]
    if isinstance(data, dict):
        for k, v in data.items():
            if (isinstance(v, basestring) and
                    any(x in v for x in ('%', 'remaining'))):
                continue
            if k in ('size', 'lvm_meta_size'):
                data[k] = utils.human2bytes(v, **conv_args)
            else:
                data[k] = convert_string_sizes(v, target=target)
    return data


class StorageParser(object):
    def __init__(self, data, image_schema):
        self.storage = objects.block_device.StorageSubsystem()
        self.disk_finder = block_device.DeviceFinder()

        operation_systems = self._collect_operation_systems(image_schema)
        self._existing_os_binding = set(operation_systems)
        self._default_os_binding = operation_systems[:1]

        self.mdfs_by_mount = {}
        self.mddev_by_mount = collections.defaultdict(list)
        self.lvm_pv_reference = collections.defaultdict(list)

        LOG.debug('--- Preparing partition scheme ---')
        LOG.debug('Looping over all disks in provision data')
        try:
            self.claim = self._parse(data)
        except KeyError:
            raise errors.DataSchemaCorruptError()

        self._assemble_lvm_vg()
        self._assemble_mdraid()
        self._validate()

    def _collect_operation_systems(self, image_schema):
        return [image.os_id for image in image_schema.images]

    def _parse(self, data):
        for raw in data['partitions']:
            kind = raw['type']
            if kind == 'disk':
                item = self._parse_disk(raw)
            elif kind == 'vg':
                item = self._parse_lvm_vg(raw)
            else:
                raise errors.DataSchemaCorruptError(exc_info=False)

            self.storage.add(item)

    def _assemble_lvm_vg(self):
        vg_by_id = {
            i.idnr: i for i in self.storage.items_by_kind(
                objects.block_device.LVMvg)}

        defined_vg = set(vg_by_id)
        referred_vg = set(self.lvm_pv_reference)
        empty_vg = defined_vg - referred_vg
        orphan_pv = referred_vg - defined_vg

        if empty_vg:
            raise errors.WrongInputDataError(
                'Following LVMvg have no any PV: "{}"'.format(
                    '", "'.join(sorted(empty_vg))))
        if orphan_pv:
            raise errors.WrongInputDataError(
                'Following LVMpv refer to missing VG: "{}"'.format(
                    '", "'.join(sorted(orphan_pv))))

        for idnr in defined_vg:
            vg = vg_by_id[idnr]
            for pv in self.lvm_pv_reference[idnr]:
                vg.add(pv)

    def _assemble_mdraid(self):
        name_template = '/dev/md{:d}'

        idx = itertools.count()
        for mount in sorted(self.mddev_by_mount):
            components = self.mddev_by_mount[mount]

            fields = self.mdfs_by_mount[mount]
            try:
                name = fields.pop('name')
                if not name.startswith('/dev/'):
                    name = '/dev/{}'.format(name)
            except KeyError:
                name = name_template.format(next(idx))

            md = objects.block_device.MDRaid(name, **fields)
            for item in components:
                md.add(item)

            self.storage.add(md)

    def _validate(self):
        for item in self.storage.items:
            if isinstance(item, objects.block_device.Disk):
                self._validate_disk(item)
            elif isinstance(item, objects.block_device.LVMvg):
                self._validate_lvm_vg(item)

    def _validate_disk(self, disk):
        remaining = []
        for item in disk.items:
            if item.size.kind != item.size.KIND_BIGGEST:
                continue
            remaining.append(item)

        if len(remaining) < 2:
            return

        raise errors.WrongInputDataError(
            'Multiple requests on "remaining" space.\n'
            'disk:\n{}\npartitions:\n{}'.format(disk.idnr, '\n'.join(
                repr(x) for x in remaining)))

    def _validate_lvm_vg(self, vg):
        remaining = []
        for item in vg.items_by_kind(objects.block_device.LVMlv):
            if item.size.kind != item.size.KIND_BIGGEST:
                continue
            remaining.append(item)

        if len(remaining) < 2:
            return

        raise errors.WrongInputDataError(
            'Multiple requests on "remaining" space.\n'
            'lvm-vg: {}\nlogical volumes:\n{}'.format(vg.idnr, '\n'.join(
                repr(x) for x in remaining)))

    def _parse_disk(self, data):
        size = self._size(data['size'])
        idnr = self._disk_id(data['id'])
        disk = objects.block_device.Disk(
            idnr, size, **self._get_fields(data, 'name'))

        for raw in data['volumes']:
            kind = raw['type']
            if kind == 'pv':
                item = self._parse_lvm_pv(raw)
            elif kind == 'raid':
                item = self._parse_mdraid_dev(raw)
            elif kind == 'partition':
                item = self._parse_disk_partition(raw)
            elif kind == 'boot':
                item = self._parse_disk_partition(raw)
                item.is_boot = True
            # FIXME(dbogun): unsupported but allowed by data-schema type
            elif kind == 'lvm_meta_pool':
                item = None
            else:
                raise errors.DataSchemaCorruptError(exc_info=False)

            if item is not None:
                disk.add(item)

        return disk

    def _parse_lvm_vg(self, data):
        vg = objects.block_device.LVMvg(
            data['id'], **self._get_fields(
                data, 'label', 'min_size', 'keep_data', '_allocate_size'))
        for raw in data['volumes']:
            item = self._parse_lvm_lv(raw)
            vg.add(item)

        return vg

    def _parse_lvm_pv(self, data):
        size = self._size(data['size'])
        fields = self._get_fields(data, 'keep_data', 'lvm_meta_size')
        if 'lvm_meta_size' in fields:
            fields['lvm_meta_size'] = self._size(fields['lvm_meta_size'])
        pv = objects.block_device.LVMpv(data['vg'], size, **fields)

        self.lvm_pv_reference[pv.vg_idnr].append(pv)

        return pv

    def _parse_mdraid_dev(self, data):
        fields = self._get_filesystem_fields(data, 'name')
        size = fields.pop('size')
        mddev = objects.block_device.MDDev(size)

        mount = fields['mount']
        self.mdfs_by_mount.setdefault(mount, fields)
        self.mddev_by_mount[mount].append(mddev)

        return mddev

    def _parse_disk_partition(self, data):
        fields = self._get_filesystem_fields(data, 'disk_label')
        self._rename_fields(fields, {'disk_label': 'label'})
        if fields.get('file_system') == 'swap':
            fields['guid_code'] = 0x8200
        size = fields.pop('size')
        return objects.block_device.Partition(size, **fields)

    def _parse_lvm_lv(self, data):
        fields = self._get_filesystem_fields(data)
        size = fields.pop('size')
        return objects.block_device.LVMlv(data['name'], size, **fields)

    def _get_filesystem_fields(self, data, *extra):
        fields = self._get_fields(
            data,
            'mount', 'keep_data', 'file_system', 'size', 'images',
            'fstab_options', 'fstab_enabled', *extra)
        self._rename_fields(fields, {
            'fstab_enabled': 'fstab_member',
            'fstab_options': 'mount_options',
            'images': 'os_binding'})

        fields.setdefault('os_binding', self._default_os_binding)
        fields['size'] = self._size(fields['size'])

        if 'mount' in fields:
            if fields['mount'].lower() == 'none':
                fields.pop('mount')
        if 'file_system' in fields:
            fields['file_system'] = fields['file_system'].lower()

        fields['os_binding'] = set(fields['os_binding'])
        missing = fields['os_binding'] - self._existing_os_binding
        if missing:
            # FIXME(dbogun): it must be treated as error
            LOG.warn(
                'Try to claim not existing operating systems: '
                '"{}"\n\n{}'.format(
                    '", "'.join(sorted(missing)),
                    json.dumps(data, indent=2)))
            fields['os_binding'] -= missing

        return fields

    @staticmethod
    def _get_fields(data, *fields):
        result = {}
        for f in fields:
            try:
                result[f] = data[f]
            except KeyError:
                pass
        return result

    @staticmethod
    def _rename_fields(data, mapping):
        for src in mapping:
            try:
                value = data.pop(src)
            except KeyError:
                continue
            data[mapping[src]] = value

    @staticmethod
    def _size(size):
        if size == 'remaining':
            result = block_device.SpaceClaim.new_biggest()
        else:
            result = block_device.SizeUnit.new_by_string(
                size, default_unit='MiB')
            result = block_device.SpaceClaim.new_by_sizeunit(result)
        return result

    def _disk_id(self, idnr):
        idnr = objects.block_device.DevIdnr(idnr['type'], idnr['value'])
        idnr(self.disk_finder)
        return idnr


class DeprecatedPartitionSchemaBuilder(object):
    def __init__(self, storage_claim, multiboot_partition):
        self.storage_claim = storage_claim
        self.multiboot_partition = multiboot_partition

        self.schema = objects.PartitionSchema()

        self._convert()

    def _convert(self):
        for claim in self.storage_claim.items:
            if isinstance(claim, objects.block_device.Disk):
                self._convert_disk(claim)
            elif isinstance(claim, objects.block_device.LVMvg):
                self._convert_lvm_vg(claim)
            elif isinstance(claim, objects.block_device.MDRaid):
                self._convert_mdraid(claim)

    def _convert_disk(self, disk):
        old_disk = self.schema.add_parted(
            name=disk.dev, label='gpt', install_bootloader=True,
            size=self._unpack_size(disk.size).bytes)

        for claim in disk.items:
            args = {}
            if isinstance(claim, objects.block_device.Partition):
                args['keep_data'] = claim.keep_data_flag
            partition = old_disk.add_partition(
                size=self._unpack_size(claim.size).bytes, guid=claim.guid,
                **args)

            if isinstance(claim, objects.block_device.FileSystemMixin):
                self._add_fs(claim, partition.name)

    def _convert_lvm_vg(self, vg):
        self.schema.add_vg(name=vg.idnr)
        for claim in vg.items:
            if isinstance(claim, objects.block_device.LVMpv):
                args = {}
                if claim.meta_size:
                    args['metadatasize'] = self._unpack_size(
                        claim.meta_size).in_unit('MiB').value_int
                self.schema.add_pv(name=claim.vg_idnr, **args)
            elif isinstance(claim, objects.block_device.LVMlv):
                self.schema.add_lv(
                    name=claim.name, vgname=vg.idnr,
                    size=self._unpack_size(claim.size).bytes)
                self._add_fs(claim, claim.name)

    def _convert_mdraid(self, md):
        self.schema.add_md(
            name=md.name, level=md.level, devices=[
                x.expected_dev for x in md.items])
        self._add_fs(md, md.name)

    def _add_fs(self, claim, dev):
        mount = claim.mount
        if claim is self.multiboot_partition:
            mount = 'multiboot'

        if not mount:
            return

        args = {k: v for k, v in (
            ('fstab_options', claim.mount_options),
            ('fs_type', claim.file_system),
            ('os_id', list(claim.os_binding))) if v}

        self.schema.add_fs(
            device=dev, mount=mount, fstab_enabled=claim.fstab_member, **args)

    @staticmethod
    def _unpack_size(size):
        return size.size

    @staticmethod
    def guid_code_to_parted_flags(code):
        flags = set()

        if code == 0xEF02:
            flags.add('bios_grub')
        elif code == 0xEF00:
            flags.add('boot')
        elif code == 0xFD00:
            flags.add('raid')
        elif code == 0x8E00:
            flags.add('lvm')

        return sorted(flags)
