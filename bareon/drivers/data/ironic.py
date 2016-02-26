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

from collections import defaultdict
import fnmatch
import math
import os

from oslo_config import cfg

from bareon.drivers.data.generic import GenericDataDriver
from bareon.drivers.data import ks_spaces_validator
from bareon import errors
from bareon import objects
from bareon.openstack.common import log as logging
from bareon.utils import hardware as hu
from bareon.utils import partition as pu
from bareon.utils import utils


LOG = logging.getLogger(__name__)

CONF = cfg.CONF

DEFAULT_LVM_META_SIZE = 64
DEFAULT_GRUB_SIZE = 24


class Ironic(GenericDataDriver):

    def __init__(self, data):
        super(Ironic, self).__init__(data)
        self._root_on_lvm = None
        self._boot_on_lvm = None

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
        # TODO(oberezovskyi): make validator work
        data = self._partition_data()
        ks_spaces_validator.validate(data, 'ironic')
        data = convert_size(data)
        partition_schema = objects.PartitionScheme()

        multiboot_installed = False

        LOG.debug('Looping over all disks in provision data')
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
                multiboot_partition = parted.add_partition(size=100)
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
        partition_schema = objects.PartitionScheme()

        disk_infos = [pu.info(disk['name']) for disk in self.hu_disks]
        fstab = self._find_hw_fstab()

        LOG.debug('Scanning all disks on the node')
        for disk_info in disk_infos:
            parted = partition_schema.add_parted(
                name=disk_info['generic']['dev'],
                label=disk_info['generic']['table'],
                install_bootloader=disk_info['generic']['has_bootloader']
            )

            LOG.debug('Scanning all partitions on disk %s '
                      % disk_info['generic']['dev'])

            for part in disk_info['parts']:
                if part.get('fstype', '') == 'free':
                    LOG.debug('Skipping a free partition at:'
                              'begin=%s, end=%s' %
                              (part.get('begin'), part.get('end')))
                    continue

                LOG.debug('Adding partition: '
                          'name=%s size=%s to hw schema' %
                          (part.get('disk_dev'), part.get('size')))

                # NOTE(lobur): avoid use of parted.add_partition to omit
                # counting logic; use real data instead.
                partition = objects.Partition(
                    name=part.get('name'),
                    count=part.get('num'),
                    device=part.get('disk_dev'),
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


def convert_size(data):
    data = convert_string_sizes(data)
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

    vg_sizes = defaultdict(int)
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
        if delta < 0:
            raise ValueError('Sum of requested filesystem sizes exceeds space '
                             'available on {type} "{id}" by {delta} '
                             'MiB'.format(delta=abs(delta), type=space['type'],
                                          id=_get_disk_id(space)))
        elif unsized_volume:
            ref = (unsized_volume['mount'] if unsized_volume.get(
                   'mount') else unsized_volume.get('pv'))
            if delta:
                LOG.info('Claiming remaining {delta} MiB for {ref} '
                         'volume/partition on {type} {id}.'
                         ''.format(delta=abs(delta),
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
                     'after completing allocations.'.format(delta=abs(delta),
                                                            type=space['type'],
                                                            id=_get_disk_id(
                                                                space)))
    return spaces


def convert_string_sizes(data):
    if isinstance(data, (list, tuple)):
        return [convert_string_sizes(el) for el in data]
    if isinstance(data, dict):
        for k, v in data.items():
            if (isinstance(v, basestring) and
                    any(x in v for x in ('%', 'remaining'))):
                continue
            if k in ('size', 'lvm_meta_size'):
                data[k] = human2bytes(v)
            else:
                data[k] = convert_string_sizes(v)
    return data


def human2bytes(value, default='MiB', target='MiB'):
    symbols = {'custom': ('B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'),
               'iec': ('KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB')}
    bytes = {}
    bytes.update({e: 1000.0 ** n for n, e in enumerate(symbols['custom'])})
    bytes.update({e: 1024.0 ** n for n, e in enumerate(symbols['iec'], 1)})
    try:
        number = ''
        prefix = default
        for index, letter in enumerate(value):
            if letter and letter.isdigit() or letter == '.':
                number += letter
            else:
                if value[index] == ' ':
                    index += 1
                prefix = value[index:]
                break
        return int(float(number) * bytes[prefix] / bytes[target])
    except Exception as ex:
        raise ValueError('Can\'t convert size %s. Error: %s' % (value, ex))
