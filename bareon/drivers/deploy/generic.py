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

import abc
import itertools
import json
import os
import re

from oslo_config import cfg
from oslo_log import log as logging
import six

from bareon.actions import configdrive
from bareon.actions import partitioning
from bareon.drivers.deploy.base import BaseDeployDriver
from bareon.drivers.deploy import mixins
from bareon import errors
from bareon import objects
from bareon.utils import block_device
from bareon.utils import fs as fu
from bareon.utils import grub as gu
from bareon.utils import lvm as lu
from bareon.utils import md as mu
from bareon.utils import partition as pu
from bareon.utils import utils

opts = [
    cfg.StrOpt(
        'udev_rules_dir',
        default='/etc/udev/rules.d',
        help='Path where to store actual rules for udev daemon',
    ),
    cfg.StrOpt(
        'udev_rules_lib_dir',
        default='/lib/udev/rules.d',
        help='Path where to store default rules for udev daemon',
    ),
    cfg.StrOpt(
        'udev_rename_substr',
        default='.renamedrule',
        help='Substring to which file extension .rules be renamed',
    ),
    cfg.StrOpt(
        'udev_empty_rule',
        default='empty_rule',
        help='Correct empty rule for udev daemon',
    ),
    cfg.IntOpt(
        'grub_timeout',
        default=5,
        help='Timeout in secs for GRUB'
    ),
]

CONF = cfg.CONF
CONF.register_opts(opts)

LOG = logging.getLogger(__name__)


# TODO(lobur): This driver mostly copies nailgun driver. Need to merge them.
class GenericDeployDriver(BaseDeployDriver, mixins.MountableMixin):

    def do_reboot(self):
        LOG.debug('--- Rebooting node (do_reboot) ---')
        utils.execute('reboot')

    def do_provisioning(self):
        LOG.debug('--- Provisioning (do_provisioning) ---')
        self.do_partitioning()
        self.do_configdrive()
        map(self.do_install_os, self.driver.get_os_ids())
        if self.driver.is_multiboot:
            self.do_multiboot_bootloader()
        LOG.debug('--- Provisioning END (do_provisioning) ---')

    def do_partitioning(self):
        LOG.debug('--- Partitioning disks (do_partitioning) ---')

        try:
            storage_claim = self.driver.storage_claim
        except AttributeError:
            # TODO(dbogun): completely replace deprecated partitioning code
            PolicyPartitioner(self.driver).partition()
        else:
            handlers_map = {
                'clean': PartitionPolicyClean,
                'verify': PartitionPolicyVerify,
                'nailgun_legacy': PartitionPolicyNailgun}
            handler = handlers_map[self.driver.partitions_policy]
            handler = handler(self, storage_claim)

            handler()

        LOG.debug('--- Partitioning disks END (do_partitioning) ---')

    def do_configdrive(self):
        configdrive.ConfigDriveAction(self.driver).execute()

    def do_copyimage(self):
        raise NotImplementedError

    def do_install_os(self, os_id):
        self.do_copyimage(os_id)
        os_dir = '/tmp/target'
        with self.mount_target(os_dir, os_id, treat_mtab=False):
            if not self.driver.is_multiboot:
                self.do_singleboot_bootloader(os_dir, os_id)
            self.do_generate_fstab(os_dir, os_id)

    def do_generate_fstab(self, os_path, os_id):
        mount2uuid = self._mount2uuid(os_id, check_root=False)
        if not os.path.exists(os.path.join(os_path, 'etc')):
            LOG.info('Can\'t create fstab for {} image'.format(os_id))
            return
        with open(os.path.join(os_path, 'etc/fstab'), 'wb') as f:
            for fs in self.driver.partition_scheme.fs_by_os_id(os_id):
                f.write('{enabled}UUID={uuid} {mount} {fs} {options} '
                        '0 0\n'.format(enabled='' if fs.fstab_enabled else '#',
                                       uuid=mount2uuid[fs.mount],
                                       mount=fs.mount,
                                       fs=fs.type,
                                       options=fs.fstab_options))

    def do_multiboot_bootloader(self):
        install_devices = [d.name for d in self.driver.partition_scheme.parteds
                           if d.install_bootloader]
        mount_dir = '/tmp/target'
        with self._mount_bootloader(mount_dir) as uuid:
            gu.grub2_install(install_devices, boot_root=mount_dir)
            self._generate_boot_info(mount_dir, uuid)

    def _mount2uuid(self, os_id, check_root=True):
        mount2uuid = {}
        for fs in self.driver.partition_scheme.fs_by_os_id(os_id):
            mount2uuid[fs.mount] = pu.get_uuid(fs.device)

        if check_root and '/' not in mount2uuid:
            raise errors.WrongPartitionSchemeError(
                'Error: device with / mountpoint has not been found')
        return mount2uuid

    def _uuid2osid(self, check_root=True):
        uuid2image = {}
        for os_id in self.driver.get_os_ids():
            mount2uuid = self._mount2uuid(os_id, check_root=check_root)
            uuid = mount2uuid.get('/', '')
            if uuid:
                uuid2image[uuid] = os_id
        return uuid2image

    def _get_multiboot_boot_image(self):
        return next((image for image in self.driver.image_scheme.images if
                     image.os_boot), None)

    def do_singleboot_bootloader(self, chroot, os_id):
        grub = self.driver.grub
        try:
            guessed_version = gu.guess_grub_version(chroot=chroot)
        except errors.GrubUtilsError as ex:
            LOG.warning('Grub detection failed. Error: {}'.format(ex))
            guessed_version = -1
        if guessed_version != grub.version:
            grub.version = guessed_version
            LOG.warning('Grub version differs from which the operating system '
                        'should have by default. Found version in image: '
                        '{0}'.format(guessed_version))

        if grub.version == 1 and self.driver.is_multiboot:
            LOG.warning('Grub1 is being used in a multiboot deployment, '
                        'thus it is not guaranteed that image name "{}"  will '
                        'be discovered by os-prober and appear in the common '
                        'grub.cfg'.format(os_id))

        install_devices = [d.name for d in self.driver.partition_scheme.parteds
                           if d.install_bootloader]

        if grub.version == 1:
            mount2uuid = self._mount2uuid(os_id)
            grub.append_kernel_params('root=UUID=%s ' % mount2uuid['/'])

        GRUB_INSTALLERS = {1: self._do_bootloader_grub1,
                           2: self._do_bootloader_grub2,
                           -1: self._do_bootloader_grub2_bundled}
        GRUB_INSTALLERS[grub.version](grub, chroot, install_devices,
                                      self.driver.boot_on_lvm)

    def _do_bootloader_grub1(self, grub, chroot, install_devices,
                             lvm_boot=False):
        if lvm_boot:
            raise NotImplementedError("Grub 1 does not support boot from LVM.")
        # TODO(kozhukalov): implement which kernel to use by default
        # Currently only grub1_cfg accepts kernel and initrd parameters.
        boot_device = self.driver.partition_scheme.boot_device(grub.version)
        kernel = grub.kernel_name or gu.guess_kernel(chroot=chroot,
                                                     regexp=grub.kernel_regexp)
        initrd = grub.initrd_name or gu.guess_initrd(chroot=chroot,
                                                     regexp=grub.initrd_regexp)
        gu.grub1_cfg(kernel=kernel, initrd=initrd,
                     kernel_params=grub.kernel_params, chroot=chroot,
                     grub_timeout=CONF.grub_timeout)
        gu.grub1_install(install_devices, boot_device, chroot=chroot)

    def _do_bootloader_grub2(self, grub, chroot, install_devices,
                             lvm_boot=False):
        try:
            gu.grub2_cfg(kernel_params=grub.kernel_params, chroot=chroot,
                         grub_timeout=CONF.grub_timeout, lvm_boot=lvm_boot)
            gu.grub2_install(install_devices, chroot=chroot, lvm_boot=lvm_boot)
        except errors.ProcessExecutionError as ex:
            LOG.warning('Tenant grub2 install failed. Error: {}'.format(ex))
            LOG.warning('Trying to install using bundled grub2')
            self._do_bootloader_grub2_bundled(grub, chroot, install_devices,
                                              lvm_boot=lvm_boot)

    def _do_bootloader_grub2_bundled(self, grub, chroot, install_devices,
                                     lvm_boot=False):
        gu.grub2_install(install_devices, boot_root=chroot, lvm_boot=lvm_boot)
        gu.grub2_cfg_bundled(kernel_params=grub.kernel_params,
                             chroot=chroot, grub_timeout=CONF.grub_timeout,
                             lvm_boot=lvm_boot)

    def _generate_boot_info(self, chroot, uuid=None):
        def list_of_seq_unique_by_key(seq, key):
            seen = set()
            seen_add = seen.add
            return [x for x in seq if
                    x[key] not in seen and not seen_add(x[key])]

        regex = re.compile('menuentry \'(?P<name>[^\']+)\'.*?search '
                           '.*?(?P<uuid>[0-9a-f\-]{36}).*?linux(?:16)? '
                           '(?P<kernel>.*?) .*?initrd(?:16)? '
                           '(?P<initrd>[^\n]*)', re.M | re.DOTALL)

        entries = '''
    set timeout=1
    insmod part_gpt
    insmod ext2
        '''
        boot_entry = '''
    menuentry '{name}'{{
    search --no-floppy --fs-uuid --set=root {uuid}
    linux {kernel} root=UUID={uuid} ro {kernel_params}
    initrd {initrd}
    }}
    '''
        boot_elements = []
        os.environ['GRUB_DISABLE_SUBMENU'] = 'y'
        os_prober_entries = utils.execute('/etc/grub.d/30_os-prober')[0]

        uuid2osid = self._uuid2osid(check_root=False)

        for index, element in enumerate(re.finditer(regex, os_prober_entries)):
            os_id = uuid2osid.get(element.group('uuid'), '')
            if not os_id:
                continue

            image = self.driver.image_scheme.get_os_root(os_id)

            entries += boot_entry.format(**{
                'name': element.group('name'),
                'uuid': element.group('uuid'),
                'kernel': element.group('kernel'),
                'initrd': element.group('initrd'),
                'kernel_params': self.driver.data.get('deploy_data', {}).get(
                    'kernel_params', '')
            })
            boot_elements.append({
                'boot_name': element.group('name'),
                'root_uuid': element.group('uuid'),
                'os_id': os_id,
                'image_name': image.image_name,
                'image_uuid': image.image_uuid,
                'grub_id': index,
            })

        boot_elements = list_of_seq_unique_by_key(boot_elements, 'root_uuid')

        boot_image = self._get_multiboot_boot_image()
        if boot_image:
            root_uuid = self._mount2uuid(boot_image.os_id)['/']
        boot_id = next((element['grub_id'] for element in boot_elements if
                        element['root_uuid'] == root_uuid), 0)

        entries += 'set default={}'.format(boot_id)
        with open(os.path.join(chroot, 'boot', 'grub2', 'grub.cfg'),
                  'w') as conf:
            conf.write(entries)

        result = {'elements': boot_elements,
                  'multiboot_partition': uuid,
                  'current_element': boot_id}
        with open('/tmp/boot_entries.json', 'w') as boot_entries_file:
            json.dump(result, boot_entries_file)


# FIXME(dbogun): deprecated due to NEWTCORE-360 fix
class PolicyPartitioner(object):
    def __init__(self, driver):
        self.driver = driver
        self.partitioning = partitioning.PartitioningAction(self.driver)

    def partition(self):
        policy = self.driver.partitions_policy
        LOG.debug("Using partitioning policy '%s'."
                  % self.driver.partitions_policy)

        policy_handlers = {
            "nailgun_legacy": self._handle_nailgun_legacy,
        }

        if policy not in policy_handlers:
            raise errors.WrongPartitionPolicyError(
                "'%s' policy is not one of known ones: %s"
                % (policy, ', '.join(policy_handlers)))

        policy_handlers[policy]()

    def _handle_nailgun_legacy(self):
        # Corresponds to nailgun behavior.
        self.partitioning.execute()


@six.add_metaclass(abc.ABCMeta)
class AbstractPartitionPolicy(object):
    space_allocation_accuracy = block_device.SizeUnit(1, 'MiB')

    def __init__(self, deploy, storage):
        self.deploy_driver = deploy
        self.storage_claim = storage

        self.dev_finder = block_device.DeviceFinder()

        self.partition = self._make_partition_plan()

    @abc.abstractmethod
    def __call__(self):
        pass

    def dev_by_guid(self, guid):
        needle = 'disk/by-partuuid/{}'.format(guid.lower())
        dev_info = self.dev_finder('path', needle)
        return dev_info['device']

    def _make_partition_plan(self):
        disks = {}
        for claim in self.storage_claim.items_by_kind(
                objects.block_device.Disk):
            LOG.info('Make partition plan for "%s"', claim.dev)
            disks[claim.dev] = self._disk_partition(claim)

        return disks

    def _disk_partition(self, claim):
        disk = block_device.Disk.new_by_scan(claim.dev, partitions=False)
        disk.allocate_accuracy = self.space_allocation_accuracy

        remaining = None
        from_tail = []
        for idx, claim in enumerate(claim.items):
            if claim.size.kind == claim.size.KIND_BIGGEST:
                remaining = claim
                from_tail = claim.items[idx + 1:]
                break
            self._apply_claim(disk, claim)

        from_tail.reverse()
        for claim in from_tail:
            self._apply_claim(disk, claim, from_tail=True)

        if remaining is not None:
            self._apply_claim(disk, remaining)

        return disk

    def _lvm_vg_partition(self, claim):
        vg = block_device.LVM.new_by_scan(claim.idnr, lv=False)
        vg.allocate_accuracy = self.space_allocation_accuracy

        remaining = None
        for lv in claim.items_by_kind(objects.block_device.LVMlv):
            if lv.size.kind == lv.size.KIND_BIGGEST:
                remaining = lv
                continue
            self._apply_claim(vg, lv)

        if remaining is not None:
            self._apply_claim(vg, remaining)

        return vg

    def _handle_filesystems(self):
        for claim in self.storage_claim.items_by_kind(
                objects.block_device.Disk):
            self._resolv_disk_partitions(claim)

        for claim in self.storage_claim.items_by_kind(
                objects.block_device.FileSystemMixin, recursion=True):
            self._make_filesystem(claim)

    def _resolv_disk_partitions(self, claim_disk):
        partition_disk = self.partition[claim_disk.dev]
        actual_disk = block_device.Disk.new_by_scan(claim_disk.dev)

        fuzzy_factor = actual_disk.sizeunit_to_blocks(
            self.space_allocation_accuracy)

        claim_segments = []
        actual_segments = []
        for storage, target in (
                (partition_disk, claim_segments),
                (actual_disk, actual_segments)):
            for segment in storage.segments:
                if segment.kind != segment.KIND_BUSY:
                    continue

                segment.set_fuzzy_cmp_factor(fuzzy_factor)
                target.append(segment)

        idx_iter = itertools.count()
        for claim, actual in itertools.izip_longest(
                claim_segments, actual_segments):
            idx = next(idx_iter)
            if claim == actual:
                if isinstance(
                        claim.payload, objects.block_device.Partition):
                    claim.payload.guid = actual.payload.guid
                continue

            raise errors.PartitionSchemeMismatchError(
                'Unable to resolv claim devices into physical devices. '
                'Claim and physical devices partitions are different. '
                '(dev={}, {}: {!r} != {!r})'.format(
                    claim_disk.dev, idx, claim, actual))

    def _make_filesystem(self, claim):
        if not claim.file_system:
            return

        if isinstance(claim, objects.block_device.Partition):
            dev = self.dev_by_guid(claim.guid)
        elif isinstance(claim, objects.block_device.MDRaid):
            dev = claim.name
        elif isinstance(claim, objects.block_device.LVMlv):
            dev = claim.dev
        else:
            raise errors.InternalError(exc_info=False)

        # FIXME(dbogun): label
        fu.make_fs(claim.file_system, '', '', dev)

    def _apply_claim(self, storage, claim, from_tail=False):
        segment = claim.size(storage, from_tail=from_tail)
        if isinstance(claim, objects.block_device.BlockDevice):
            if claim.is_service:
                segment.set_is_service()
        segment.payload = claim


class PartitionPolicyClean(AbstractPartitionPolicy):
    def __call__(self):
        LOG.info('Apply "clean" partitions policy')

        self._remove_all_compound_devices()

        for dev in sorted(self.partition):
            self._handle_disk(self.partition[dev])

        # update dev finder after all changes to disks
        self.dev_finder = block_device.DeviceFinder()

        for md in self.storage_claim.items_by_kind(
                objects.block_device.MDRaid):
            self._handle_mdraid(md)
        for vg in self.storage_claim.items_by_kind(objects.block_device.LVMvg):
            self._handle_lvm(vg)

        self._handle_filesystems()

    def _remove_all_compound_devices(self):
        mu.mdclean_all()
        lu.lvremove_all()
        lu.vgremove_all()
        lu.pvremove_all()

    def _handle_disk(self, disk):
        gdisk = block_device.GDisk(disk.dev)

        gdisk.zap()
        try:
            idx = itertools.count(1)
            for segment in disk.segments:
                if segment.is_free():
                    continue
                partition = block_device.Partition.new_by_disk_segment(
                    segment, next(idx), segment.payload.guid_code)
                partition.guid = segment.payload.guid
                segment.payload.guid = gdisk.new(partition)
        finally:
            pu.reread_partitions(disk.dev)

    def _handle_mdraid(self, md):
        components = set()
        for item in md.items:
            components.add(self.dev_by_guid(item.guid))

        mu.mdcreate(md.name, md.level, sorted(components))

    def _handle_lvm(self, vg):
        components = set()
        for pv in vg.items_by_kind(objects.block_device.LVMpv):
            dev = self.dev_by_guid(pv.guid)
            components.add(dev)

            args = {}
            if pv.meta_size is not None:
                args['metadatasize'] = pv.meta_size.size.in_unit(
                    'MiB').value_int
            lu.pvcreate(dev, **args)

        lu.vgcreate(vg.idnr, *sorted(components))

        partition = self._lvm_vg_partition(vg)
        for segment in partition.segments:
            if segment.kind != segment.KIND_BUSY:
                continue
            lu.lvcreate(vg.idnr, segment.payload.name, segment.size)

    def _resolv_disk_partitions(self, claim_dist):
        """Dummy to avoid already done operation

        Actual disk partition resolv have happened in __call__ method, during
        disks partitioning.
        """


class PartitionPolicyNailgun(PartitionPolicyClean):
    _respect_keep_data = True

    def __call__(self):
        self._respect_keep_data = self._check_keep_data_claim()
        if self._respect_keep_data:
            LOG.debug('Some of fs has keep_data (preserve) flag, '
                      'skipping partitioning')

            self._handle_filesystems()
            return

        super(PartitionPolicyNailgun, self).__call__()

    def _check_keep_data_claim(self):
        for item in itertools.chain(
                self.storage_claim.items_by_kind(
                    objects.block_device.FileSystemMixin, recursion=True),
                self.storage_claim.items_by_kind(objects.block_device.LVMvg)):
            if not item.keep_data_flag:
                continue
            break
        else:
            return False
        return True

    def _make_filesystem(self, claim):
        if self._respect_keep_data and claim.keep_data_flag:
            return

        super(PartitionPolicyNailgun, self)._make_filesystem(claim)


class PartitionPolicyVerify(AbstractPartitionPolicy):
    _lvm_fuzzy_cmp_factor = 2

    def __call__(self):
        LOG.info('Apply "verify" partitions policy')

        for dev in self.partition:
            self._handle_disk(self.partition[dev])

        for vg in self.storage_claim.items_by_kind(objects.block_device.LVMvg):
            self._handle_lvm(vg)

        self._handle_filesystems()

    def _handle_disk(self, disk):
        actual_disk = block_device.Disk.new_by_scan(disk.dev)
        actual_partition = self._grab_storage_segments(actual_disk)
        desired_partition = self._grab_storage_segments(disk)

        if actual_partition == desired_partition:
            return

        self._report_mismatch(disk.dev, desired_partition, actual_partition)

    def _handle_lvm(self, vg_claim):
        try:
            vg = block_device.LVM.new_by_scan(vg_claim.idnr)
            actual_partition = self._grab_storage_segments(
                vg, self._lvm_fuzzy_cmp_factor)

            vg = self._lvm_vg_partition(vg_claim)
            desired_partition = self._grab_storage_segments(
                vg, self._lvm_fuzzy_cmp_factor)
        except errors.VGNotFoundError:
            raise errors.PartitionSchemeMismatchError(
                'There is no LVMvg {}'.format(vg_claim.idnr))

        if actual_partition == desired_partition:
            return

        self._report_mismatch(
            vg_claim.idnr, desired_partition, actual_partition)

    def _grab_storage_segments(self, storage, factor=None):
        if factor is None:
            factor = storage.sizeunit_to_blocks(self.space_allocation_accuracy)

        result = []
        for segment in storage.segments:
            if segment.kind != segment.KIND_BUSY:
                continue
            segment.set_fuzzy_cmp_factor(factor)
            result.append(segment)

        return result

    # TODO(dbogun): increase verbosity
    def _report_mismatch(self, dev, desired, actual):
        raise errors.PartitionSchemeMismatchError(
            'Partition mismatch on {}'.format(dev))

    def _make_filesystem(self, claim):
        if claim.keep_data_flag:
            return

        super(PartitionPolicyVerify, self)._make_filesystem(claim)
