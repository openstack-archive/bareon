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

import json
import os
import re

from contextlib import contextmanager

from oslo_config import cfg

from bareon.drivers.deploy.base import BaseDeployDriver
from bareon import errors
from bareon.openstack.common import log as logging
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
class GenericDeployDriver(BaseDeployDriver):

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
        PolicyPartitioner(self.driver).partition()
        LOG.debug('--- Partitioning disks END (do_partitioning) ---')

    def do_configdrive(self):
        self.driver.create_configdrive()

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

    def _mount_target(self, mount_dir, os_id, pseudo=True, treat_mtab=True):
        LOG.debug('Mounting target file systems: %s', mount_dir)
        # Here we are going to mount all file systems in partition schema.
        for fs in self.driver.partition_scheme.fs_sorted_by_depth(os_id):
            if fs.mount == 'swap':
                continue
            mount = os.path.join(mount_dir, fs.mount.strip(os.sep))
            utils.makedirs_if_not_exists(mount)
            fu.mount_fs(fs.type, str(fs.device), mount)

        if pseudo:
            for path in ('/sys', '/dev', '/proc'):
                utils.makedirs_if_not_exists(
                    os.path.join(mount_dir, path.strip(os.sep)))
                fu.mount_bind(mount_dir, path)

        if treat_mtab:
            mtab = utils.execute('chroot', mount_dir, 'grep', '-v', 'rootfs',
                                 '/proc/mounts')[0]
            mtab_path = os.path.join(mount_dir, 'etc/mtab')
            if os.path.islink(mtab_path):
                os.remove(mtab_path)
            with open(mtab_path, 'wb') as f:
                f.write(mtab)

    def _umount_target(self, mount_dir, os_id, pseudo=True):
        LOG.debug('Umounting target file systems: %s', mount_dir)
        if pseudo:
            for path in ('/proc', '/dev', '/sys'):
                fu.umount_fs(os.path.join(mount_dir, path.strip(os.sep)),
                             try_lazy_umount=True)
        for fs in self.driver.partition_scheme.fs_sorted_by_depth(os_id,
                                                                  True):
            if fs.mount == 'swap':
                continue
            fu.umount_fs(os.path.join(mount_dir, fs.mount.strip(os.sep)))

    @contextmanager
    def mount_target(self, mount_dir, os_id, pseudo=True, treat_mtab=True):
        self._mount_target(mount_dir, os_id, pseudo=pseudo,
                           treat_mtab=treat_mtab)
        try:
            yield
        finally:
            self._umount_target(mount_dir, os_id, pseudo)

    @contextmanager
    def _mount_bootloader(self, mount_dir):
        fs = filter(lambda fss: fss.mount == 'multiboot',
                    self.driver.partition_scheme.fss)
        if len(fs) > 1:
            raise errors.WrongPartitionSchemeError(
                'Multiple multiboot partitions found')

        utils.makedirs_if_not_exists(mount_dir)
        fu.mount_fs(fs[0].type, str(fs[0].device), mount_dir)

        yield pu.get_uuid(fs[0].device)

        fu.umount_fs(mount_dir)


class PolicyPartitioner(object):

    def __init__(self, driver):
        self.driver = driver

    def partition(self):
        policy = self.driver.partitions_policy
        LOG.debug("Using partitioning policy '%s'."
                  % self.driver.partitions_policy)

        policy_handlers = {
            "verify": self._handle_verify,
            "clean": self._handle_clean,
            "nailgun_legacy": self._handle_nailgun_legacy,
        }

        known_policies = policy_handlers.keys()
        if policy not in known_policies:
            raise errors.WrongPartitionPolicyError(
                "'%s' policy is not one of known ones: %s"
                % (policy, known_policies))

        policy_handlers[policy]()

    def _handle_verify(self):
        provision_schema = self.driver.partition_scheme.to_dict()
        hw_schema = self.driver.hw_partition_scheme.to_dict()
        PartitionSchemaCompareTool().assert_no_diff(provision_schema,
                                                    hw_schema)
        self._do_clean_filesystems()

    @staticmethod
    def _verify_disk_size(parteds, hu_disks):
        for parted in parteds:
            disks = [d for d in hu_disks if d.get('name') == parted.name]
            if not disks:
                raise errors.DiskNotFoundError(
                    'No physical disks found matching: %s' % parted.name)
            disk_size_bytes = disks[0].get('bspec', {}).get('size64')
            if not disk_size_bytes:
                raise ValueError('Cannot read size of the disk: %s'
                                 % disks[0].get('name'))
            # It's safer to understate the physical disk size
            disk_size_mib = utils.B2MiB(disk_size_bytes, ceil=False)
            if parted.disk_size > disk_size_mib:
                raise errors.NotEnoughSpaceError(
                    'Partition scheme for: %(disk)s exceeds the size of the '
                    'disk. Scheme size is %(scheme_size)s MiB, and disk size '
                    'is %(disk_size)s MiB.' % {
                        'disk': parted.name, 'scheme_size': parted.disk_size,
                        'disk_size': disk_size_mib})

    def _handle_clean(self):
        self._verify_disk_size(self.driver.partition_scheme.parteds,
                               self.driver.hu_disks)
        self._do_partitioning()

    def _handle_nailgun_legacy(self):
        # Corresponds to nailgun behavior.
        if self.driver.partition_scheme.skip_partitioning:
            LOG.debug('Some of fs has keep_data (preserve) flag, '
                      'skipping partitioning')
            self._do_clean_filesystems()
        else:
            LOG.debug('No keep_data (preserve) flag passed, wiping out all'
                      'disks and re-partitioning')
            self._do_partitioning()

    def _do_clean_filesystems(self):
        # NOTE(agordeev): it turns out that only mkfs.xfs needs '-f' flag in
        # order to force recreation of filesystem.
        # This option will be added to mkfs.xfs call explicitly in fs utils.
        # TODO(asvechnikov): need to refactor processing keep_flag logic when
        # data model will become flat
        for fs in self.driver.partition_scheme.fss:
            if not fs.keep_data:
                fu.make_fs(fs.type, fs.options, fs.label, fs.device)

    def _do_partitioning(self):
        # If disks are not wiped out at all, it is likely they contain lvm
        # and md metadata which will prevent re-creating a partition table
        # with 'device is busy' error.
        mu.mdclean_all()
        lu.lvremove_all()
        lu.vgremove_all()
        lu.pvremove_all()

        LOG.debug("Enabling udev's rules blacklisting")
        utils.blacklist_udev_rules(udev_rules_dir=CONF.udev_rules_dir,
                                   udev_rules_lib_dir=CONF.udev_rules_lib_dir,
                                   udev_rename_substr=CONF.udev_rename_substr,
                                   udev_empty_rule=CONF.udev_empty_rule)

        for parted in self.driver.partition_scheme.parteds:
            for prt in parted.partitions:
                # We wipe out the beginning of every new partition
                # right after creating it. It allows us to avoid possible
                # interactive dialog if some data (metadata or file system)
                # present on this new partition and it also allows udev not
                # hanging trying to parse this data.
                utils.execute('dd', 'if=/dev/zero', 'bs=1M',
                              'seek=%s' % max(prt.begin - 3, 0), 'count=5',
                              'of=%s' % prt.device, check_exit_code=[0])
                # Also wipe out the ending of every new partition.
                # Different versions of md stores metadata in different places.
                # Adding exit code 1 to be accepted as for handling situation
                # when 'no space left on device' occurs.
                utils.execute('dd', 'if=/dev/zero', 'bs=1M',
                              'seek=%s' % max(prt.end - 3, 0), 'count=5',
                              'of=%s' % prt.device, check_exit_code=[0, 1])

        for parted in self.driver.partition_scheme.parteds:
            pu.make_label(parted.name, parted.label)
            for prt in parted.partitions:
                pu.make_partition(prt.device, prt.begin, prt.end, prt.type)
                for flag in prt.flags:
                    pu.set_partition_flag(prt.device, prt.count, flag)
                if prt.guid:
                    pu.set_gpt_type(prt.device, prt.count, prt.guid)
                # If any partition to be created doesn't exist it's an error.
                # Probably it's again 'device or resource busy' issue.
                if not os.path.exists(prt.name):
                    raise errors.PartitionNotFoundError(
                        'Partition %s not found after creation' % prt.name)

        LOG.debug("Disabling udev's rules blacklisting")
        utils.unblacklist_udev_rules(
            udev_rules_dir=CONF.udev_rules_dir,
            udev_rename_substr=CONF.udev_rename_substr)

        # If one creates partitions with the same boundaries as last time,
        # there might be md and lvm metadata on those partitions. To prevent
        # failing of creating md and lvm devices we need to make sure
        # unused metadata are wiped out.
        mu.mdclean_all()
        lu.lvremove_all()
        lu.vgremove_all()
        lu.pvremove_all()

        # creating meta disks
        for md in self.driver.partition_scheme.mds:
            mu.mdcreate(md.name, md.level, md.devices, md.metadata)

        # creating physical volumes
        for pv in self.driver.partition_scheme.pvs:
            lu.pvcreate(pv.name, metadatasize=pv.metadatasize,
                        metadatacopies=pv.metadatacopies)

        # creating volume groups
        for vg in self.driver.partition_scheme.vgs:
            lu.vgcreate(vg.name, *vg.pvnames)

        # creating logical volumes
        for lv in self.driver.partition_scheme.lvs:
            lu.lvcreate(lv.vgname, lv.name, lv.size)

        # making file systems
        for fs in self.driver.partition_scheme.fss:
            found_images = [img for img in self.driver.image_scheme.images
                            if img.target_device == fs.device]
            if not found_images:
                fu.make_fs(fs.type, fs.options, fs.label, fs.device)


class PartitionSchemaCompareTool(object):

    def assert_no_diff(self, user_schema, hw_schema):
        usr_sch = self._prepare_user_schema(user_schema, hw_schema)
        hw_sch = self._prepare_hw_schema(user_schema, hw_schema)
        # NOTE(lobur): this may not work on bm hardware: because of the
        # partition alignments sizes may not match precisely, so need to
        # write own diff tool
        if not usr_sch == hw_sch:
            diff_str = utils.dict_diff(usr_sch, hw_sch,
                                       "user_schema", "hw_schema")
            raise errors.PartitionSchemeMismatchError(diff_str)
        LOG.debug("hw_schema and user_schema matched")

    def _prepare_user_schema(self, user_schema, hw_schema):
        LOG.debug('Preparing user_schema for verification:\n%s' %
                  user_schema)
        # Set all keep_data (preserve) flags to false.
        # They are just instructions to deploy driver and do not stored on
        # resulting partitions, so we have no means to read them from
        # hw_schema
        for fs in user_schema['fss']:
            fs['keep_data'] = False
            fs['os_id'] = []
        for parted in user_schema['parteds']:
            for part in parted['partitions']:
                part['keep_data'] = False

        self._drop_schema_size(user_schema)

        LOG.debug('Prepared user_schema is:\n%s' % user_schema)
        return user_schema

    @staticmethod
    def _drop_schema_size(schema):
        # If it exists, drop the schema size attribute. This should
        # be valid because doing a full layout comparison implicitly
        # involves verification of the disk sizes, and we don't need
        # to check those separately.
        for parted in schema['parteds']:
            parted.pop('size', None)

    def _prepare_hw_schema(self, user_schema, hw_schema):
        LOG.debug('Preparing hw_schema to verification:\n%s' %
                  hw_schema)

        user_disks = [p['name'] for p in user_schema['parteds']]

        # Ignore disks which are not mentioned in user_schema
        filtered_disks = []
        for disk in hw_schema['parteds']:
            if disk['name'] in user_disks:
                filtered_disks.append(disk)
            else:
                LOG.info("Node disk '%s' is not mentioned in deploy_config"
                         " thus it will be skipped." % disk['name'])
        hw_schema['parteds'] = filtered_disks

        # Ignore filesystems that belong to disk not mentioned in user_schema
        filtered_fss = []
        for fs in hw_schema['fss']:
            if fs['device'].rstrip("0123456789") in user_disks:
                filtered_fss.append(fs)
            else:
                LOG.info("Node filesystem '%s' belongs to disk not mentioned"
                         " in deploy_config thus it will be skipped."
                         % fs['device'])
        hw_schema['fss'] = filtered_fss

        # Transform filesystem types
        for fs in hw_schema['fss']:
            fs['fs_type'] = self._transform_fs_type(fs['fs_type'])
            fs['os_id'] = []

        self._drop_schema_size(hw_schema)

        LOG.debug('Prepared hw_schema is:\n%s' % hw_schema)
        return hw_schema

    def _transform_fs_type(self, hw_fs_type):
        # hw fstype name pattern -> fstype name in user schema
        hw_fs_to_user_fs_map = {
            'linux-swap': 'swap'
        }

        for hw_fs_pattern, usr_schema_val in hw_fs_to_user_fs_map.iteritems():
            if hw_fs_pattern in hw_fs_type:
                LOG.info("Node fs type '%s' is transformed to the user "
                         "schema type as '%s'."
                         % (hw_fs_type, usr_schema_val))
                return usr_schema_val

        return hw_fs_type
