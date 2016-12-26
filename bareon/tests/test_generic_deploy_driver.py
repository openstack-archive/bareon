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

from collections import namedtuple
import mock
import os
import unittest2

from oslo_config import cfg

from bareon.drivers.deploy import generic
from bareon.objects.partition.fs import FileSystem

CONF = cfg.CONF


class TestDoReboot(unittest2.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestDoReboot, self).__init__(*args, **kwargs)
        self.driver = generic.GenericDeployDriver(None)

    @mock.patch('bareon.utils.utils.execute')
    def test_do_reboot(self, mock_execute):
        result = self.driver.do_reboot()
        self.assertIsNone(result)
        mock_execute.assert_called_once_with('reboot')


@unittest2.skip("Fix after cray rebase")
class TestDoProvision(unittest2.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestDoProvision, self).__init__(*args, **kwargs)
        self.driver = generic.GenericDeployDriver(None)

    def test_do_provision(self):
        self.driver.do_partitioning = mock_partitioning = mock.MagicMock()
        self.driver.do_configdrive = mock_configdrive = mock.MagicMock()
        self.driver.do_copyimage = mock_copyimage = mock.MagicMock()
        self.driver.do_bootloader = mock_bootloader = mock.MagicMock()

        result = self.driver.do_provisioning()
        self.assertIsNone(result)

        mock_partitioning.assert_called_once_with()
        mock_configdrive.assert_called_once_with()
        mock_copyimage.assert_called_once_with()
        mock_bootloader.assert_called_once_with()


class TestMountTarget(unittest2.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestMountTarget, self).__init__(*args, **kwargs)
        self.mock_data = mock.MagicMock()
        self.driver = generic.GenericDeployDriver(self.mock_data)
        self.fs_sorted = self.mock_data.partition_scheme.fs_sorted_by_depth

    @mock.patch('bareon.utils.fs.mount_bind')
    @mock.patch('bareon.utils.utils.makedirs_if_not_exists')
    def test_pseudo(self, mock_makedirs, mock_mount_bind):
        self.fs_sorted.return_value = []
        pseudo_fs = ('/sys', '/dev', '/proc')
        chroot = '/tmp/target'
        os_id = 'test'

        result = self.driver._mount_target(chroot, os_id, True,
                                           False)
        self.assertIsNone(result)
        mock_makedirs.assert_has_calls([mock.call(chroot + path)
                                        for path in pseudo_fs], any_order=True)
        mock_mount_bind.assert_has_calls([mock.call(chroot, path)
                                          for path in pseudo_fs],
                                         any_order=True)
        self.fs_sorted.assert_called_once_with(os_id)

    @mock.patch('os.path.islink', return_value=False)
    @mock.patch('bareon.utils.utils.execute')
    @mock.patch('__builtin__.open')
    def test_treat_mtab_no_link(self, mock_open, mock_execute, mock_islink):
        chroot = '/tmp/target'
        os_id = 'test'

        mock_open.return_value = context_manager = mock.MagicMock()
        context_manager.__enter__.return_value = file_mock = mock.MagicMock()

        mock_execute.return_value = mtab = ('mtab',)

        result = self.driver._mount_target(chroot, os_id, False,
                                           True)

        self.assertIsNone(result)
        mock_execute.assert_called_once_with('chroot', chroot, 'grep', '-v',
                                             'rootfs', '/proc/mounts')
        mock_islink.assert_called_once_with(chroot + '/etc/mtab')
        file_mock.assert_has_calls([mock.call.write(mtab[0])], any_order=True)

    @mock.patch('os.remove')
    @mock.patch('os.path.islink', return_value=True)
    @mock.patch('bareon.utils.utils.execute')
    @mock.patch('__builtin__.open')
    def test_treat_mtab_link(self, mock_open, mock_execute, mock_islink,
                             mock_remove):
        chroot = '/tmp/target'
        os_id = 'test'

        mock_open.return_value = context_manager = mock.MagicMock()
        context_manager.__enter__.return_value = file_mock = mock.MagicMock()

        mock_execute.return_value = mtab = ('mtab',)

        result = self.driver._mount_target(chroot, os_id, False,
                                           True)

        self.assertIsNone(result)
        mock_execute.assert_called_once_with('chroot', chroot, 'grep', '-v',
                                             'rootfs', '/proc/mounts')
        mock_islink.assert_called_once_with(chroot + '/etc/mtab')
        mock_remove.assert_called_once_with(chroot + '/etc/mtab')
        file_mock.assert_has_calls([mock.call.write(mtab[0])], any_order=True)

    @mock.patch('bareon.utils.fs.mount_fs')
    @mock.patch('bareon.utils.utils.makedirs_if_not_exists')
    def test_partition_swap(self, mock_makedirs, mock_mount):
        chroot = '/tmp/target/'
        os_id = 'test'

        fs = namedtuple('fs', 'mount type device')
        fss = [fs(mount='swap', type='swap', device='/dev/sdc'),
               fs(mount='/', type='ext4', device='/dev/sda'),
               fs(mount='/usr', type='ext4', device='/dev/sdb')]
        self.fs_sorted.return_value = fss

        result = self.driver._mount_target(chroot, os_id, False,
                                           False)

        self.assertIsNone(result)
        mock_makedirs.assert_has_calls(
            [mock.call(os.path.join(chroot, f.mount.strip(os.sep))) for f
             in fss[1:]], any_order=True)
        mock_mount.assert_has_calls(
            [mock.call(f.type, str(f.device),
                       os.path.join(chroot, f.mount.strip(os.sep))) for f
             in fss[1:]], any_order=True)
        self.fs_sorted.assert_called_once_with(os_id)

    @mock.patch('bareon.utils.fs.mount_fs')
    @mock.patch('bareon.utils.utils.makedirs_if_not_exists')
    def test_partition(self, mock_makedirs, mock_mount):
        chroot = '/tmp/target/'
        os_id = 'test'
        fs = namedtuple('fs', 'mount type device')
        fss = [fs(mount='/', type='ext4', device='/dev/sda'),
               fs(mount='/usr', type='ext4', device='/dev/sdb')]
        self.fs_sorted.return_value = fss

        result = self.driver._mount_target(chroot, os_id, False,
                                           False)

        self.assertIsNone(result)
        mock_makedirs.assert_has_calls(
            [mock.call(os.path.join(chroot, f.mount.strip(os.sep))) for f
             in fss], any_order=True)
        mock_mount.assert_has_calls(
            [mock.call(f.type, str(f.device),
                       os.path.join(chroot, f.mount.strip(os.sep))) for f
             in fss], any_order=True)
        self.fs_sorted.assert_called_once_with(os_id)


class TestUmountTarget(unittest2.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestUmountTarget, self).__init__(*args, **kwargs)
        self.mock_data = mock.MagicMock()
        self.driver = generic.GenericDeployDriver(self.mock_data)
        self.fs_sorted = self.mock_data.partition_scheme.fs_sorted_by_depth

    @mock.patch('bareon.utils.fs.umount_fs')
    def test_pseudo(self, mock_umount_fs):
        self.fs_sorted.return_value = []
        pseudo_fs = ('/sys', '/dev', '/proc')
        chroot = '/tmp/target'

        result = self.driver._umount_target(chroot, True)

        self.assertIsNone(result)
        mock_umount_fs.assert_has_calls(
            [mock.call(chroot + path, try_lazy_umount=True) for path in
             pseudo_fs], any_order=True)

    @mock.patch('bareon.utils.fs.umount_fs')
    def test_partition(self, mock_umount):
        chroot = '/tmp/target/'
        os_id = 'test'
        fs = namedtuple('fs', 'mount type device')
        fss = [fs(mount='/', type='ext4', device='/dev/sda'),
               fs(mount='/usr', type='ext4', device='/dev/sdb')]
        self.fs_sorted.return_value = fss

        result = self.driver._umount_target(chroot, os_id, False)
        self.assertIsNone(result)
        mock_umount.assert_has_calls(
            [mock.call(os.path.join(chroot, f.mount.strip(os.sep))) for f
             in fss], any_order=True)
        self.fs_sorted.assert_called_once_with(os_id, True)

    @mock.patch('bareon.utils.fs.umount_fs')
    def test_partition_swap(self, mock_umount):
        chroot = '/tmp/target/'
        os_id = 'test'
        fs = namedtuple('fs', 'mount type device')
        fss = [fs(mount='swap', type='swap', device='/dev/sdc'),
               fs(mount='/', type='ext4', device='/dev/sda'),
               fs(mount='/usr', type='ext4', device='/dev/sdb')]
        self.fs_sorted.return_value = fss

        result = self.driver._umount_target(chroot, os_id, False)

        self.assertIsNone(result)
        mock_umount.assert_has_calls(
            [mock.call(os.path.join(chroot, f.mount.strip(os.sep))) for f
             in fss[1:]], any_order=True)
        self.fs_sorted.assert_called_once_with(os_id, True)


class TestDoBootloader(unittest2.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestDoBootloader, self).__init__(*args, **kwargs)
        self.mock_data = mock.MagicMock()
        self.driver = generic.GenericDeployDriver(self.mock_data)
        self.driver._generate_fstab = mock.MagicMock()
        self.mock_mount = self.driver.mount_target = mock.MagicMock()
        self.mock_umount = self.driver._umount_target = mock.MagicMock()
        self.mock_grub = self.mock_data.grub

    @mock.patch('bareon.utils.grub.grub2_install')
    @mock.patch('bareon.utils.grub.grub2_cfg')
    @mock.patch('bareon.utils.grub.guess_grub_version')
    @mock.patch('bareon.utils.utils.execute')
    def test_wrong_version_grub_2(self, mock_execute, mock_guess_grub,
                                  mock_grub2_cfg, mock_grub2_install):
        chroot = '/tmp/target'
        os_id = 'test'
        fs = namedtuple('fs', 'mount type device os_id')
        fss = [fs(mount='swap', type='swap', device='/dev/sdc', os_id=[os_id]),
               fs(mount='/', type='ext4', device='/dev/sda', os_id=[os_id]),
               fs(mount='/usr', type='ext4', device='/dev/sdb', os_id=[os_id])]
        self.mock_data.partition_scheme.fss = fss
        self.mock_data.boot_on_lvm = mock.Mock()
        mock_execute.side_effect = [('uuid1',), ('uuid2',), ('uuid3',)]
        self.mock_grub.version = 1
        mock_guess_grub.return_value = 2
        self.mock_grub.kernel_name = 'kernel_name'
        self.mock_grub.initrd_name = 'initrd_name'
        self.mock_grub.kernel_params = kernel_params = 'params'
        self.mock_data.partition_scheme.boot_device.return_value = fss[
            1].device

        result = self.driver.do_singleboot_bootloader(chroot, os_id)

        self.assertIsNone(result)
        mock_grub2_cfg.assert_called_once_with(
            kernel_params=kernel_params,
            chroot=chroot,
            grub_timeout=CONF.grub_timeout,
            lvm_boot=self.mock_data.boot_on_lvm)
        mock_grub2_install.assert_called_once_with(
            [], chroot=chroot,
            lvm_boot=self.mock_data.boot_on_lvm)

    @mock.patch('bareon.utils.grub.grub1_install')
    @mock.patch('bareon.utils.grub.grub1_cfg')
    @mock.patch('bareon.utils.grub.guess_grub_version')
    @mock.patch('bareon.utils.utils.execute')
    def test_wrong_version_grub_1(self, mock_execute, mock_guess_grub,
                                  mock_grub1_cfg, mock_grub1_install):
        chroot = '/tmp/target'
        os_id = 'test'
        fs = namedtuple('fs', 'mount type device os_id')
        fss = [fs(mount='swap', type='swap', device='/dev/sdc', os_id=[os_id]),
               fs(mount='/', type='ext4', device='/dev/sda', os_id=[os_id]),
               fs(mount='/usr', type='ext4', device='/dev/sdb', os_id=[os_id])]
        self.mock_data.partition_scheme.fs_by_os_id.return_value = fss
        self.mock_data.boot_on_lvm = None
        mock_execute.side_effect = [('uuid1',), ('uuid2',), ('uuid3',)]
        self.mock_grub.version = 2
        mock_guess_grub.return_value = 1
        self.mock_grub.kernel_name = kernel_name = 'kernel_name'
        self.mock_grub.initrd_name = initrd_name = 'initrd_name'
        self.mock_grub.kernel_params = kernel_params = 'params'
        self.mock_data.partition_scheme.boot_device.return_value = fss[
            1].device

        result = self.driver.do_singleboot_bootloader(chroot, os_id)

        self.assertIsNone(result)
        mock_grub1_cfg.assert_called_once_with(kernel=kernel_name,
                                               initrd=initrd_name,
                                               kernel_params=kernel_params,
                                               chroot=chroot,
                                               grub_timeout=CONF.grub_timeout)
        mock_grub1_install.assert_called_once_with([], '/dev/sda',
                                                   chroot=chroot)

    @mock.patch('bareon.utils.grub.grub2_install')
    @mock.patch('bareon.utils.grub.grub2_cfg')
    @mock.patch('bareon.utils.grub.guess_grub_version')
    @mock.patch('bareon.utils.utils.execute')
    def test_grub_2(self, mock_execute, mock_guess_grub, mock_grub2_cfg,
                    mock_grub2_install):
        chroot = '/tmp/target'
        os_id = 'test'
        fs = namedtuple('fs', 'mount type device os_id')
        fss = [fs(mount='swap', type='swap', device='/dev/sdc', os_id=[os_id]),
               fs(mount='/', type='ext4', device='/dev/sda', os_id=[os_id]),
               fs(mount='/usr', type='ext4', device='/dev/sdb', os_id=[os_id])]
        self.mock_data.partition_scheme.fss = fss
        self.mock_data.boot_on_lvm = None
        mock_execute.side_effect = [('uuid1',), ('uuid2',), ('uuid3',)]
        self.mock_grub.version = mock_guess_grub.return_value = 2
        self.mock_grub.kernel_name = 'kernel_name'
        self.mock_grub.initrd_name = 'initrd_name'
        self.mock_grub.kernel_params = kernel_params = 'params'
        self.mock_data.partition_scheme.boot_device.return_value = fss[
            1].device

        result = self.driver.do_singleboot_bootloader(chroot, os_id)

        self.assertIsNone(result)
        mock_grub2_cfg.assert_called_once_with(kernel_params=kernel_params,
                                               chroot=chroot,
                                               grub_timeout=CONF.grub_timeout,
                                               lvm_boot=None)
        mock_grub2_install.assert_called_once_with([], chroot=chroot,
                                                   lvm_boot=None)

    @mock.patch('bareon.utils.grub.grub1_install')
    @mock.patch('bareon.utils.grub.grub1_cfg')
    @mock.patch('bareon.utils.grub.guess_grub_version')
    @mock.patch('bareon.utils.utils.execute')
    def test_grub_1(self, mock_execute, mock_guess_grub, mock_grub1_cfg,
                    mock_grub1_install):
        chroot = '/tmp/target'
        os_id = 'test'
        fs = namedtuple('fs', 'mount type device os_id')
        fss = [fs(mount='swap', type='swap', device='/dev/sdc', os_id=[os_id]),
               fs(mount='/', type='ext4', device='/dev/sda', os_id=[os_id]),
               fs(mount='/usr', type='ext4', device='/dev/sdb', os_id=[os_id])]
        self.mock_data.partition_scheme.fs_by_os_id.return_value = fss
        mock_execute.side_effect = [('uuid1',), ('uuid2',), ('uuid3',)]
        self.mock_grub.version = mock_guess_grub.return_value = 1
        self.mock_grub.kernel_name = kernel_name = 'kernel_name'
        self.mock_grub.initrd_name = initrd_name = 'initrd_name'
        self.mock_grub.kernel_params = kernel_params = 'params'
        self.mock_data.partition_scheme.boot_device.return_value = fss[
            1].device
        self.mock_data.boot_on_lvm = None

        result = self.driver.do_singleboot_bootloader(chroot, os_id)

        self.assertIsNone(result)
        mock_grub1_cfg.assert_called_once_with(kernel=kernel_name,
                                               initrd=initrd_name,
                                               kernel_params=kernel_params,
                                               chroot=chroot,
                                               grub_timeout=CONF.grub_timeout)
        mock_grub1_install.assert_called_once_with([], '/dev/sda',
                                                   chroot=chroot)

    @mock.patch('bareon.utils.grub.guess_initrd')
    @mock.patch('bareon.utils.grub.guess_kernel')
    @mock.patch('bareon.utils.grub.grub1_install')
    @mock.patch('bareon.utils.grub.grub1_cfg')
    @mock.patch('bareon.utils.grub.guess_grub_version')
    @mock.patch('bareon.utils.utils.execute')
    def test_grub1_nokernel_noinitrd(self, mock_execute, mock_guess_grub,
                                     mock_grub1_cfg, mock_grub1_install,
                                     mock_guess_kernel, mock_guess_initrd):
        chroot = '/tmp/target'
        os_id = 'test'
        fs = namedtuple('fs', 'mount type device os_id')
        fss = [fs(mount='swap', type='swap', device='/dev/sdc', os_id=[os_id]),
               fs(mount='/', type='ext4', device='/dev/sda', os_id=[os_id]),
               fs(mount='/usr', type='ext4', device='/dev/sdb', os_id=[os_id])]
        self.mock_data.partition_scheme.fs_by_os_id.return_value = fss
        mock_execute.side_effect = [('uuid1',), ('uuid2',), ('uuid3',)]
        self.mock_grub.version = mock_guess_grub.return_value = 1
        self.mock_grub.kernel_name = None
        self.mock_grub.initrd_name = None
        self.mock_grub.kernel_regexp = kernel_regex = 'kernel_regex'
        self.mock_grub.initrd_regexp = initrd_regex = 'initrd_regex'
        self.mock_grub.kernel_params = kernel_params = 'params'
        self.mock_data.partition_scheme.boot_device.return_value = fss[
            1].device
        self.mock_data.boot_on_lvm = None
        mock_guess_kernel.return_value = kernel_name = 'kernel_name'
        mock_guess_initrd.return_value = initrd_name = 'initrd_name'

        result = self.driver.do_singleboot_bootloader(chroot, os_id)

        self.assertIsNone(result)
        mock_grub1_cfg.assert_called_once_with(kernel=kernel_name,
                                               initrd=initrd_name,
                                               kernel_params=kernel_params,
                                               chroot=chroot,
                                               grub_timeout=CONF.grub_timeout)
        mock_grub1_install.assert_called_once_with([], '/dev/sda',
                                                   chroot=chroot)
        mock_guess_kernel.assert_called_once_with(chroot=chroot,
                                                  regexp=kernel_regex)
        mock_guess_initrd.assert_called_once_with(chroot=chroot,
                                                  regexp=initrd_regex)


class TestGenerateFstab(unittest2.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestGenerateFstab, self).__init__(*args, **kwargs)
        self.mock_data = mock.MagicMock()
        self.driver = generic.GenericDeployDriver(self.mock_data)

    @mock.patch('os.path.exists', return_value=True)
    @mock.patch('__builtin__.open')
    def test_success(self, mock_open, mock_execute):
        chroot = '/tmp/target'
        os_id = 'test'

        fss = [FileSystem('/dev/sdc', mount='swap', fs_type='swap',
                          os_id=[os_id]),
               FileSystem('/dev/sda', mount='/', fs_type='ext4',
                          os_id=[os_id]),
               FileSystem('/dev/sdb', mount='/usr', fs_type='ext4',
                          os_id=[os_id])]
        self.mock_data.partition_scheme.fs_by_os_id.return_value = fss
        self.driver._mount2uuid = mock_mount2uuid = mock.MagicMock()
        mock_mount2uuid.return_value = {fs.mount: id for id, fs in
                                        enumerate(fss)}

        mock_open.return_value = context_manager = mock.MagicMock()
        context_manager.__enter__.return_value = file_mock = mock.MagicMock()

        result = self.driver.do_generate_fstab(chroot, 'test')

        self.assertIsNone(result)

        file_mock.assert_has_calls(
            [mock.call.write('UUID=0 swap swap defaults 0 0\n'),
             mock.call.write('UUID=1 / ext4 defaults 0 0\n'),
             mock.call.write('UUID=2 /usr ext4 defaults 0 0\n')],
            any_order=True)

    @mock.patch('os.path.exists', return_value=True)
    @mock.patch('__builtin__.open')
    def test_fstab_disabled(self, mock_open, mock_execute):
        chroot = '/tmp/target'
        os_id = 'test'
        fss = [FileSystem('/dev/sdc', mount='swap', fs_type='swap',
                          os_id=[os_id]),
               FileSystem('/dev/sda', mount='/', fs_type='ext4',
                          fstab_enabled=True, os_id=[os_id]),
               FileSystem('/dev/sdb', mount='/usr', fs_type='ext4',
                          fstab_enabled=False, os_id=[os_id])]
        self.mock_data.partition_scheme.fs_by_os_id.return_value = fss
        self.driver._mount2uuid = mock_mount2uuid = mock.MagicMock()
        mock_mount2uuid.return_value = {fs.mount: id for id, fs in
                                        enumerate(fss)}

        mock_open.return_value = context_manager = mock.MagicMock()
        context_manager.__enter__.return_value = file_mock = mock.MagicMock()

        result = self.driver.do_generate_fstab(chroot, 'test')

        self.assertIsNone(result)

        file_mock.assert_has_calls(
            [mock.call.write('UUID=0 swap swap defaults 0 0\n'),
             mock.call.write('UUID=1 / ext4 defaults 0 0\n'),
             mock.call.write('#UUID=2 /usr ext4 defaults 0 0\n')],
            any_order=True)

    @mock.patch('os.path.exists', return_value=True)
    @mock.patch('__builtin__.open')
    def test_fstab_options(self, mock_open, mock_execute):
        chroot = '/tmp/target'
        os_id = 'test'

        fss = [FileSystem('/dev/sdc', mount='swap', fs_type='swap',
                          os_id=[os_id]),
               FileSystem('/dev/sda', mount='/', fs_type='ext4',
                          fstab_options='defaults', os_id=[os_id]),
               FileSystem('/dev/sdb', mount='/usr', fs_type='ext4',
                          fstab_options='noatime', os_id=[os_id])]
        self.mock_data.partition_scheme.fs_by_os_id.return_value = fss
        self.driver._mount2uuid = mock_mount2uuid = mock.MagicMock()
        mock_mount2uuid.return_value = {fs.mount: id for id, fs in
                                        enumerate(fss)}

        mock_open.return_value = context_manager = mock.MagicMock()
        context_manager.__enter__.return_value = file_mock = mock.MagicMock()

        result = self.driver.do_generate_fstab(chroot, 'test')

        self.assertIsNone(result)

        file_mock.assert_has_calls(
            [mock.call.write('UUID=0 swap swap defaults 0 0\n'),
             mock.call.write('UUID=1 / ext4 defaults 0 0\n'),
             mock.call.write('UUID=2 /usr ext4 noatime 0 0\n')],
            any_order=True)
