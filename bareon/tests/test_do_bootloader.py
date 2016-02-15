# Copyright 2016 Mirantis, Inc.
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

import six
import unittest2

from bareon.actions import bootloader
from bareon.drivers.data import nailgun
from bareon import errors
from bareon import objects

if six.PY2:
    import mock
elif six.PY3:
    import unittest.mock as mock


class TestBootLoaderAction(unittest2.TestCase):

    def setUp(self):
        super(TestBootLoaderAction, self).setUp()
        self.drv = mock.MagicMock(spec=nailgun.Nailgun)
        self.action = bootloader.BootLoaderAction(self.drv)
        self.action._mount_target = mock.Mock()
        self.action._umount_target = mock.Mock()
        self.drv.grub = objects.Grub(
            kernel_params=' console=ttyS0,9600 console=tty0 '
                          'rootdelay=90 nomodeset')
        root_fs = objects.FS('/dev/sda', mount='/')
        self.drv.partition_scheme.fss = [root_fs]
        self.drv.partition_scheme.boot_device.return_value = '/dev/sda3'
        parteds = [objects.Parted('/dev/sd%s' % x, 'gpt',
                                  install_bootloader=True)
                   for x in ['a', 'b', 'c']]
        self.drv.partition_scheme.parteds = parteds

    @mock.patch.object(bootloader, 'open', create=True,
                       new_callable=mock.mock_open)
    @mock.patch.object(bootloader, 'gu', autospec=True)
    @mock.patch.object(bootloader, 'utils', autospec=True)
    def test_do_bootloader_grub1_kernel_initrd_guessed(self, mock_utils,
                                                       mock_gu, mock_open):
        mock_utils.execute.return_value = ('fake_root_uuid', '')
        mock_gu.guess_grub_version.return_value = 1
        # grub has kernel_name and initrd_name both set to None
        self.drv.grub.kernel_name = None
        self.drv.grub.initrd_name = None
        self.drv.grub.kernel_params = 'fake_kernel_params'
        self.drv.grub.kernel_regexp = 'fake_kernel_regexp'
        self.drv.grub.initrd_regexp = 'fake_initrd_regexp'
        mock_gu.guess_kernel.return_value = 'guessed_kernel'
        mock_gu.guess_initrd.return_value = 'guessed_initrd'
        self.action.execute()
        self.assertFalse(mock_gu.grub2_cfg.called)
        self.assertFalse(mock_gu.grub2_install.called)
        mock_gu.grub1_cfg.assert_called_once_with(
            kernel_params='fake_kernel_params root=UUID=fake_root_uuid ',
            initrd='guessed_initrd', kernel='guessed_kernel',
            chroot='/tmp/target', grub_timeout=10)
        mock_gu.grub1_install.assert_called_once_with(
            ['/dev/sda', '/dev/sdb', '/dev/sdc'],
            '/dev/sda3', chroot='/tmp/target')
        mock_gu.guess_initrd.assert_called_once_with(
            regexp='fake_initrd_regexp', chroot='/tmp/target')
        mock_gu.guess_kernel.assert_called_once_with(
            regexp='fake_kernel_regexp', chroot='/tmp/target')

    @mock.patch.object(bootloader, 'open', create=True,
                       new_callable=mock.mock_open)
    @mock.patch.object(bootloader, 'gu', autospec=True)
    @mock.patch.object(bootloader, 'utils', autospec=True)
    def test_do_bootloader_grub1_kernel_initrd_set(self, mock_utils,
                                                   mock_gu, mock_open):
        mock_utils.execute.return_value = ('', '')
        mock_gu.guess_grub_version.return_value = 1
        self.drv.grub.kernel_params = 'fake_kernel_params'
        # grub has kernel_name and initrd_name set
        self.drv.grub.kernel_name = 'kernel_name'
        self.drv.grub.initrd_name = 'initrd_name'
        self.action.execute()
        self.assertFalse(mock_gu.grub2_cfg.called)
        self.assertFalse(mock_gu.grub2_install.called)
        mock_gu.grub1_cfg.assert_called_once_with(
            kernel_params='fake_kernel_params root=UUID= ',
            initrd='initrd_name', kernel='kernel_name', chroot='/tmp/target',
            grub_timeout=10)
        mock_gu.grub1_install.assert_called_once_with(
            ['/dev/sda', '/dev/sdb', '/dev/sdc'],
            '/dev/sda3', chroot='/tmp/target')
        self.assertFalse(mock_gu.guess_initrd.called)
        self.assertFalse(mock_gu.guess_kernel.called)

    @mock.patch.object(objects, 'Grub', autospec=True)
    @mock.patch.object(bootloader, 'open', create=True,
                       new_callable=mock.mock_open)
    @mock.patch.object(bootloader, 'gu', autospec=True)
    @mock.patch.object(bootloader, 'utils', autospec=True)
    def test_do_bootloader_rootfs_uuid(self, mock_utils, mock_gu, mock_open,
                                       mock_grub):
        def _fake_uuid(*args, **kwargs):
            if len(args) >= 6 and args[5] == '/dev/sda':
                return ('FAKE_ROOTFS_UUID', None)
            else:
                return ('FAKE_UUID', None)
        mock_utils.execute.side_effect = _fake_uuid
        mock_grub.version = 2
        mock_gu.guess_grub_version.return_value = 2
        mock_grub.kernel_name = 'fake_kernel_name'
        mock_grub.initrd_name = 'fake_initrd_name'
        mock_grub.kernel_params = 'fake_kernel_params'
        self.drv.grub = mock_grub
        self.action.execute()
        mock_grub.append_kernel_params.assert_called_once_with(
            'root=UUID=FAKE_ROOTFS_UUID ')
        self.assertEqual(2, mock_grub.version)

    @mock.patch.object(bootloader, 'utils', autospec=True)
    def test_do_bootloader_rootfs_not_found(self, mock_utils):
        mock_utils.execute.return_value = ('fake', 'fake')
        self.drv.partition_scheme.fss = [
            objects.FS(device='fake', mount='/boot', fs_type='ext2'),
            objects.FS(device='fake', mount='swap', fs_type='swap')]
        self.assertRaises(errors.WrongPartitionSchemeError,
                          self.action.execute)

    @mock.patch.object(bootloader, 'open', create=True,
                       new_callable=mock.mock_open)
    @mock.patch.object(bootloader, 'gu', autospec=True)
    @mock.patch.object(bootloader, 'utils', autospec=True)
    def test_do_bootloader_grub_version_changes(
            self, mock_utils, mock_gu, mock_open):
        # actually covers only grub1 related logic
        mock_utils.execute.return_value = ('fake_UUID\n', None)
        mock_gu.guess_grub_version.return_value = 'expected_version'
        self.action.execute()
        mock_gu.guess_grub_version.assert_called_once_with(
            chroot='/tmp/target')
        self.assertEqual('expected_version', self.drv.grub.version)

    @mock.patch.object(bootloader, 'open', create=True,
                       new_callable=mock.mock_open)
    @mock.patch.object(bootloader, 'gu', autospec=True)
    @mock.patch.object(bootloader, 'utils', autospec=True)
    def test_do_bootloader_grub1(self, mock_utils, mock_gu, mock_open):
        # actually covers only grub1 related logic
        mock_utils.execute.return_value = ('fake_UUID\n', None)
        mock_gu.guess_initrd.return_value = 'guessed_initrd'
        mock_gu.guess_kernel.return_value = 'guessed_kernel'
        mock_gu.guess_grub_version.return_value = 1
        self.action.execute()
        mock_gu.guess_grub_version.assert_called_once_with(
            chroot='/tmp/target')
        mock_gu.grub1_cfg.assert_called_once_with(
            kernel_params=' console=ttyS0,9600 console=tty0 rootdelay=90 '
                          'nomodeset root=UUID=fake_UUID ',
            initrd='guessed_initrd',
            chroot='/tmp/target',
            kernel='guessed_kernel',
            grub_timeout=10)
        mock_gu.grub1_install.assert_called_once_with(
            ['/dev/sda', '/dev/sdb', '/dev/sdc'],
            '/dev/sda3', chroot='/tmp/target')
        self.assertFalse(mock_gu.grub2_cfg.called)
        self.assertFalse(mock_gu.grub2_install.called)

    @mock.patch.object(bootloader, 'open', create=True,
                       new_callable=mock.mock_open)
    @mock.patch.object(bootloader, 'gu', autospec=True)
    @mock.patch.object(bootloader, 'utils', autospec=True)
    def test_do_bootloader_grub2(self, mock_utils, mock_gu, mock_open):
        # actually covers only grub2 related logic
        mock_utils.execute.return_value = ('fake_UUID\n', None)
        mock_gu.guess_grub_version.return_value = 2
        self.action.execute()
        mock_gu.guess_grub_version.assert_called_once_with(
            chroot='/tmp/target')
        mock_gu.grub2_cfg.assert_called_once_with(
            kernel_params=' console=ttyS0,9600 console=tty0 rootdelay=90 '
                          'nomodeset root=UUID=fake_UUID ',
            chroot='/tmp/target', grub_timeout=10)
        mock_gu.grub2_install.assert_called_once_with(
            ['/dev/sda', '/dev/sdb', '/dev/sdc'],
            chroot='/tmp/target')
        self.assertFalse(mock_gu.grub1_cfg.called)
        self.assertFalse(mock_gu.grub1_install.called)

    @mock.patch.object(bootloader, 'gu', autospec=True)
    @mock.patch.object(bootloader, 'utils', autospec=True)
    def test_do_bootloader_writes(self, mock_utils, mock_gu):
        # actually covers only write() calls
        mock_utils.execute.return_value = ('fake_UUID\n', None)
        self.drv.configdrive_scheme.common.udevrules = "08:00:27:79:da:80_"\
            "eth0,08:00:27:46:43:60_eth1,08:00:27:b1:d7:15_eth2"
        self.drv.partition_scheme.fss = [
            objects.FS('device', mount='/boot', fs_type='ext2'),
            objects.FS('device', mount='/tmp', fs_type='ext2'),
            objects.FS('device', mount='/', fs_type='ext4'),
            objects.FS('device', mount='swap', fs_type='swap'),
            objects.FS('device', mount='/var/lib/glance')]
        with mock.patch.object(bootloader, 'open', create=True) as mock_open:
            file_handle_mock = mock_open.return_value.__enter__.return_value
            self.action.execute()
            expected_open_calls = [
                mock.call('/tmp/target/etc/udev/rules.d/70-persistent-net.'
                          'rules', 'wt', encoding='utf-8'),
                mock.call('/tmp/target/etc/udev/rules.d/75-persistent-net-'
                          'generator.rules', 'wt', encoding='utf-8'),
                mock.call('/tmp/target/etc/nailgun-agent/nodiscover', 'w'),
                mock.call('/tmp/target/etc/fstab', 'wt', encoding='utf-8')]
            self.assertEqual(expected_open_calls, mock_open.call_args_list)
            expected_write_calls = [
                mock.call('# Generated by bareon during provisioning: '
                          'BEGIN\n'),
                mock.call('SUBSYSTEM=="net", ACTION=="add", DRIVERS=="?*", '
                          'ATTR{address}=="08:00:27:79:da:80", ATTR{type}=="1"'
                          ', KERNEL=="eth*", NAME="eth0"\n'),
                mock.call('SUBSYSTEM=="net", ACTION=="add", DRIVERS=="?*", '
                          'ATTR{address}=="08:00:27:46:43:60", ATTR{type}=="1"'
                          ', KERNEL=="eth*", NAME="eth1"\n'),
                mock.call('SUBSYSTEM=="net", ACTION=="add", DRIVERS=="?*", '
                          'ATTR{address}=="08:00:27:b1:d7:15", ATTR{type}=="1"'
                          ', KERNEL=="eth*", NAME="eth2"\n'),
                mock.call('# Generated by bareon during provisioning: '
                          'END\n'),
                mock.call('# Generated by bareon during provisioning:\n# '
                          'DO NOT DELETE. It is needed to disable '
                          'net-generator\n'),
                mock.call('UUID=fake_UUID /boot ext2 defaults 0 0\n'),
                mock.call('UUID=fake_UUID /tmp ext2 defaults 0 0\n'),
                mock.call(
                    'UUID=fake_UUID / ext4 defaults,errors=panic 0 0\n'),
                mock.call('UUID=fake_UUID swap swap defaults 0 0\n'),
                mock.call('UUID=fake_UUID /var/lib/glance xfs defaults 0 0\n')
            ]
            self.assertEqual(expected_write_calls,
                             file_handle_mock.write.call_args_list)
        self.action._mount_target.assert_called_once_with(
            '/tmp/target', os_id=None, pseudo=True, treat_mtab=True)
        mock_utils.makedirs_if_not_exists.assert_called_once_with(
            '/tmp/target/etc/nailgun-agent')
        self.action._umount_target.assert_called_once_with(
            '/tmp/target', os_id=None, pseudo=True)
