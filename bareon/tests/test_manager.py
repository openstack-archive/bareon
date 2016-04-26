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
import signal

from oslo_config import cfg
import six
import unittest2

import bareon
from bareon.drivers.data import nailgun as nailgun_data
from bareon.drivers.deploy import nailgun as nailgun_deploy
from bareon import errors
from bareon import objects
from bareon.tests import test_nailgun
from bareon.utils import hardware as hu
from bareon.utils import utils

if six.PY2:
    import mock
elif six.PY3:
    import unittest.mock as mock

CONF = cfg.CONF


@unittest2.skip("Fix after cray rebase")
class TestManager(unittest2.TestCase):

    @mock.patch('bareon.drivers.data.nailgun.Nailgun.parse_image_meta',
                return_value={})
    @mock.patch.object(hu, 'list_block_devices')
    def setUp(self, mock_lbd, mock_image_meta):
        super(TestManager, self).setUp()
        mock_lbd.return_value = test_nailgun.LIST_BLOCK_DEVICES_SAMPLE
        self.mgr = nailgun_deploy.Manager(test_nailgun.PROVISION_SAMPLE_DATA)

    @mock.patch('bareon.drivers.deploy.nailgun.open',
                create=True, new_callable=mock.mock_open)
    @mock.patch('bareon.drivers.deploy.nailgun.gu', create=True)
    @mock.patch('bareon.drivers.deploy.nailgun.utils', create=True)
    @mock.patch.object(bareon.drivers.deploy.nailgun, 'mount_target')
    @mock.patch.object(bareon.drivers.deploy.nailgun, 'umount_target')
    def test_do_bootloader_grub1_kernel_initrd_guessed(self, mock_umount,
                                                       mock_mount, mock_utils,
                                                       mock_gu, mock_open):
        mock_utils.execute.return_value = ('', '')
        mock_gu.guess_grub_version.return_value = 1
        # grub has kernel_name and initrd_name both set to None
        self.mgr.driver.grub.kernel_name = None
        self.mgr.driver.grub.initrd_name = None
        self.mgr.driver.grub.kernel_params = 'fake_kernel_params'
        self.mgr.driver.grub.kernel_regexp = 'fake_kernel_regexp'
        self.mgr.driver.grub.initrd_regexp = 'fake_initrd_regexp'
        mock_gu.guess_kernel.return_value = 'guessed_kernel'
        mock_gu.guess_initrd.return_value = 'guessed_initrd'
        self.mgr.do_bootloader()
        self.assertFalse(mock_gu.grub2_cfg.called)
        self.assertFalse(mock_gu.grub2_install.called)
        mock_gu.grub1_cfg.assert_called_once_with(
            kernel_params='fake_kernel_params root=UUID= ',
            initrd='guessed_initrd', kernel='guessed_kernel',
            chroot='/tmp/target', grub_timeout=5)
        mock_gu.grub1_install.assert_called_once_with(
            ['/dev/sda', '/dev/sdb', '/dev/sdc'],
            '/dev/sda3', chroot='/tmp/target')
        mock_gu.guess_initrd.assert_called_once_with(
            regexp='fake_initrd_regexp', chroot='/tmp/target')
        mock_gu.guess_kernel.assert_called_once_with(
            regexp='fake_kernel_regexp', chroot='/tmp/target')

    @mock.patch('bareon.drivers.deploy.nailgun.open',
                create=True, new_callable=mock.mock_open)
    @mock.patch('bareon.drivers.deploy.nailgun.gu', create=True)
    @mock.patch('bareon.drivers.deploy.nailgun.utils', create=True)
    @mock.patch.object(bareon.drivers.deploy.nailgun, 'mount_target')
    @mock.patch.object(bareon.drivers.deploy.nailgun, 'umount_target')
    def test_do_bootloader_grub1_kernel_initrd_set(self, mock_umount,
                                                   mock_mount, mock_utils,
                                                   mock_gu, mock_open):
        mock_utils.execute.return_value = ('', '')
        mock_gu.guess_grub_version.return_value = 1
        self.mgr.driver.grub.kernel_params = 'fake_kernel_params'
        # grub has kernel_name and initrd_name set
        self.mgr.driver.grub.kernel_name = 'kernel_name'
        self.mgr.driver.grub.initrd_name = 'initrd_name'
        self.mgr.do_bootloader()
        self.assertFalse(mock_gu.grub2_cfg.called)
        self.assertFalse(mock_gu.grub2_install.called)
        mock_gu.grub1_cfg.assert_called_once_with(
            kernel_params='fake_kernel_params root=UUID= ',
            initrd='initrd_name', kernel='kernel_name', chroot='/tmp/target',
            grub_timeout=5)
        mock_gu.grub1_install.assert_called_once_with(
            ['/dev/sda', '/dev/sdb', '/dev/sdc'],
            '/dev/sda3', chroot='/tmp/target')
        self.assertFalse(mock_gu.guess_initrd.called)
        self.assertFalse(mock_gu.guess_kernel.called)

    @mock.patch('bareon.objects.bootloader.Grub', autospec=True)
    @mock.patch('bareon.drivers.deploy.nailgun.open',
                create=True, new_callable=mock.mock_open)
    @mock.patch('bareon.drivers.deploy.nailgun.gu', create=True)
    @mock.patch('bareon.drivers.deploy.nailgun.utils', create=True)
    @mock.patch.object(bareon.drivers.deploy.nailgun, 'mount_target')
    @mock.patch.object(bareon.drivers.deploy.nailgun, 'umount_target')
    def test_do_bootloader_rootfs_uuid(self, mock_umount, mock_mount,
                                       mock_utils, mock_gu, mock_open,
                                       mock_grub):
        def _fake_uuid(*args, **kwargs):
            if len(args) >= 6 and args[5] == '/dev/mapper/os-root':
                return ('FAKE_ROOTFS_UUID', None)
            else:
                return ('FAKE_UUID', None)
        mock_utils.execute.side_effect = _fake_uuid
        mock_grub.version = 2
        mock_gu.guess_grub_version.return_value = 2
        mock_grub.kernel_name = 'fake_kernel_name'
        mock_grub.initrd_name = 'fake_initrd_name'
        mock_grub.kernel_params = 'fake_kernel_params'
        self.mgr.driver._grub = mock_grub
        self.mgr.do_bootloader()
        mock_grub.append_kernel_params.assert_called_once_with(
            'root=UUID=FAKE_ROOTFS_UUID ')
        self.assertEqual(2, mock_grub.version)

    @mock.patch('bareon.drivers.deploy.nailgun.utils', create=True)
    @mock.patch.object(bareon.drivers.deploy.nailgun, 'mount_target')
    def test_do_bootloader_rootfs_not_found(self, mock_umount, mock_utils):
        mock_utils.execute.return_value = ('fake', 'fake')
        self.mgr.driver._partition_scheme = objects.PartitionScheme()
        self.mgr.driver.partition_scheme.add_fs(
            device='fake', mount='/boot', fs_type='ext2')
        self.mgr.driver.partition_scheme.add_fs(
            device='fake', mount='swap', fs_type='swap')
        self.assertRaises(errors.WrongPartitionSchemeError,
                          self.mgr.do_bootloader)

    @mock.patch('bareon.drivers.deploy.nailgun.open',
                create=True, new_callable=mock.mock_open)
    @mock.patch('bareon.drivers.deploy.nailgun.gu', create=True)
    @mock.patch('bareon.drivers.deploy.nailgun.utils', create=True)
    @mock.patch.object(bareon.drivers.deploy.nailgun, 'mount_target')
    @mock.patch.object(bareon.drivers.deploy.nailgun, 'umount_target')
    def test_do_bootloader_grub_version_changes(
            self, mock_umount, mock_mount, mock_utils, mock_gu, mock_open):
        # actually covers only grub1 related logic
        mock_utils.execute.return_value = ('fake_UUID\n', None)
        mock_gu.guess_grub_version.return_value = 'expected_version'
        self.mgr.do_bootloader()
        mock_gu.guess_grub_version.assert_called_once_with(
            chroot='/tmp/target')
        self.assertEqual('expected_version', self.mgr.driver.grub.version)

    @mock.patch('bareon.drivers.deploy.nailgun.open',
                create=True, new_callable=mock.mock_open)
    @mock.patch('bareon.drivers.deploy.nailgun.gu', create=True)
    @mock.patch('bareon.drivers.deploy.nailgun.utils', create=True)
    @mock.patch.object(bareon.drivers.deploy.nailgun, 'mount_target')
    @mock.patch.object(bareon.drivers.deploy.nailgun, 'umount_target')
    def test_do_bootloader_grub1(self, mock_umount, mock_mount, mock_utils,
                                 mock_gu, mock_open):
        # actually covers only grub1 related logic
        mock_utils.execute.return_value = ('fake_UUID\n', None)
        mock_gu.guess_initrd.return_value = 'guessed_initrd'
        mock_gu.guess_kernel.return_value = 'guessed_kernel'
        mock_gu.guess_grub_version.return_value = 1
        self.mgr.do_bootloader()
        mock_gu.guess_grub_version.assert_called_once_with(
            chroot='/tmp/target')
        mock_gu.grub1_cfg.assert_called_once_with(
            kernel_params=' console=ttyS0,9600 console=tty0 rootdelay=90 '
                          'nomodeset root=UUID=fake_UUID ',
            initrd='guessed_initrd',
            chroot='/tmp/target',
            kernel='guessed_kernel',
            grub_timeout=5)
        mock_gu.grub1_install.assert_called_once_with(
            ['/dev/sda', '/dev/sdb', '/dev/sdc'],
            '/dev/sda3', chroot='/tmp/target')
        self.assertFalse(mock_gu.grub2_cfg.called)
        self.assertFalse(mock_gu.grub2_install.called)

    @mock.patch('bareon.drivers.deploy.nailgun.open',
                create=True, new_callable=mock.mock_open)
    @mock.patch('bareon.drivers.deploy.nailgun.gu', create=True)
    @mock.patch('bareon.drivers.deploy.nailgun.utils', create=True)
    @mock.patch.object(bareon.drivers.deploy.nailgun, 'mount_target')
    @mock.patch.object(bareon.drivers.deploy.nailgun, 'umount_target')
    def test_do_bootloader_grub2(self, mock_umount, mock_mount, mock_utils,
                                 mock_gu, mock_open):
        # actually covers only grub2 related logic
        mock_utils.execute.return_value = ('fake_UUID\n', None)
        mock_gu.guess_grub_version.return_value = 2
        self.mgr.do_bootloader()
        mock_gu.guess_grub_version.assert_called_once_with(
            chroot='/tmp/target')
        mock_gu.grub2_cfg.assert_called_once_with(
            kernel_params=' console=ttyS0,9600 console=tty0 rootdelay=90 '
                          'nomodeset root=UUID=fake_UUID ',
            chroot='/tmp/target', grub_timeout=5)
        mock_gu.grub2_install.assert_called_once_with(
            ['/dev/sda', '/dev/sdb', '/dev/sdc'],
            chroot='/tmp/target')
        self.assertFalse(mock_gu.grub1_cfg.called)
        self.assertFalse(mock_gu.grub1_install.called)

    @mock.patch('bareon.drivers.deploy.nailgun.gu', create=True)
    @mock.patch('bareon.drivers.deploy.nailgun.utils', create=True)
    @mock.patch.object(bareon.drivers.deploy.nailgun, 'mount_target')
    @mock.patch.object(bareon.drivers.deploy.nailgun, 'umount_target')
    def test_do_bootloader_writes(self, mock_umount, mock_mount, mock_utils,
                                  mock_gu):
        # actually covers only write() calls
        mock_utils.execute.return_value = ('fake_UUID\n', None)
        with mock.patch('bareon.drivers.deploy.nailgun.open',
                        create=True) as mock_open:
            file_handle_mock = mock_open.return_value.__enter__.return_value
            self.mgr.do_bootloader()
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
        mock_umount.assert_called_once_with('/tmp/target')
        mock_mount.assert_called_once_with('/tmp/target')
        mock_utils.makedirs_if_not_exists.assert_called_once_with(
            '/tmp/target/etc/nailgun-agent')

    @mock.patch('bareon.drivers.deploy.nailgun.fu', create=True)
    @mock.patch('bareon.drivers.deploy.nailgun.utils', create=True)
    @mock.patch('bareon.drivers.deploy.nailgun.open',
                create=True, new_callable=mock.mock_open)
    @mock.patch('bareon.drivers.deploy.nailgun.os', create=True)
    def test_mount_target_mtab_is_link(self, mock_os, mock_open, mock_utils,
                                       mock_fu):
        mock_os.path.islink.return_value = True
        mock_utils.execute.return_value = (None, None)
        self.mgr.driver._partition_scheme = objects.PartitionScheme()
        self.mgr.mount_target('fake_chroot')
        mock_open.assert_called_once_with('fake_chroot/etc/mtab', 'wt',
                                          encoding='utf-8')
        mock_os.path.islink.assert_called_once_with('fake_chroot/etc/mtab')
        mock_os.remove.assert_called_once_with('fake_chroot/etc/mtab')

    @mock.patch('bareon.drivers.deploy.nailgun.fu', create=True)
    @mock.patch('bareon.drivers.deploy.nailgun.utils', create=True)
    @mock.patch('bareon.drivers.deploy.nailgun.open',
                create=True, new_callable=mock.mock_open)
    @mock.patch('bareon.drivers.deploy.nailgun.os', create=True)
    def test_mount_target(self, mock_os, mock_open, mock_utils, mock_fu):
        mock_os.path.islink.return_value = False
        self.mgr.driver._partition_scheme = objects.PartitionScheme()
        self.mgr.driver.partition_scheme.add_fs(
            device='fake', mount='/var/lib', fs_type='xfs')
        self.mgr.driver.partition_scheme.add_fs(
            device='fake', mount='/', fs_type='ext4')
        self.mgr.driver.partition_scheme.add_fs(
            device='fake', mount='/boot', fs_type='ext2')
        self.mgr.driver.partition_scheme.add_fs(
            device='fake', mount='swap', fs_type='swap')
        self.mgr.driver.partition_scheme.add_fs(
            device='fake', mount='/var', fs_type='ext4')
        fake_mtab = """
proc /proc proc rw,noexec,nosuid,nodev 0 0
sysfs /sys sysfs rw,noexec,nosuid,nodev 0 0
none /sys/fs/fuse/connections fusectl rw 0 0
none /sys/kernel/debug debugfs rw 0 0
none /sys/kernel/security securityfs rw 0 0
udev /dev devtmpfs rw,mode=0755 0 0
devpts /dev/pts devpts rw,noexec,nosuid,gid=5,mode=0620 0 0
tmpfs /run tmpfs rw,noexec,nosuid,size=10%,mode=0755 0 0
none /run/lock tmpfs rw,noexec,nosuid,nodev,size=5242880 0 0
none /run/shm tmpfs rw,nosuid,nodev 0 0"""
        mock_utils.execute.return_value = (fake_mtab, None)
        self.mgr.mount_target('fake_chroot')
        self.assertEqual([mock.call('fake_chroot/'),
                          mock.call('fake_chroot/boot'),
                          mock.call('fake_chroot/var'),
                          mock.call('fake_chroot/var/lib'),
                          mock.call('fake_chroot/sys'),
                          mock.call('fake_chroot/dev'),
                          mock.call('fake_chroot/proc')],
                         mock_utils.makedirs_if_not_exists.call_args_list)
        self.assertEqual([mock.call('ext4', 'fake', 'fake_chroot/'),
                          mock.call('ext2', 'fake', 'fake_chroot/boot'),
                          mock.call('ext4', 'fake', 'fake_chroot/var'),
                          mock.call('xfs', 'fake', 'fake_chroot/var/lib')],
                         mock_fu.mount_fs.call_args_list)
        self.assertEqual([mock.call('fake_chroot', '/sys'),
                          mock.call('fake_chroot', '/dev'),
                          mock.call('fake_chroot', '/proc')],
                         mock_fu.mount_bind.call_args_list)
        file_handle = mock_open.return_value.__enter__.return_value
        file_handle.write.assert_called_once_with(fake_mtab)
        mock_open.assert_called_once_with('fake_chroot/etc/mtab', 'wt',
                                          encoding='utf-8')
        mock_os.path.islink.assert_called_once_with('fake_chroot/etc/mtab')
        self.assertFalse(mock_os.remove.called)

    @mock.patch('bareon.drivers.deploy.nailgun.fu', create=True)
    def test_umount_target(self, mock_fu):
        self.mgr.driver._partition_scheme = objects.PartitionScheme()
        self.mgr.driver.partition_scheme.add_fs(
            device='fake', mount='/var/lib', fs_type='xfs')
        self.mgr.driver.partition_scheme.add_fs(
            device='fake', mount='/', fs_type='ext4')
        self.mgr.driver.partition_scheme.add_fs(
            device='fake', mount='/boot', fs_type='ext2')
        self.mgr.driver.partition_scheme.add_fs(
            device='fake', mount='swap', fs_type='swap')
        self.mgr.driver.partition_scheme.add_fs(
            device='fake', mount='/var', fs_type='ext4')
        self.mgr.umount_target('fake_chroot')
        self.assertEqual([mock.call('fake_chroot/proc'),
                          mock.call('fake_chroot/dev'),
                          mock.call('fake_chroot/sys/fs/fuse/connections'),
                          mock.call('fake_chroot/sys'),
                          mock.call('fake_chroot/var/lib'),
                          mock.call('fake_chroot/boot'),
                          mock.call('fake_chroot/var'),
                          mock.call('fake_chroot/')],
                         mock_fu.umount_fs.call_args_list)


@unittest2.skip("Fix after cray rebase")
class TestImageBuild(unittest2.TestCase):

    @mock.patch('yaml.load')
    @mock.patch.object(utils, 'init_http_request')
    @mock.patch.object(utils, 'get_driver')
    def setUp(self, mock_driver, mock_http, mock_yaml):
        super(self.__class__, self).setUp()
        mock_driver.return_value = nailgun_data.NailgunBuildImage
        image_conf = {
            "image_data": {
                "/": {
                    "container": "gzip",
                    "format": "ext4",
                    "uri": "http:///centos_65_x86_64.img.gz",
                },
            },
            "output": "/var/www/nailgun/targetimages",
            "repos": [
                {
                    "name": "repo",
                    "uri": "http://some",
                    'type': 'deb',
                    'suite': '/',
                    'section': '',
                    'priority': 1001
                }
            ],
            "codename": "trusty"
        }
        self.mgr = bareon.drivers.deploy.nailgun(image_conf)

    @mock.patch.object(bareon.drivers.deploy.nailgun, '_set_apt_repos')
    @mock.patch('bareon.drivers.deploy.nailgun.bu', create=True)
    @mock.patch('bareon.drivers.deploy.nailgun.fu', create=True)
    @mock.patch('bareon.drivers.deploy.nailgun.utils', create=True)
    @mock.patch('bareon.drivers.deploy.nailgun.os', create=True)
    @mock.patch('bareon.drivers.deploy.nailgun.shutil.move')
    @mock.patch('bareon.drivers.deploy.nailgun.open',
                create=True, new_callable=mock.mock_open)
    @mock.patch('bareon.drivers.deploy.nailgun.yaml.safe_dump')
    @mock.patch.object(bareon.drivers.deploy.nailgun, 'mount_target')
    @mock.patch.object(bareon.drivers.deploy.nailgun, 'umount_target')
    def test_do_build_image(self, mock_umount_target, mock_mount_target,
                            mock_yaml_dump, mock_open, mock_shutil_move,
                            mock_os, mock_utils,
                            mock_fu, mock_bu, mock_set_apt_repos):

        loops = [objects.Loop(), objects.Loop()]

        self.mgr.driver._image_scheme = objects.ImageScheme([
            objects.Image('file:///fake/img.img.gz', loops[0], 'ext4', 'gzip'),
            objects.Image('file:///fake/img-boot.img.gz',
                          loops[1], 'ext2', 'gzip')])
        self.mgr.driver._partition_scheme = objects.PartitionScheme()
        self.mgr.driver.partition_scheme.add_fs(
            device=loops[0], mount='/', fs_type='ext4')
        self.mgr.driver.partition_scheme.add_fs(
            device=loops[1], mount='/boot', fs_type='ext2')
        self.mgr.driver.metadata_uri = 'file:///fake/img.yaml'
        self.mgr.driver._operating_system = objects.Ubuntu(
            repos=[
                objects.DEBRepo('ubuntu', 'http://fakeubuntu',
                                'trusty', 'fakesection', priority=900),
                objects.DEBRepo('ubuntu_zero', 'http://fakeubuntu_zero',
                                'trusty', 'fakesection', priority=None),
                objects.DEBRepo('mos', 'http://fakemos',
                                'mosX.Y', 'fakesection', priority=1000)],
            packages=['fakepackage1', 'fakepackage2'])
        self.mgr.driver.operating_system.proxies = objects.RepoProxies(
            proxies={'fake': 'fake'},
            direct_repo_addr_list='fake_addr')
        self.mgr.driver.operating_system.minor = 4
        self.mgr.driver.operating_system.major = 14
        mock_os.path.exists.return_value = False
        mock_os.path.join.return_value = '/tmp/imgdir/proc'
        mock_os.path.basename.side_effect = ['img.img.gz', 'img-boot.img.gz']
        mock_bu.create_sparse_tmp_file.side_effect = \
            ['/tmp/img', '/tmp/img-boot']
        mock_bu.attach_file_to_free_loop_device.side_effect = [
            '/dev/loop0', '/dev/loop1']
        mock_bu.mkdtemp_smart.return_value = '/tmp/imgdir'
        getsize_side = [20, 2, 10, 1]
        mock_os.path.getsize.side_effect = getsize_side
        md5_side = ['fakemd5_raw', 'fakemd5_gzip',
                    'fakemd5_raw_boot', 'fakemd5_gzip_boot']
        mock_utils.calculate_md5.side_effect = md5_side
        mock_bu.containerize.side_effect = ['/tmp/img.gz', '/tmp/img-boot.gz']
        mock_bu.stop_chrooted_processes.side_effect = [
            False, True, False, True]
        metadata = {'os': {'name': 'Ubuntu', 'major': 14, 'minor': 4},
                    'packages': self.mgr.driver.operating_system.packages}

        self.mgr.do_build_image()

        self.assertEqual(
            [mock.call('/fake/img.img.gz'),
             mock.call('/fake/img-boot.img.gz')],
            mock_os.path.exists.call_args_list)
        self.assertEqual([mock.call(dir=CONF.image_build_dir,
                                    suffix=CONF.image_build_suffix,
                                    size=CONF.sparse_file_size)] * 2,
                         mock_bu.create_sparse_tmp_file.call_args_list)
        self.assertEqual(
            [mock.call(
                '/tmp/img',
                loop_device_major_number=CONF.loop_device_major_number,
                max_loop_devices_count=CONF.max_loop_devices_count,
                max_attempts=CONF.max_allowed_attempts_attach_image),
             mock.call(
                '/tmp/img-boot',
                loop_device_major_number=CONF.loop_device_major_number,
                max_loop_devices_count=CONF.max_loop_devices_count,
                max_attempts=CONF.max_allowed_attempts_attach_image)
             ],
            mock_bu.attach_file_to_free_loop_device.call_args_list)
        self.assertEqual([mock.call(fs_type='ext4', fs_options='',
                                    fs_label='', dev='/dev/loop0'),
                          mock.call(fs_type='ext2', fs_options='',
                                    fs_label='', dev='/dev/loop1')],
                         mock_fu.make_fs.call_args_list)
        mock_bu.mkdtemp_smart.assert_called_once_with(
            CONF.image_build_dir, CONF.image_build_suffix)
        mock_mount_target.assert_called_once_with(
            '/tmp/imgdir', treat_mtab=False, pseudo=False)
        self.assertEqual([mock.call('/tmp/imgdir')] * 2,
                         mock_bu.suppress_services_start.call_args_list)
        mock_bu.run_debootstrap.assert_called_once_with(
            uri='http://fakeubuntu', suite='trusty', chroot='/tmp/imgdir',
            attempts=CONF.fetch_packages_attempts,
            proxies={'fake': 'fake'}, direct_repo_addr='fake_addr')
        mock_bu.set_apt_get_env.assert_called_once_with()
        mock_bu.pre_apt_get.assert_called_once_with(
            '/tmp/imgdir', allow_unsigned_file=CONF.allow_unsigned_file,
            force_ipv4_file=CONF.force_ipv4_file, proxies={'fake': 'fake'},
            direct_repo_addr='fake_addr')
        mock_set_apt_repos.assert_called_with(
            '/tmp/imgdir',
            self.mgr.driver.operating_system.repos
        )

        mock_utils.makedirs_if_not_exists.assert_called_once_with(
            '/tmp/imgdir/proc')
        self.assertEqual([
            mock.call('tune2fs', '-O', '^has_journal', '/dev/loop0'),
            mock.call('tune2fs', '-O', 'has_journal', '/dev/loop0')],
            mock_utils.execute.call_args_list)
        mock_fu.mount_bind.assert_called_once_with('/tmp/imgdir', '/proc')
        mock_bu.populate_basic_dev.assert_called_once_with('/tmp/imgdir')
        mock_bu.run_apt_get.assert_called_once_with(
            '/tmp/imgdir', packages=['fakepackage1', 'fakepackage2'],
            attempts=CONF.fetch_packages_attempts)
        mock_bu.do_post_inst.assert_called_once_with(
            '/tmp/imgdir', allow_unsigned_file=CONF.allow_unsigned_file,
            force_ipv4_file=CONF.force_ipv4_file)

        signal_calls = mock_bu.stop_chrooted_processes.call_args_list
        self.assertEqual(2 * [mock.call('/tmp/imgdir', signal=signal.SIGTERM),
                              mock.call('/tmp/imgdir', signal=signal.SIGKILL)],
                         signal_calls)
        self.assertEqual(
            [mock.call('/tmp/imgdir/proc')] * 2,
            mock_fu.umount_fs.call_args_list)
        self.assertEqual(
            [mock.call(
                '/tmp/imgdir', pseudo=False)] * 2,
            mock_umount_target.call_args_list)
        self.assertEqual(
            [mock.call('/dev/loop0'), mock.call('/dev/loop1')] * 2,
            mock_bu.deattach_loop.call_args_list)
        self.assertEqual([mock.call('/tmp/img'), mock.call('/tmp/img-boot')],
                         mock_bu.shrink_sparse_file.call_args_list)
        self.assertEqual([mock.call('/tmp/img'),
                          mock.call('/fake/img.img.gz'),
                          mock.call('/tmp/img-boot'),
                          mock.call('/fake/img-boot.img.gz')],
                         mock_os.path.getsize.call_args_list)
        self.assertEqual([mock.call('/tmp/img', 20),
                          mock.call('/fake/img.img.gz', 2),
                          mock.call('/tmp/img-boot', 10),
                          mock.call('/fake/img-boot.img.gz', 1)],
                         mock_utils.calculate_md5.call_args_list)
        self.assertEqual([mock.call('/tmp/img', 'gzip',
                                    chunk_size=CONF.data_chunk_size),
                          mock.call('/tmp/img-boot', 'gzip',
                                    chunk_size=CONF.data_chunk_size)],
                         mock_bu.containerize.call_args_list)
        mock_open.assert_called_once_with('/fake/img.yaml', 'wt',
                                          encoding='utf-8')
        self.assertEqual(
            [mock.call('/tmp/img.gz', '/fake/img.img.gz'),
             mock.call('/tmp/img-boot.gz', '/fake/img-boot.img.gz')],
            mock_shutil_move.call_args_list)

        for repo in self.mgr.driver.operating_system.repos:
            metadata.setdefault('repos', []).append({
                'type': 'deb',
                'name': repo.name,
                'uri': repo.uri,
                'suite': repo.suite,
                'section': repo.section,
                'priority': repo.priority,
                'meta': repo.meta})
        metadata['images'] = [
            {
                'raw_md5': md5_side[0],
                'raw_size': getsize_side[0],
                'raw_name': None,
                'container_name':
                os.path.basename(
                    self.mgr.driver.image_scheme.images[0].uri.split(
                        'file://', 1)[1]),
                'container_md5': md5_side[1],
                'container_size': getsize_side[1],
                'container': self.mgr.driver.image_scheme.images[0].container,
                'format': self.mgr.driver.image_scheme.images[0].format
            },
            {
                'raw_md5': md5_side[2],
                'raw_size': getsize_side[2],
                'raw_name': None,
                'container_name':
                os.path.basename(
                    self.mgr.driver.image_scheme.images[1].uri.split(
                        'file://', 1)[1]),
                'container_md5': md5_side[3],
                'container_size': getsize_side[3],
                'container': self.mgr.driver.image_scheme.images[1].container,
                'format': self.mgr.driver.image_scheme.images[1].format
            }
        ]
        mock_yaml_dump.assert_called_once_with(metadata, stream=mock_open())
