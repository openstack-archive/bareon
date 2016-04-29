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

from bareon.drivers.data import nailgun
from bareon.drivers.deploy import mixins
from bareon import objects

if six.PY2:
    import mock
elif six.PY3:
    import unittest.mock as mock


class TestMountableMixin(unittest2.TestCase):
    def setUp(self):
        self.mxn = mixins.MountableMixin()
        self.mxn.driver = mock.MagicMock(spec=nailgun.Nailgun)
        self.mxn.driver.partition_scheme = objects.PartitionScheme()

    @mock.patch('bareon.drivers.deploy.mixins.fu', autospec=True)
    @mock.patch('bareon.drivers.deploy.mixins.utils', autospec=True)
    @mock.patch('bareon.drivers.deploy.mixins.open',
                create=True, new_callable=mock.mock_open)
    @mock.patch('bareon.drivers.deploy.mixins.os', autospec=True)
    def test_mount_target_mtab_is_link(self, mock_os, mock_open, mock_utils,
                                       mock_fu):
        mock_os.path.islink.return_value = True
        mock_os.sep = '/'
        mock_os.path.join.side_effect = lambda x, y: "%s/%s" % (x, y)
        mock_utils.execute.return_value = (None, None)
        self.mxn._mount_target('fake_chroot')
        mock_open.assert_called_once_with('fake_chroot/etc/mtab', 'wt',
                                          encoding='utf-8')
        mock_os.path.islink.assert_called_once_with('fake_chroot/etc/mtab')
        mock_os.remove.assert_called_once_with('fake_chroot/etc/mtab')

    @mock.patch('bareon.drivers.deploy.mixins.fu', autospec=True)
    @mock.patch('bareon.drivers.deploy.mixins.utils', autospec=True)
    @mock.patch('bareon.drivers.deploy.mixins.open',
                create=True, new_callable=mock.mock_open)
    @mock.patch('bareon.drivers.deploy.mixins.os', autospec=True)
    def test_mount_target(self, mock_os, mock_open, mock_utils, mock_fu):
        mock_os.path.islink.return_value = False
        mock_os.sep = '/'
        mock_os.path.join.side_effect = lambda x, y: "%s/%s" % (x, y)
        self.mxn.driver.partition_scheme.add_fs(
            device='fake', mount='/var/lib', fs_type='xfs')
        self.mxn.driver.partition_scheme.add_fs(
            device='fake', mount='/', fs_type='ext4')
        self.mxn.driver.partition_scheme.add_fs(
            device='fake', mount='/boot', fs_type='ext2')
        self.mxn.driver.partition_scheme.add_fs(
            device='fake', mount='swap', fs_type='swap')
        self.mxn.driver.partition_scheme.add_fs(
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
        self.mxn._mount_target('fake_chroot')
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

    @mock.patch('bareon.drivers.deploy.mixins.fu', autospec=True)
    def test_umount_target(self, mock_fu):
        self.mxn.driver.partition_scheme.add_fs(
            device='fake', mount='/var/lib', fs_type='xfs')
        self.mxn.driver.partition_scheme.add_fs(
            device='fake', mount='/', fs_type='ext4')
        self.mxn.driver.partition_scheme.add_fs(
            device='fake', mount='/boot', fs_type='ext2')
        self.mxn.driver.partition_scheme.add_fs(
            device='fake', mount='swap', fs_type='swap')
        self.mxn.driver.partition_scheme.add_fs(
            device='fake', mount='/var', fs_type='ext4')
        self.mxn._umount_target('fake_chroot')
        self.assertEqual([mock.call('fake_chroot/proc', try_lazy_umount=True),
                          mock.call('fake_chroot/dev', try_lazy_umount=True),
                          mock.call('fake_chroot/sys/fs/fuse/connections',
                                    try_lazy_umount=True),
                          mock.call('fake_chroot/sys', try_lazy_umount=True),
                          mock.call('fake_chroot/var/lib'),
                          mock.call('fake_chroot/boot'),
                          mock.call('fake_chroot/var'),
                          mock.call('fake_chroot/')],
                         mock_fu.umount_fs.call_args_list)
