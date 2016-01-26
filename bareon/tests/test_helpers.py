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

import copy
import os
import signal

from oslo_config import cfg
import six
import unittest2

from bareon.drivers import nailgun
from bareon import errors
from bareon import helpers
from bareon import manager
from bareon import objects
from bareon.tests import test_nailgun
from bareon.utils import artifact as au
from bareon.utils import fs as fu
from bareon.utils import hardware as hu
from bareon.utils import lvm as lu
from bareon.utils import md as mu
from bareon.utils import partition as pu
from bareon.utils import utils

if six.PY2:
    import mock
elif six.PY3:
    import unittest.mock as mock

CONF = cfg.CONF


class TestHelpers(unittest2.TestCase):

    @mock.patch('bareon.drivers.nailgun.Nailgun.parse_image_meta',
                return_value={})
    @mock.patch.object(hu, 'list_block_devices')
    def setUp(self, mock_lbd, mock_image_meta):
        super(TestManager, self).setUp()
        mock_lbd.return_value = test_nailgun.LIST_BLOCK_DEVICES_SAMPLE
        self.mgr = manager.Manager(test_nailgun.PROVISION_SAMPLE_DATA)

    @mock.patch('bareon.manager.fu', create=True)
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

    @mock.patch('bareon.manager.fu', create=True)
    @mock.patch('bareon.manager.utils', create=True)
    @mock.patch('bareon.manager.open',
                create=True, new_callable=mock.mock_open)
    @mock.patch('bareon.manager.os', create=True)
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

    @mock.patch('bareon.manager.fu', create=True)
    @mock.patch('bareon.manager.utils', create=True)
    @mock.patch('bareon.manager.open',
                create=True, new_callable=mock.mock_open)
    @mock.patch('bareon.manager.os', create=True)
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
