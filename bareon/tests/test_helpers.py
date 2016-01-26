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

from oslo_config import cfg
import six
import unittest2

from bareon import helpers
from bareon import manager
from bareon import objects
from bareon.tests import test_nailgun
from bareon.utils import hardware as hu

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
        super(TestHelpers, self).setUp()
        mock_lbd.return_value = test_nailgun.LIST_BLOCK_DEVICES_SAMPLE
        self.mgr = manager.Manager(test_nailgun.PROVISION_SAMPLE_DATA)
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

    @mock.patch('bareon.helpers.utils', autospec=True)
    @mock.patch('bareon.helpers.fu', autospec=True)
    def test_mount_target(self, mock_fu, mock_utils):
        helpers.mount_target(self.mgr.driver, 'fake_chroot')
        self.assertEqual([mock.call('fake_chroot/'),
                          mock.call('fake_chroot/boot'),
                          mock.call('fake_chroot/var'),
                          mock.call('fake_chroot/var/lib')],
                         mock_utils.makedirs_if_not_exists.call_args_list)
        self.assertEqual([mock.call('ext4', 'fake', 'fake_chroot/'),
                          mock.call('ext2', 'fake', 'fake_chroot/boot'),
                          mock.call('ext4', 'fake', 'fake_chroot/var'),
                          mock.call('xfs', 'fake', 'fake_chroot/var/lib')],
                         mock_fu.mount_fs.call_args_list)

    @mock.patch('bareon.helpers.fu', autospec=True)
    def test_umount_target(self, mock_fu):
        helpers.umount_target(self.mgr.driver, 'fake_chroot')
        self.assertEqual([mock.call('fake_chroot/var/lib'),
                          mock.call('fake_chroot/boot'),
                          mock.call('fake_chroot/var'),
                          mock.call('fake_chroot/')],
                         mock_fu.umount_fs.call_args_list)
