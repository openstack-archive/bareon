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

import copy
import six
import unittest2

from oslo_config import cfg

from bareon.actions import partitioning
from bareon.drivers.data import nailgun
from bareon import objects
from bareon.tests import test_nailgun

if six.PY2:
    import mock
elif six.PY3:
    import unittest.mock as mock

CONF = cfg.CONF


class TestPartitioningAction(unittest2.TestCase):

    @mock.patch('bareon.drivers.data.nailgun.Nailgun.parse_image_meta',
                return_value={})
    @mock.patch('bareon.drivers.data.nailgun.hu.list_block_devices')
    def setUp(self, mock_lbd, mock_image_meta):
        super(TestPartitioningAction, self).setUp()
        mock_lbd.return_value = test_nailgun.LIST_BLOCK_DEVICES_SAMPLE
        self.drv = nailgun.Nailgun(test_nailgun.PROVISION_SAMPLE_DATA)
        self.action = partitioning.PartitioningAction(self.drv)

    @mock.patch('bareon.drivers.data.nailgun.Nailgun.parse_image_meta',
                return_value={})
    @mock.patch('bareon.drivers.data.nailgun.hu.list_block_devices')
    @mock.patch.object(partitioning, 'fu', autospec=True)
    def test_do_partitioning_with_keep_data_flag(self, mock_fu, mock_lbd,
                                                 mock_image_meta):
        mock_lbd.return_value = test_nailgun.LIST_BLOCK_DEVICES_SAMPLE
        data = copy.deepcopy(test_nailgun.PROVISION_SAMPLE_DATA)

        for disk in data['ks_meta']['pm_data']['ks_spaces']:
            for volume in disk['volumes']:
                if volume['type'] == 'pv' and volume['vg'] == 'image':
                    volume['keep_data'] = True

        self.drv = nailgun.Nailgun(data)
        self.action = partitioning.PartitioningAction(self.drv)
        self.action.execute()
        mock_fu_mf_expected_calls = [
            mock.call('ext2', '', '', '/dev/sda3'),
            mock.call('ext2', '', '', '/dev/sda4'),
            mock.call('swap', '', '', '/dev/mapper/os-swap')]
        self.assertEqual(mock_fu_mf_expected_calls,
                         mock_fu.make_fs.call_args_list)

    @mock.patch.object(partitioning, 'os', autospec=True)
    @mock.patch.object(partitioning, 'utils', autospec=True)
    @mock.patch.object(partitioning, 'mu', autospec=True)
    @mock.patch.object(partitioning, 'lu', autospec=True)
    @mock.patch.object(partitioning, 'fu', autospec=True)
    @mock.patch.object(partitioning, 'pu', autospec=True)
    def test_do_partitioning_md(self, mock_pu, mock_fu, mock_lu, mock_mu,
                                mock_utils, mock_os):
        mock_os.path.exists.return_value = True
        self.drv.partition_scheme.mds = [
            objects.MD('fake_md1', 'mirror', devices=['/dev/sda1',
                                                      '/dev/sdb1']),
            objects.MD('fake_md2', 'mirror', devices=['/dev/sdb3',
                                                      '/dev/sdc1']),
        ]
        self.action.execute()
        self.assertEqual([mock.call('fake_md1', 'mirror',
                                    ['/dev/sda1', '/dev/sdb1'], 'default'),
                          mock.call('fake_md2', 'mirror',
                                    ['/dev/sdb3', '/dev/sdc1'], 'default')],
                         mock_mu.mdcreate.call_args_list)

    @mock.patch.object(partitioning, 'os', autospec=True)
    @mock.patch.object(partitioning, 'utils', autospec=True)
    @mock.patch.object(partitioning, 'mu', autospec=True)
    @mock.patch.object(partitioning, 'lu', autospec=True)
    @mock.patch.object(partitioning, 'fu', autospec=True)
    @mock.patch.object(partitioning, 'pu', autospec=True)
    def test_do_partitioning(self, mock_pu, mock_fu, mock_lu, mock_mu,
                             mock_utils, mock_os):
        mock_os.path.exists.return_value = True
        self.action.execute()
        mock_utils.unblacklist_udev_rules.assert_called_once_with(
            udev_rules_dir='/etc/udev/rules.d',
            udev_rename_substr='.renamedrule')
        mock_utils.blacklist_udev_rules.assert_called_once_with(
            udev_rules_dir='/etc/udev/rules.d',
            udev_rules_lib_dir='/lib/udev/rules.d',
            udev_empty_rule='empty_rule', udev_rename_substr='.renamedrule')
        mock_pu_ml_expected_calls = [mock.call('/dev/sda', 'gpt'),
                                     mock.call('/dev/sdb', 'gpt'),
                                     mock.call('/dev/sdc', 'gpt')]
        self.assertEqual(mock_pu_ml_expected_calls,
                         mock_pu.make_label.call_args_list)

        mock_pu_mp_expected_calls = [
            mock.call('/dev/sda', 1, 25, 'primary'),
            mock.call('/dev/sda', 25, 225, 'primary'),
            mock.call('/dev/sda', 225, 425, 'primary'),
            mock.call('/dev/sda', 425, 625, 'primary'),
            mock.call('/dev/sda', 625, 20063, 'primary'),
            mock.call('/dev/sda', 20063, 65660, 'primary'),
            mock.call('/dev/sda', 65660, 65680, 'primary'),
            mock.call('/dev/sdb', 1, 25, 'primary'),
            mock.call('/dev/sdb', 25, 225, 'primary'),
            mock.call('/dev/sdb', 225, 65196, 'primary'),
            mock.call('/dev/sdc', 1, 25, 'primary'),
            mock.call('/dev/sdc', 25, 225, 'primary'),
            mock.call('/dev/sdc', 225, 65196, 'primary')]
        self.assertEqual(mock_pu_mp_expected_calls,
                         mock_pu.make_partition.call_args_list)

        mock_pu_spf_expected_calls = [mock.call('/dev/sda', 1, 'bios_grub'),
                                      mock.call('/dev/sdb', 1, 'bios_grub'),
                                      mock.call('/dev/sdc', 1, 'bios_grub')]
        self.assertEqual(mock_pu_spf_expected_calls,
                         mock_pu.set_partition_flag.call_args_list)

        mock_pu_sgt_expected_calls = [mock.call('/dev/sda', 4, 'fake_guid')]
        self.assertEqual(mock_pu_sgt_expected_calls,
                         mock_pu.set_gpt_type.call_args_list)

        mock_lu_p_expected_calls = [
            mock.call('/dev/sda5', metadatasize=28, metadatacopies=2),
            mock.call('/dev/sda6', metadatasize=28, metadatacopies=2),
            mock.call('/dev/sdb3', metadatasize=28, metadatacopies=2),
            mock.call('/dev/sdc3', metadatasize=28, metadatacopies=2)]
        self.assertEqual(mock_lu_p_expected_calls,
                         mock_lu.pvcreate.call_args_list)

        mock_lu_v_expected_calls = [mock.call('os', '/dev/sda5'),
                                    mock.call('image', '/dev/sda6',
                                              '/dev/sdb3', '/dev/sdc3')]
        self.assertEqual(mock_lu_v_expected_calls,
                         mock_lu.vgcreate.call_args_list)

        mock_lu_l_expected_calls = [mock.call('os', 'root', 15360),
                                    mock.call('os', 'swap', 4014),
                                    mock.call('image', 'glance', 175347)]
        self.assertEqual(mock_lu_l_expected_calls,
                         mock_lu.lvcreate.call_args_list)

        mock_fu_mf_expected_calls = [
            mock.call('ext2', '', '', '/dev/sda3'),
            mock.call('ext2', '', '', '/dev/sda4'),
            mock.call('swap', '', '', '/dev/mapper/os-swap'),
            mock.call('xfs', '', '', '/dev/mapper/image-glance')]
        self.assertEqual(mock_fu_mf_expected_calls,
                         mock_fu.make_fs.call_args_list)
