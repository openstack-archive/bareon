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
import unittest2

from bareon.drivers.data import ironic
from bareon import errors
from bareon.utils import hardware as hu
from bareon.utils import utils


class TestGetImageSchema(unittest2.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestGetImageSchema, self).__init__(*args, **kwargs)

    def test_get_image_schema(self):
        image_uri = 'test_uri'
        rsync_flags = '-a -X'
        deploy_flags = {'rsync_flags': rsync_flags}
        data = {'images': [
            {
                'image_pull_url': image_uri,
                'target': '/',
                'name': 'test'
            }
        ], 'image_deploy_flags': deploy_flags}
        self.driver = ironic.Ironic(data)

        result = self.driver._get_image_scheme()

        self.assertEqual(len(result.images), 1)

        result_image = result.images[0]
        self.assertEqual(result_image.deployment_flags, deploy_flags)
        self.assertEqual(result_image.uri, image_uri)


class TestMatchDevice(unittest2.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestMatchDevice, self).__init__(*args, **kwargs)
        self.driver = ironic.Ironic(None)

    def test_match_list_value(self):
        test_type = 'path'
        test_value = 'test_path'
        ks_disk = {'id': {'type': test_type, 'value': test_value}}
        hu_disk = {test_type: ['path1', test_value]}

        result = self.driver._match_device(hu_disk, ks_disk)

        self.assertTrue(result)

    def test_not_match_list_value(self):
        test_type = 'path'
        test_value = 'test_path'
        ks_disk = {'id': {'type': test_type, 'value': test_value}}
        hu_disk = {test_type: ['path1', 'path2']}

        result = self.driver._match_device(hu_disk, ks_disk)

        self.assertFalse(result)

    def test_match_one_value(self):
        test_type = 'path'
        test_value = 'test_path'
        ks_disk = {'id': {'type': test_type, 'value': test_value}}
        hu_disk = {test_type: test_value}

        result = self.driver._match_device(hu_disk, ks_disk)

        self.assertTrue(result)

    def test_not_match_one_value(self):
        test_type = 'path'
        test_value = 'test_path'
        ks_disk = {'id': {'type': test_type, 'value': test_value}}
        hu_disk = {test_type: 'path1'}

        result = self.driver._match_device(hu_disk, ks_disk)

        self.assertFalse(result)


class TestDiskDev(unittest2.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestDiskDev, self).__init__(*args, **kwargs)
        self.driver = ironic.Ironic(None)
        self.driver._match_device = self.mock_match_device = mock.MagicMock()

    def test_no_valid_disks(self):
        self.mock_match_device.side_effect = [False, False, False]
        self.driver._hu_disks = [{'name': 'disk1'},
                                 {'name': 'disk2'},
                                 {'name': 'disk3'}]
        ks_disk = {'id': {'type': 'name', 'value': 'not_found'}}

        self.assertRaises(errors.DiskNotFoundError, self.driver._disk_dev,
                          ks_disk)

    def test_more_than_one_valid_disk(self):
        self.mock_match_device.side_effect = [True, False, True]
        self.driver._hu_disks = [{'name': 'disk1', 'device': 'disk1'},
                                 {'name': 'disk2'},
                                 {'name': 'disk3', 'device': 'disk3'}]
        ks_disk = {'id': {'type': 'name', 'value': 'ks_disk'}}

        self.assertRaises(errors.DiskNotFoundError, self.driver._disk_dev,
                          ks_disk)

    def test_one_valid_disk(self):
        ks_disk = 'ks_disk'
        self.mock_match_device.side_effect = [True, False, False]
        self.driver._hu_disks = [{'name': 'disk1', 'device': ks_disk},
                                 {'name': 'disk2'},
                                 {'name': 'disk3'}]

        result = self.driver._disk_dev(None)

        self.assertEqual(result, 'disk1')


class TestMatchPartition(unittest2.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestMatchPartition, self).__init__(*args, **kwargs)
        self.driver = ironic.Ironic(None)

    def test_match_list_value(self):
        test_type = 'path'
        test_value = 'test_path'
        ks_partition = {'id': {'type': test_type, 'value': test_value}}
        hu_partition = {test_type: ['path1', test_value]}

        result = self.driver._match_data_by_pattern(hu_partition, ks_partition)

        self.assertTrue(result)

    def test_match_list_value_wildcard(self):
        test_type = 'path'
        test_value_wc = 'test_*'
        test_value = 'test_path'
        ks_partition = {'id': {'type': test_type, 'value': test_value_wc}}
        hu_partition = {test_type: ['path1', test_value]}

        result = self.driver._match_data_by_pattern(hu_partition, ks_partition)

        self.assertTrue(result)

    def test_not_match_list_value(self):
        test_type = 'path'
        test_value = 'test_path'
        ks_partition = {'id': {'type': test_type, 'value': test_value}}
        hu_partition = {test_type: ['path1', 'path2']}

        result = self.driver._match_data_by_pattern(hu_partition, ks_partition)

        self.assertFalse(result)

    def test_not_match_list_value_wildcard(self):
        test_type = 'path'
        test_value_wc = 'test_*'
        ks_partition = {'id': {'type': test_type, 'value': test_value_wc}}
        hu_partition = {test_type: ['path1', 'path2']}

        result = self.driver._match_data_by_pattern(hu_partition, ks_partition)

        self.assertFalse(result)

    def test_match_one_value(self):
        test_type = 'path'
        test_value = 'test_path'
        ks_partition = {'id': {'type': test_type, 'value': test_value}}
        hu_partition = {test_type: test_value}

        result = self.driver._match_data_by_pattern(hu_partition, ks_partition)

        self.assertTrue(result)

    def test_match_one_value_wildcard(self):
        test_type = 'path'
        test_value_wc = 'test_*'
        test_value = 'test_path'
        ks_partition = {'id': {'type': test_type, 'value': test_value_wc}}
        hu_partition = {test_type: test_value}

        result = self.driver._match_data_by_pattern(hu_partition, ks_partition)

        self.assertTrue(result)

    def test_not_match_one_value(self):
        test_type = 'path'
        test_value = 'test_path'
        ks_partition = {'id': {'type': test_type, 'value': test_value}}
        hu_partition = {test_type: 'path1'}

        result = self.driver._match_data_by_pattern(hu_partition, ks_partition)

        self.assertFalse(result)

    def test_not_match_one_wildcard(self):
        test_type = 'path'
        test_value_wc = 'test_*'
        ks_partition = {'id': {'type': test_type, 'value': test_value_wc}}
        hu_partition = {test_type: 'path1'}

        result = self.driver._match_data_by_pattern(hu_partition, ks_partition)

        self.assertFalse(result)


class TestDiskPartition(unittest2.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestDiskPartition, self).__init__(*args, **kwargs)
        self.driver = ironic.Ironic(None)
        self.driver._match_data_by_pattern = \
            self.mock_match_part = mock.MagicMock()

    def test_no_valid_disks(self):
        self.mock_match_part.side_effect = [False, False, False]
        self.driver._hu_partitions = [{'name': 'disk1'},
                                      {'name': 'disk2'},
                                      {'name': 'disk3'}]
        ks_disk = {'id': {'type': 'name', 'value': 'ks_disk'}}

        self.assertRaises(errors.DiskNotFoundError,
                          self.driver._disk_partition, ks_disk)

    def test_more_than_one_valid_disk(self):
        self.mock_match_part.side_effect = [True, False, True]
        self.driver._hu_partitions = [{'name': 'disk1', 'device': 'disk1'},
                                      {'name': 'disk2'},
                                      {'name': 'disk3', 'device': 'disk3'}]
        ks_disk = {'id': {'type': 'name', 'value': 'ks_disk'}}

        self.assertRaises(errors.DiskNotFoundError,
                          self.driver._disk_partition, ks_disk)

    def test_one_valid_disk(self):
        desired = ks_disk = 'ks_disk'
        self.mock_match_part.side_effect = [True, False, False]
        self.driver._hu_partitions = [{'name': ks_disk},
                                      {'name': 'disk2'},
                                      {'name': 'disk3'}]

        result = self.driver._disk_partition(None)

        self.assertEqual(result, desired)


@mock.patch('bareon.utils.hardware.get_partitions_from_udev_db')
@mock.patch('bareon.utils.hardware.get_device_ids')
class TestGetPartitionIds(unittest2.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestGetPartitionIds, self).__init__(*args, **kwargs)
        self.driver = ironic.Ironic(None)

    def test_no_devices(self, mock_ids, mock_partitions):
        mock_partitions.return_value = []
        desired = []

        result = self.driver._get_device_ids(dev_type=hu.PARTITION)

        self.assertEqual(result, desired)
        self.assertFalse(mock_ids.called)

    def test_no_ids_on_devices(self, mock_ids, mock_partitions):
        mock_partitions.return_value = parts = ['/dev/sda1', '/dev/sda2']
        mock_ids.return_value = []
        desired = []

        result = self.driver._get_device_ids(dev_type=hu.PARTITION)

        self.assertEqual(result, desired)
        mock_ids.assert_has_calls([mock.call(part) for part in parts])

    def test_success(self, mock_ids, mock_partitions):
        mock_partitions.return_value = parts = ['/dev/sda1', '/dev/sda2']
        mock_ids.side_effect = desired = [
            {'name': '/dev/sda1'},
            {'name': '/dev/sda2'}
        ]

        result = self.driver._get_device_ids(dev_type=hu.PARTITION)

        self.assertEqual(result, desired)
        mock_ids.assert_has_calls([mock.call(part) for part in parts])


class TestFindHwFstab(unittest2.TestCase):
    @mock.patch.object(utils, 'execute')
    def test__find_hw_fstab_success_single_disk(self, exec_mock):
        fs = namedtuple('fs', 'mount type device os_id')
        fss = [fs(mount='/', type='ext4', device='/dev/sda', os_id='1'),
               fs(mount='/usr', type='ext4', device='/dev/sdb', os_id='1')]

        data_driver = ironic.Ironic(None)
        data_driver._partition_scheme = ironic.objects.PartitionScheme()
        data_driver.partition_scheme.fss = fss

        exec_mock.side_effect = [('stdout', 'stderr'),
                                 ('fstab_1', 'stderr'),
                                 ('stdout', 'stderr')]

        res = data_driver._find_hw_fstab()

        self.assertEqual('\n'.join(('fstab_1',)), res)

    @mock.patch.object(utils, 'execute')
    def test__find_hw_fstab_success_two_disk(self, exec_mock):
        fs = namedtuple('fs', 'mount type device os_id')
        fss = [fs(mount='/', type='ext4', device='/dev/sda', os_id='1'),
               fs(mount='/usr', type='ext4', device='/dev/sdb', os_id='1'),
               fs(mount='/', type='ext4', device='/dev/sda', os_id='2')]

        data_driver = ironic.Ironic(None)
        data_driver._partition_scheme = ironic.objects.PartitionScheme()
        data_driver.partition_scheme.fss = fss

        exec_mock.side_effect = [('stdout', 'stderr'),
                                 ('fstab_1', 'stderr'),
                                 ('stdout', 'stderr'),
                                 ('stdout', 'stderr'),
                                 ('fstab_2', 'stderr'),
                                 ('stdout', 'stderr')]

        res = data_driver._find_hw_fstab()

        self.assertEqual('\n'.join(('fstab_1', 'fstab_2')), res)

    @mock.patch.object(utils, 'execute')
    def test__find_hw_fstab_fail_error_while_reading_fstba(self, exec_mock):
        fs = namedtuple('fs', 'mount type device os_id')
        fss = [fs(mount='/etc', type='ext4', device='/dev/sda', os_id='1'),
               fs(mount='/', type='ext4', device='/dev/sda', os_id='1')]

        data_driver = ironic.Ironic(None)
        data_driver._partition_scheme = ironic.objects.PartitionScheme()
        data_driver.partition_scheme.fss = fss
        exec_mock.side_effect = [('stdout', 'stderr'),
                                 errors.ProcessExecutionError,
                                 ('stdout', 'stderr')]

        self.assertRaises(errors.HardwarePartitionSchemeCannotBeReadError,
                          data_driver._find_hw_fstab)


class TestConvertStringSize(unittest2.TestCase):
    @mock.patch.object(ironic, 'human2bytes')
    def test_success_single_disk(self, mock_converter):
        data = {'image_deploy_flags': {'rsync_flags': '-a -A -X'},
                'partitions': [{'extra': [],
                                'id': {'type': 'name', 'value': 'vda'},
                                'size': '10000 MB',
                                'type': 'disk',
                                'volumes': [{'file_system': 'ext4',
                                             'mount': '/',
                                             'size': '5 GB',
                                             'type': 'partition'},
                                            {'file_system': 'ext4',
                                             'mount': '/var',
                                             'size': '4000',
                                             'type': 'partition'}]}]}
        ironic.convert_string_sizes(data)
        mock_converter.assert_has_calls(
            [mock.call('10000 MB'), mock.call('5 GB'), mock.call('4000')],
            any_order=True)

    @mock.patch.object(ironic, 'human2bytes')
    def test_success_two_disks(self, mock_converter):
        data = {'image_deploy_flags': {'rsync_flags': '-a -A -X'},
                'partitions': [{'extra': [],
                                'id': {'type': 'name', 'value': 'vda'},
                                'size': '10000 MB',
                                'type': 'disk',
                                'volumes': [{'file_system': 'ext4',
                                             'mount': '/',
                                             'size': '5 GB',
                                             'type': 'partition'},
                                            {'file_system': 'ext4',
                                             'mount': '/var',
                                             'size': '4000',
                                             'type': 'partition'}]},
                               {'extra': [],
                                'id': {'type': 'name', 'value': 'vdb'},
                                'size': '2000 MB',
                                'type': 'disk',
                                'volumes': [{'file_system': 'ext4',
                                             'mount': '/usr',
                                             'size': '2 GB',
                                             'type': 'partition'}]}]}
        ironic.convert_string_sizes(data)
        mock_converter.assert_has_calls(
            [mock.call('10000 MB'), mock.call('5 GB'), mock.call('4000'),
             mock.call('2000 MB'), mock.call('2 GB')], any_order=True)

    @mock.patch.object(ironic, 'human2bytes')
    def test_success_lvm_meta_size(self, mock_converter):
        data = {'image_deploy_flags': {'rsync_flags': '-a -A -X'},
                'partitions': [{'extra': [],
                                'id': {'type': 'name', 'value': 'vda'},
                                'size': '10000 MB',
                                'type': 'disk',
                                'volumes': [{'file_system': 'ext4',
                                             'mount': '/',
                                             'size': '5 GB',
                                             'type': 'partition'},
                                            {"size": "4 GB",
                                             "type": "pv",
                                             "lvm_meta_size": "64",
                                             "vg": "os"
                                             }]}]}
        ironic.convert_string_sizes(data)
        mock_converter.assert_has_calls(
            [mock.call('10000 MB'), mock.call('5 GB'), mock.call('4 GB'),
             mock.call('64')], any_order=True)

    @mock.patch.object(ironic, 'human2bytes')
    def test_success_ignore_percent(self, mock_converter):
        data = {'image_deploy_flags': {'rsync_flags': '-a -A -X'},
                'partitions': [{'extra': [],
                                'id': {'type': 'name', 'value': 'vda'},
                                'size': '10000 MB',
                                'type': 'disk',
                                'volumes': [{'file_system': 'ext4',
                                             'mount': '/',
                                             'size': '50%',
                                             'type': 'partition'},
                                            {'file_system': 'ext4',
                                             'mount': '/var',
                                             'size': '4000',
                                             'type': 'partition'}]}]}
        ironic.convert_string_sizes(data)
        mock_converter.assert_has_calls(
            [mock.call('10000 MB'), mock.call('4000')],
            any_order=True)

    @mock.patch.object(ironic, 'human2bytes')
    def test_success_ignore_remaining(self, mock_converter):
        data = {'image_deploy_flags': {'rsync_flags': '-a -A -X'},
                'partitions': [{'extra': [],
                                'id': {'type': 'name', 'value': 'vda'},
                                'size': '10000 MB',
                                'type': 'disk',
                                'volumes': [{'file_system': 'ext4',
                                             'mount': '/',
                                             'size': 'remaining',
                                             'type': 'partition'},
                                            {'file_system': 'ext4',
                                             'mount': '/var',
                                             'size': '4000',
                                             'type': 'partition'}]}]}
        ironic.convert_string_sizes(data)
        mock_converter.assert_has_calls(
            [mock.call('10000 MB'), mock.call('4000')],
            any_order=True)


class TestHumantoBytesConverter(unittest2.TestCase):
    def test_default_convertion(self):
        result = ironic.human2bytes('1000', default='GiB')
        self.assertEqual(result, 1024000)

    def test_target_convertion(self):
        result = ironic.human2bytes('1024 MiB', target='GiB')
        self.assertEqual(result, 1)

    def test_invalid_data(self):
        self.assertRaises(ValueError, ironic.human2bytes, 'invalid data')


class TestConvertPercentSizes(unittest2.TestCase):
    GRUB = ironic.DEFAULT_GRUB_SIZE
    LVM = ironic.DEFAULT_LVM_META_SIZE

    def test_single_disk_no_percent(self):
        start_data = [
            {'id': {'type': 'name', 'value': 'vda'}, 'size': 10000,
             'type': 'disk', 'volumes': [{'size': 5000, 'type': 'partition'},
                                         {'size': 4900,
                                          'type': 'partition'}]}]
        desired = [
            {'id': {'type': 'name', 'value': 'vda'}, 'size': 10000,
             'type': 'disk', 'volumes': [{'size': 5000, 'type': 'partition'},
                                         {'size': 4900,
                                          'type': 'partition'}]}]
        result = ironic._resolve_all_sizes(start_data)

        map(lambda r, d: self.assertDictEqual(r, d), result, desired)

    def test_single_disk_percent(self):
        start_data = [
            {'id': {'type': 'name', 'value': 'vda'}, 'size': 10000,
             'type': 'disk', 'volumes': [{'size': '50%', 'type': 'partition'},
                                         {'size': 4900,
                                          'type': 'partition'}]}]
        desired = [
            {'id': {'type': 'name', 'value': 'vda'}, 'size': 10000,
             'type': 'disk', 'volumes': [{'size': 5000, 'type': 'partition'},
                                         {'size': 4900,
                                          'type': 'partition'}]}]
        result = ironic._resolve_all_sizes(start_data)

        map(lambda r, d: self.assertDictEqual(r, d), result, desired)

    def test_single_disk_percent_unicode(self):
        start_data = [
            {'id': {'type': 'name', 'value': 'vda'}, 'size': 10000,
             'type': 'disk', 'volumes': [{'size': u'50%', 'type': 'partition'},
                                         {'size': 4900,
                                          'type': 'partition'}]}]
        desired = [
            {'id': {'type': 'name', 'value': 'vda'}, 'size': 10000,
             'type': 'disk', 'volumes': [{'size': 5000, 'type': 'partition'},
                                         {'size': 4900,
                                          'type': 'partition'}]}]
        result = ironic._resolve_all_sizes(start_data)

        map(lambda r, d: self.assertDictEqual(r, d), result, desired)

    def test_single_disk_without_size(self):
        start_data = [
            {'id': {'type': 'name', 'value': 'vda'},
             'type': 'disk', 'volumes': [{'size': '50%', 'type': 'partition'},
                                         {'size': 4900,
                                          'type': 'partition'}]}]

        self.assertRaises(ValueError, ironic._resolve_all_sizes, start_data)

    def test_single_disk_insufficient_size(self):
        start_data = [
            {'id': {'type': 'name', 'value': 'vda'}, 'size': 10000,
             'type': 'disk', 'volumes': [{'size': '50%', 'type': 'partition'},
                                         {'size': 6000,
                                          'type': 'partition'}]}]

        self.assertRaises(ValueError, ironic._resolve_all_sizes, start_data)

    def test_single_disk_with_vg(self):
        start_data = [{'id': {'type': 'name', 'value': 'vda'},
                       'size': 10000,
                       'type': 'disk',
                       'volumes': [{'size': '50%', 'type': 'partition'},
                                   {'size': '49%', 'type': 'pv',
                                    'vg': 'home'}]},
                      {'id': 'home',
                       'type': 'vg',
                       'volumes': [{'file_system': 'ext3',
                                    'mount': '/home',
                                    'name': 'home',
                                    'size': "100%",
                                    'type': 'lv'}]}]

        desired = [{'id': {'type': 'name', 'value': 'vda'},
                    'size': 10000,
                    'type': 'disk',
                    'volumes': [{'size': 5000, 'type': 'partition'},
                                {'size': 4900, 'type': 'pv',
                                 'vg': 'home'}]},
                   {'id': 'home',
                    'type': 'vg',
                    'volumes': [{'file_system': 'ext3',
                                 'mount': '/home',
                                 'name': 'home',
                                 'size': 4900 - self.LVM,
                                 'type': 'lv'}]}]
        result = ironic._resolve_all_sizes(start_data)

        map(lambda r, d: self.assertDictEqual(r, d), result, desired)

    def test_single_disk_with_vg_insufficient_size(self):
        start_data = [{'id': {'type': 'name', 'value': 'vda'},
                       'size': 10000,
                       'type': 'disk',
                       'volumes': [{'size': '50%', 'type': 'partition'},
                                   {'size': '49%', 'type': 'pv',
                                    'vg': 'home'}]},
                      {'id': 'home',
                       'type': 'vg',
                       'volumes': [{'file_system': 'ext3',
                                    'mount': '/home',
                                    'name': 'home',
                                    'size': "60%",
                                    'type': 'lv'},
                                   {'file_system': 'ext3',
                                    'mount': '/media',
                                    'name': 'media',
                                    'size': "60%",
                                    'type': 'lv'}]}]

        self.assertRaises(ValueError, ironic._resolve_all_sizes, start_data)

    def test_single_disk_with_vg_size_more_than_100_percent(self):
        start_data = [{'id': {'type': 'name', 'value': 'vda'},
                       'size': 10000,
                       'type': 'disk',
                       'volumes': [{'size': '50%', 'type': 'partition'},
                                   {'size': '49%', 'type': 'pv',
                                    'vg': 'home'}]},
                      {'id': 'home',
                       'type': 'vg',
                       'volumes': [{'file_system': 'ext3',
                                    'mount': '/home',
                                    'name': 'home',
                                    'size': "101%",
                                    'type': 'lv'}]}]

        self.assertRaises(ValueError, ironic._resolve_all_sizes, start_data)

    def test_single_disk_with_vg_lvm_meta_size(self):
        start_data = [{'id': {'type': 'name', 'value': 'vda'},
                       'size': 10000,
                       'type': 'disk',
                       'volumes': [{'size': '50%', 'type': 'partition'},
                                   {'size': '49%', 'type': 'pv',
                                    'vg': 'home',
                                    'lvm_meta_size': 49}]},
                      {'id': 'home',
                       'type': 'vg',
                       'volumes': [{'file_system': 'ext3',
                                    'mount': '/home',
                                    'name': 'home',
                                    'size': "100%",
                                    'type': 'lv'}]}]

        desired = [{'id': {'type': 'name', 'value': 'vda'},
                    'size': 10000,
                    'type': 'disk',
                    'volumes': [{'size': 5000, 'type': 'partition'},
                                {'size': 4900, 'type': 'pv',
                                 'vg': 'home',
                                 'lvm_meta_size': 49}]},
                   {'id': 'home',
                    'type': 'vg',
                    'volumes': [{'file_system': 'ext3',
                                 'mount': '/home',
                                 'name': 'home',
                                 'size': 4900 - 49,
                                 'type': 'lv'}]}]
        result = ironic._resolve_all_sizes(start_data)
        map(lambda r, d: self.assertDictEqual(r, d), result, desired)

    def test_single_disk_remaining(self):
        start_data = [
            {'id': {'type': 'name', 'value': 'vda'}, 'size': 10000,
             'type': 'disk',
             'volumes': [{'size': '50%', 'type': 'partition', 'mount': '/'},
                         {'size': 'remaining', 'type': 'partition',
                          'mount': '/home'}]}]
        desired = [
            {'id': {'type': 'name', 'value': 'vda'}, 'size': 10000,
             'type': 'disk',
             'volumes': [{'size': 5000, 'type': 'partition', 'mount': '/'},
                         {'size': 5000 - self.GRUB, 'type': 'partition',
                          'mount': '/home'}]}]
        result = ironic._resolve_all_sizes(start_data)

        map(lambda r, d: self.assertDictEqual(r, d), result, desired)

    def test_single_disk_remaining_nothing_left(self):
        start_data = [
            {'id': {'type': 'name', 'value': 'vda'}, 'size': 10000,
             'type': 'disk',
             'volumes': [{'size': 10000 - self.GRUB, 'type': 'partition',
                          'mount': '/'},
                         {'size': 'remaining', 'type': 'partition',
                          'mount': '/home'}]}]
        self.assertRaises(ValueError, ironic._resolve_all_sizes, start_data)

    def test_single_disk_remaining_insufficient_size(self):
        start_data = [
            {'id': {'type': 'name', 'value': 'vda'}, 'size': 10000,
             'type': 'disk',
             'volumes': [{'size': 'remaining', 'type': 'partition',
                          'mount': '/'},
                         {'size': 11000, 'type': 'partition',
                          'mount': '/home'}]}]
        self.assertRaises(ValueError, ironic._resolve_all_sizes, start_data)

    def test_single_disk_with_lv_remaining(self):
        start_data = [{'id': {'type': 'name', 'value': 'vda'},
                       'size': 10000,
                       'type': 'disk',
                       'volumes': [{'mount': '/',
                                    'size': '50%',
                                    'type': 'partition'},
                                   {'size': '49%',
                                    'type': 'pv',
                                    'vg': 'home'}]},
                      {'id': 'home',
                       'type': 'vg',
                       'volumes': [{'mount': '/var',
                                    'size': 'remaining',
                                    'type': 'lv'},
                                   {'mount': '/home',
                                    'size': '30%',
                                    'type': 'lv'}]}]

        desired = [{'id': {'type': 'name', 'value': 'vda'},
                    'size': 10000,
                    'type': 'disk',
                    'volumes': [{'mount': '/',
                                 'size': 5000,
                                 'type': 'partition'},
                                {'size': 4900,
                                 'type': 'pv',
                                 'vg': 'home'}]},
                   {'id': 'home',
                    'type': 'vg',
                    'volumes': [{'mount': '/var',
                                 'size': 4836 - (int(0.3 * 4836)),
                                 'type': 'lv'},
                                {'mount': '/home',
                                 'size': int(0.3 * 4836),
                                 'type': 'lv'}]}]

        result = ironic._resolve_all_sizes(start_data)
        map(lambda r, d: self.assertDictEqual(r, d), result, desired)

    def test_single_disk_with_pv_and_lv_remaining(self):
        disk_size = 10000
        start_data = [{'id': {'type': 'name', 'value': 'vda'},
                       'size': disk_size,
                       'type': 'disk',
                       'volumes': [{'mount': '/',
                                    'size': '50%',
                                    'type': 'partition'},
                                   {'size': 'remaining',
                                    'type': 'pv',
                                    'vg': 'home'}]},
                      {'id': 'home',
                       'type': 'vg',
                       'volumes': [{'mount': '/var',
                                    'size': 'remaining',
                                    'type': 'lv'},
                                   {'mount': '/home',
                                    'size': '30%',
                                    'type': 'lv'}]}]

        expected_partition_size = disk_size * 0.50
        expected_home_pv_size = (disk_size - expected_partition_size -
                                 ironic.DEFAULT_GRUB_SIZE)
        expected_home_lv_size = int((expected_home_pv_size -
                                     ironic.DEFAULT_LVM_META_SIZE) * 0.3)
        expected_var_lv_size = (expected_home_pv_size - expected_home_lv_size -
                                ironic.DEFAULT_LVM_META_SIZE)
        desired = [{'id': {'type': 'name', 'value': 'vda'},
                    'size': disk_size,
                    'type': 'disk',
                    'volumes': [{'mount': '/',
                                 'size': expected_partition_size,
                                 'type': 'partition'},
                                {'size': expected_home_pv_size,
                                 'type': 'pv',
                                 'vg': 'home'}]},
                   {'id': 'home',
                    'type': 'vg',
                    'volumes': [{'mount': '/var',
                                 'size': expected_var_lv_size,
                                 'type': 'lv'},
                                {'mount': '/home',
                                 'size': expected_home_lv_size,
                                 'type': 'lv'}]}]

        result = ironic._resolve_all_sizes(start_data)
        map(lambda r, d: self.assertDictEqual(r, d), result, desired)

    def test_single_disk_multiple_remaining(self):
        start_data = [
            {'id': {'type': 'name', 'value': 'vda'}, 'size': 10000,
             'type': 'disk',
             'volumes': [{'size': 'remaining', 'type': 'partition',
                          'mount': '/'},
                         {'size': 'remaining', 'type': 'partition',
                          'mount': '/home'}]}]
        self.assertRaises(ValueError, ironic._resolve_all_sizes, start_data)

    def test_single_disk_with_vg_reverse_order(self):
        start_data = [{'id': 'home',
                       'type': 'vg',
                       'volumes': [{'file_system': 'ext3',
                                    'mount': '/home',
                                    'name': 'home',
                                    'size': "100%",
                                    'type': 'lv'}]},
                      {'id': {'type': 'name', 'value': 'vda'},
                       'size': 10000,
                       'type': 'disk',
                       'volumes': [{'size': '50%', 'type': 'partition'},
                                   {'size': '49%', 'type': 'pv',
                                    'vg': 'home'}]}]

        desired = [{'id': {'type': 'name', 'value': 'vda'},
                    'size': 10000,
                    'type': 'disk',
                    'volumes': [{'size': 5000, 'type': 'partition'},
                                {'size': 4900, 'type': 'pv',
                                 'vg': 'home'}]},
                   {'id': 'home',
                    'type': 'vg',
                    'volumes': [{'file_system': 'ext3',
                                 'mount': '/home',
                                 'name': 'home',
                                 'size': 4900 - self.LVM,
                                 'type': 'lv'}]}]

        result = ironic._resolve_all_sizes(start_data)

        map(lambda r, d: self.assertDictEqual(r, d), result, desired)

    def test_single_disk_with_vg_multiple_pv(self):
        start_data = [{'id': {'type': 'name', 'value': 'vda'},
                       'size': 10000,
                       'type': 'disk',
                       'volumes': [
                           {'size': 7000, 'type': 'pv', 'vg': 'home'}]},
                      {'id': {'type': 'name', 'value': 'vdb'},
                       'size': 5000,
                       'type': 'disk',
                       'volumes': [
                           {'size': 4000, 'type': 'pv', 'vg': 'home'}]},
                      {'id': 'home',
                       'type': 'vg',
                       'volumes': [{'file_system': 'ext3',
                                    'mount': '/home',
                                    'name': 'home',
                                    'size': '50%',
                                    'type': 'lv'}]}]

        desired = [{'id': {'type': 'name', 'value': 'vda'},
                    'size': 10000,
                    'type': 'disk',
                    'volumes': [{'size': 7000, 'type': 'pv', 'vg': 'home'}]},
                   {'id': {'type': 'name', 'value': 'vdb'},
                    'size': 5000,
                    'type': 'disk',
                    'volumes': [{'size': 4000, 'type': 'pv', 'vg': 'home'}]},
                   {'id': 'home',
                    'type': 'vg',
                    'volumes': [{'file_system': 'ext3',
                                 'mount': '/home',
                                 'name': 'home',
                                 'size': 5500 - self.LVM,
                                 'type': 'lv'}]}]

        result = ironic._resolve_all_sizes(start_data)

        map(lambda r, d: self.assertDictEqual(r, d), result, desired)


class TestProcessPartition(unittest2.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestProcessPartition, self).__init__(*args, **kwargs)
        self.driver = ironic.Ironic(None)
        self.driver._partition_data = self.mock_part_data = mock.MagicMock()
        self.driver._add_partition = self.mock_add_part = mock.MagicMock()
        self.mock_add_part.return_value = self.mock_part = mock.MagicMock()
        self.driver.get_os_ids = self.mock_get_os_ids = mock.MagicMock()
        self.driver.get_image_ids = self.mock_get_image_ids = mock.MagicMock()

    def test_with_partition_guid(self):
        mock_volume = {'partition_guid': 'test_guid'}

        self.driver._process_partition(mock_volume, None, None, None)

        self.mock_part.set_guid.assert_called_once_with('test_guid')

    def test_no_mount_option(self):
        mock_volume = {}
        mock_part_schema = mock.MagicMock()

        self.driver._process_partition(mock_volume, None, None, None)

        self.assertEqual(mock_part_schema.call_count, 0)

    def test_none_mount_option(self):
        mock_volume = {'mount': 'none'}
        mock_part_schema = mock.MagicMock()

        self.driver._process_partition(mock_volume, None, None, None)

        self.assertEqual(mock_part_schema.call_count, 0)

    def test_non_boot_volume_non_default(self):
        mock_volume = {'mount': '/', 'file_system': 'ext4',
                       'fstab_options': 'noatime', 'fstab_enabled': False,
                       'disk_label': 'test_label'}
        part_schema = ironic.objects.PartitionScheme()
        parted = part_schema.add_parted(name='test_parted', label='gpt')

        self.driver._process_partition(mock_volume, None, parted,
                                       part_schema)
        self.assertEqual(len(part_schema.fss), 1)

        fs = part_schema.fss[0]
        self.assertEqual(fs.type, 'ext4')
        self.assertEqual(fs.label, ' -L test_label ')
        self.assertEqual(fs.fstab_options, 'noatime')
        self.assertEqual(fs.fstab_enabled, False)
        self.assertEqual(fs.mount, '/')

        self.assertFalse(self.driver._boot_done)

    def test_non_boot_volume_default(self):
        mock_volume = {'mount': '/'}
        part_schema = ironic.objects.PartitionScheme()
        parted = part_schema.add_parted(name='test_parted', label='gpt')

        self.driver._process_partition(mock_volume, None, parted,
                                       part_schema)
        self.assertEqual(len(part_schema.fss), 1)

        fs = part_schema.fss[0]
        self.assertEqual(fs.type, 'xfs')
        self.assertEqual(fs.label, '')
        self.assertEqual(fs.fstab_options, 'defaults')
        self.assertEqual(fs.fstab_enabled, True)
        self.assertEqual(fs.mount, '/')

        self.assertFalse(self.driver._boot_done)

    def test_already_boot_volume(self):
        mock_volume = {'mount': '/boot'}
        self.driver._boot_done = True

        self.driver._process_partition(mock_volume, None, mock.MagicMock(),
                                       mock.MagicMock())

        self.assertTrue(self.driver._boot_done)

    def test_boot_volume(self):
        mock_volume = {'mount': '/boot'}

        self.driver._process_partition(mock_volume, None, mock.MagicMock(),
                                       mock.MagicMock())

        self.assertTrue(self.driver._boot_done)
