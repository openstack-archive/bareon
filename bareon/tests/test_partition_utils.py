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
import time

import mock
import unittest2

from bareon import errors
from bareon.utils import partition as pu
from bareon.utils import utils


class TestPartitionUtils(unittest2.TestCase):
    @mock.patch.object(pu, 'make_label')
    def test_wipe(self, mock_label):
        # should run call make_label method
        # in order to create new empty table which we think
        # is equivalent to wiping the old one
        pu.wipe('/dev/fake')
        mock_label.assert_called_once_with('/dev/fake')

    @mock.patch.object(utils, 'udevadm_settle')
    @mock.patch.object(pu, 'reread_partitions')
    @mock.patch.object(utils, 'execute')
    def test_make_label(self, mock_exec, mock_rerd, mock_udev):
        # should run parted OS command
        # in order to create label on a device
        mock_exec.return_value = ('out', '')

        # gpt by default
        pu.make_label('/dev/fake')
        mock_exec_expected_calls = [
            mock.call('parted', '-s', '/dev/fake', 'mklabel', 'gpt',
                      check_exit_code=[0, 1])]
        self.assertEqual(mock_exec_expected_calls, mock_exec.call_args_list)
        mock_rerd.assert_called_once_with('/dev/fake', out='out')
        mock_udev.assert_called_once_with()
        mock_exec.reset_mock()
        mock_rerd.reset_mock()
        mock_udev.reset_mock()

        # label is set explicitly
        pu.make_label('/dev/fake', label='msdos')
        mock_exec_expected_calls = [
            mock.call('parted', '-s', '/dev/fake', 'mklabel', 'msdos',
                      check_exit_code=[0, 1])]
        self.assertEqual(mock_exec_expected_calls, mock_exec.call_args_list)
        mock_udev.assert_called_once_with()
        mock_rerd.assert_called_once_with('/dev/fake', out='out')

    def test_make_label_wrong_label(self):
        # should check if label is valid
        # should raise exception if it is not
        self.assertRaises(errors.WrongPartitionLabelError,
                          pu.make_label, '/dev/fake', 'wrong')

    @mock.patch.object(utils, 'udevadm_settle')
    @mock.patch.object(pu, 'reread_partitions')
    @mock.patch.object(utils, 'execute')
    def test_set_partition_flag(self, mock_exec, mock_rerd, mock_udev):
        # should run parted OS command
        # in order to set flag on a partition
        mock_exec.return_value = ('out', '')

        # default state is 'on'
        pu.set_partition_flag('/dev/fake', 1, 'boot')
        mock_exec_expected_calls = [
            mock.call('parted', '-s', '/dev/fake', 'set', '1', 'boot', 'on',
                      check_exit_code=[0, 1])]
        mock_udev.assert_called_once_with()
        self.assertEqual(mock_exec_expected_calls, mock_exec.call_args_list)
        mock_rerd.assert_called_once_with('/dev/fake', out='out')
        mock_exec.reset_mock()
        mock_rerd.reset_mock()
        mock_udev.reset_mock()

        # if state argument is given use it
        pu.set_partition_flag('/dev/fake', 1, 'boot', state='off')
        mock_exec_expected_calls = [
            mock.call('parted', '-s', '/dev/fake', 'set', '1', 'boot', 'off',
                      check_exit_code=[0, 1])]
        mock_udev.assert_called_once_with()
        self.assertEqual(mock_exec_expected_calls, mock_exec.call_args_list)
        mock_rerd.assert_called_once_with('/dev/fake', out='out')

    @mock.patch.object(utils, 'execute')
    def test_set_partition_flag_wrong_flag(self, mock_exec):
        # should check if flag is valid
        # should raise exception if it is not
        self.assertRaises(errors.WrongPartitionSchemeError,
                          pu.set_partition_flag,
                          '/dev/fake', 1, 'wrong')

    @mock.patch.object(utils, 'execute')
    def test_set_partition_flag_wrong_state(self, mock_exec):
        # should check if flag is valid
        # should raise exception if it is not
        self.assertRaises(errors.WrongPartitionSchemeError,
                          pu.set_partition_flag,
                          '/dev/fake', 1, 'boot', state='wrong')

    @mock.patch.object(utils, 'udevadm_settle')
    @mock.patch.object(pu, 'reread_partitions')
    @mock.patch.object(pu, 'info')
    @mock.patch.object(utils, 'execute')
    def test_make_partition(self, mock_exec, mock_info, mock_rerd, mock_udev):
        # should run parted OS command
        # in order to create new partition
        mock_exec.return_value = ('out', '')

        mock_info.return_value = {
            'parts': [
                {'begin': 0, 'end': 1000, 'fstype': 'free'},
            ]
        }
        pu.make_partition('/dev/fake', 100, 200, 'primary')
        mock_exec_expected_calls = [
            mock.call('parted', '-a', 'optimal', '-s', '/dev/fake', 'unit',
                      'MiB', 'mkpart', 'primary', '100', '200',
                      check_exit_code=[0, 1])]
        mock_udev.assert_called_once_with()
        self.assertEqual(mock_exec_expected_calls, mock_exec.call_args_list)
        mock_rerd.assert_called_once_with('/dev/fake', out='out')

    @mock.patch.object(utils, 'udevadm_settle')
    @mock.patch.object(pu, 'reread_partitions')
    @mock.patch.object(pu, 'info')
    @mock.patch.object(utils, 'execute')
    def test_make_partition_minimal(self, mock_exec, mock_info, mock_rerd,
                                    mock_udev):
        # should run parted OS command
        # in order to create new partition
        mock_exec.return_value = ('out', '')

        mock_info.return_value = {
            'parts': [
                {'begin': 0, 'end': 1000, 'fstype': 'free'},
            ]
        }
        pu.make_partition('/dev/fake', 100, 200, 'primary',
                          alignment='minimal')
        mock_exec_expected_calls = [
            mock.call('parted', '-a', 'minimal', '-s', '/dev/fake', 'unit',
                      'MiB', 'mkpart', 'primary', '100', '200',
                      check_exit_code=[0, 1])]
        mock_udev.assert_called_once_with()
        self.assertEqual(mock_exec_expected_calls, mock_exec.call_args_list)
        mock_rerd.assert_called_once_with('/dev/fake', out='out')

    def test_make_partition_wrong_alignment(self):
        self.assertRaises(errors.WrongPartitionSchemeError, pu.make_partition,
                          '/dev/fake', 1, 10, 'primary', 'invalid')

    @mock.patch.object(utils, 'execute')
    def test_make_partition_wrong_ptype(self, mock_exec):
        # should check if partition type is one of
        # 'primary' or 'logical'
        # should raise exception if it is not
        self.assertRaises(errors.WrongPartitionSchemeError, pu.make_partition,
                          '/dev/fake', 200, 100, 'wrong')

    @mock.patch.object(utils, 'execute')
    def test_make_partition_begin_overlaps_end(self, mock_exec):
        # should check if begin is less than end
        # should raise exception if it isn't
        self.assertRaises(errors.WrongPartitionSchemeError, pu.make_partition,
                          '/dev/fake', 200, 100, 'primary')

    @mock.patch.object(pu, 'info')
    @mock.patch.object(utils, 'execute')
    def test_make_partition_overlaps_other_parts(self, mock_exec, mock_info):
        # should check if begin or end overlap other partitions
        # should raise exception if it does
        mock_info.return_value = {
            'parts': [
                {'begin': 0, 'end': 100, 'fstype': 'free'},
                {'begin': 100, 'end': 200, 'fstype': 'notfree'},
                {'begin': 200, 'end': 300, 'fstype': 'free'}
            ]
        }
        self.assertRaises(errors.WrongPartitionSchemeError, pu.make_partition,
                          '/dev/fake', 99, 101, 'primary')
        self.assertRaises(errors.WrongPartitionSchemeError, pu.make_partition,
                          '/dev/fake', 100, 200, 'primary')
        self.assertRaises(errors.WrongPartitionSchemeError, pu.make_partition,
                          '/dev/fake', 200, 301, 'primary')
        self.assertEqual(mock_info.call_args_list,
                         [mock.call('/dev/fake')] * 3)

    @mock.patch.object(utils, 'udevadm_settle')
    @mock.patch.object(pu, 'reread_partitions')
    @mock.patch.object(pu, 'info')
    @mock.patch.object(utils, 'execute')
    def test_remove_partition(self, mock_exec, mock_info, mock_rerd,
                              mock_udev):
        # should run parted OS command
        # in order to remove partition
        mock_exec.return_value = ('out', '')
        mock_info.return_value = {
            'parts': [
                {
                    'begin': 1,
                    'end': 100,
                    'size': 100,
                    'num': 1,
                    'fstype': 'ext2'
                },
                {
                    'begin': 100,
                    'end': 200,
                    'size': 100,
                    'num': 2,
                    'fstype': 'ext2'
                }
            ]
        }
        pu.remove_partition('/dev/fake', 1)
        mock_exec_expected_calls = [
            mock.call('parted', '-s', '/dev/fake', 'rm', '1',
                      check_exit_code=[0, 1])]
        mock_udev.assert_called_once_with()
        self.assertEqual(mock_exec_expected_calls, mock_exec.call_args_list)
        mock_rerd.assert_called_once_with('/dev/fake', out='out')

    @mock.patch.object(pu, 'info')
    @mock.patch.object(utils, 'execute')
    def test_remove_partition_notexists(self, mock_exec, mock_info):
        # should check if partition does exist
        # should raise exception if it doesn't
        mock_info.return_value = {
            'parts': [
                {
                    'begin': 1,
                    'end': 100,
                    'size': 100,
                    'num': 1,
                    'fstype': 'ext2'
                },
                {
                    'begin': 100,
                    'end': 200,
                    'size': 100,
                    'num': 2,
                    'fstype': 'ext2'
                }
            ]
        }
        self.assertRaises(errors.PartitionNotFoundError, pu.remove_partition,
                          '/dev/fake', 3)

    @mock.patch.object(utils, 'udevadm_settle')
    @mock.patch.object(utils, 'execute')
    def test_set_gpt_type(self, mock_exec, mock_udev):
        pu.set_gpt_type('dev', 'num', 'type')
        mock_exec_expected_calls = [
            mock.call('sgdisk', '--typecode=%s:%s' % ('num', 'type'), 'dev',
                      check_exit_code=[0])]
        self.assertEqual(mock_exec_expected_calls, mock_exec.call_args_list)
        mock_udev.assert_called_once_with()

    @mock.patch.object(utils, 'udevadm_settle', mock.Mock())
    @mock.patch.object(pu, '_disk_dummy')
    @mock.patch.object(pu, '_disk_info_by_lsblk', mock.Mock())
    @mock.patch.object(pu, '_disk_info_by_file', mock.Mock())
    @mock.patch.object(pu, '_disk_info_by_parted')
    def test_info(self, _disk_info_by_parted, disk_dummy):
        disk_dummy.return_value = {
            'generic': {},
            'parts': [{'flags': set(), 'num': None}]}
        pu.info('/dev/fake')
        self.assertEqual(1, _disk_info_by_parted.call_count)

    @mock.patch.object(utils, 'udevadm_settle', mock.Mock())
    @mock.patch.object(pu, '_disk_dummy')
    @mock.patch.object(pu, '_disk_info_by_lsblk', mock.Mock())
    @mock.patch.object(pu, '_disk_info_by_file', mock.Mock())
    @mock.patch.object(pu, '_disk_info_by_parted')
    def test_info_no_partitions(self, _disk_info_by_parted, disk_dummy):
        disk_dummy.return_value = {
            'generic': {},
            'parts': []}
        pu.info('/dev/fake')
        self.assertEqual(0, _disk_info_by_parted.call_count)

    @mock.patch.object(utils, 'udevadm_settle', mock.Mock())
    @mock.patch.object(utils, 'execute')
    def test_info_merge(self, utils_exec):
        utils_exec.side_effect = [
            (_info_lsblk_output, ''),
            (_info_file_output, ''),
            (_info_parted_output, '')
        ]

        disk = pu.info('/dev/vda')
        partitions = disk['parts']
        partitions_by_dev = {x['dev']: x for x in partitions}

        vda2 = partitions_by_dev['/dev/vda2']
        self.assertEqual(vda2['uuid'], 'c5c1e495-a44e-4ee5-ab49-ed8012ae456e')
        self.assertEqual(vda2['fstype'], 'ext4')
        self.assertEqual(vda2['begin'], 25)
        self.assertEqual(vda2['end'], 4025)

        vda3 = partitions_by_dev['/dev/vda3']
        self.assertEqual(vda3['uuid'], 'e0f19b74-c80a-4cef-bc3f-0a51f54b23a6')
        self.assertEqual(vda3['fstype'], 'linux-swap(v1)')
        self.assertEqual(vda3['begin'], 4025)
        self.assertEqual(vda3['end'], 6025)

        vda4 = partitions_by_dev['/dev/vda4']
        self.assertEqual(vda4['uuid'], '1ebab003-f31b-43d8-9bff-fe23eb86c1db')
        self.assertEqual(vda4['fstype'], 'ext4')
        self.assertEqual(vda4['begin'], 6025)
        self.assertEqual(vda4['end'], 9925)

    @mock.patch.object(utils, 'execute')
    def test__disk_info_by_lsblk(self, utils_exec):
        utils_exec.return_value = (_info_lsblk_output, '')

        expected = {
            'generic': {
                'dev': '/dev/vda',
                'size': 11264,
                'logical_block': 512,
                'physical_block': 512,
                'has_bootloader': None,
                'model': None,
                'table': None},

            'parts': [
                {
                    'master_suffix': '1',
                    'dev': '/dev/vda1',
                    'disk_dev': '/dev/vda',
                    'name': '/dev/vda1',
                    'num': '1',
                    'size': 24,
                    'fstype': None,
                    'begin': None, 'end': None, 'type': None, 'uuid': None,
                    'flags': set([])
                }, {
                    'master_suffix': '2',
                    'dev': '/dev/vda2',
                    'disk_dev': '/dev/vda',
                    'name': '/dev/vda2',
                    'num': '2',
                    'size': 4000,
                    'fstype': 'ext4',
                    'begin': None, 'end': None, 'type': None,
                    'uuid': 'c5c1e495-a44e-4ee5-ab49-ed8012ae456e',
                    'flags': set([])
                }, {
                    'master_suffix': '3',
                    'dev': '/dev/vda3',
                    'disk_dev': '/dev/vda',
                    'name': '/dev/vda3',
                    'num': '3',
                    'size': 2000,
                    'fstype': 'swap',
                    'begin': None, 'end': None, 'type': None,
                    'uuid': 'e0f19b74-c80a-4cef-bc3f-0a51f54b23a6',
                    'flags': set([])
                }, {
                    'master_suffix': '4',
                    'dev': '/dev/vda4',
                    'disk_dev': '/dev/vda',
                    'name': '/dev/vda4',
                    'num': '4',
                    'size': 3900,
                    'fstype': 'ext4',
                    'begin': None, 'end': None, 'type': None,
                    'uuid': '1ebab003-f31b-43d8-9bff-fe23eb86c1db',
                    'flags': set([])
                }]}

        disk = pu._disk_dummy('/dev/vda')
        pu._disk_info_by_lsblk(disk)

        self.maxDiff = None
        self.assertDictEqual(expected, disk)

    @mock.patch.object(utils, 'execute')
    def test__disk_info_by_parted(self, utils_exec):
        utils_exec.return_value = (_info_parted_output, '')

        """
        1:0.02MiB:1.00MiB:0.98MiB:free;
        1:1.00MiB:25.0MiB:24.0MiB::primary:bios_grub;
        2:25.0MiB:4025MiB:4000MiB:ext4:primary:;
        3:4025MiB:6025MiB:2000MiB:linux-swap(v1):primary:;
        4:6025MiB:9925MiB:3900MiB:ext4:primary:;
        1:9925MiB:11264MiB:1339MiB:free;
        """
        expected = {
            'generic': {'dev': '/dev/vda',
                        'size': 11264,
                        'logical_block': 512,
                        'physical_block': 512,
                        'model': 'Virtio Block Device',
                        'table': 'gpt',
                        'has_bootloader': None},

            'parts': [{'dev': None, 'master_suffix': None,
                       'disk_dev': '/dev/vda', 'name': None,
                       'begin': 1, 'end': 1, 'fstype': 'free',
                       'num': None, 'size': 1, 'uuid': None,
                       'type': None, 'flags': set()},
                      {'dev': None, 'master_suffix': None,
                       'disk_dev': '/dev/vda', 'name': None,
                       'begin': 9925, 'end': 11264,
                       'fstype': 'free', 'num': None, 'size': 1339,
                       'uuid': None, 'type': None, 'flags': set()},
                      {'dev': '/dev/vda1', 'master_suffix': '1',
                       'disk_dev': '/dev/vda', 'name': '/dev/vda1',
                       'begin': 1, 'end': 25, 'fstype': None,
                       'num': '1', 'size': 24, 'uuid': None,
                       'type': 'primary', 'flags': set(('bios_grub',))},
                      {'dev': '/dev/vda2', 'master_suffix': '2',
                       'disk_dev': '/dev/vda', 'name': '/dev/vda2',
                       'begin': 25, 'end': 4025, 'fstype': 'ext4',
                       'num': '2', 'size': 4000, 'uuid': None,
                       'type': 'primary', 'flags': set()},
                      {'dev': '/dev/vda3', 'master_suffix': '3',
                       'disk_dev': '/dev/vda', 'name': '/dev/vda3',
                       'begin': 4025, 'end': 6025, 'fstype': 'linux-swap(v1)',
                       'num': '3', 'size': 2000, 'uuid': None,
                       'type': 'primary', 'flags': set()},
                      {'dev': '/dev/vda4', 'master_suffix': '4',
                       'disk_dev': '/dev/vda', 'name': '/dev/vda4',
                       'begin': 6025, 'end': 9925, 'fstype': 'ext4',
                       'num': '4', 'size': 3900, 'uuid': None,
                       'type': 'primary', 'flags': set()}
                      ]}

        disk = pu._disk_dummy('/dev/fake')
        pu._disk_info_by_parted(disk)

        self.maxDiff = None
        self.assertDictEqual(expected, disk)

    @mock.patch.object(utils, 'execute')
    def test__disk_info_by_file(self, utils_exec):
        utils_exec.return_value = (_info_file_output, '')

        disk = pu._disk_dummy('/dev/fake')
        expected = copy.deepcopy(disk)
        expected['generic']['has_bootloader'] = True

        pu._disk_info_by_file(disk)

        self.assertDictEqual(expected, disk)

    @mock.patch.object(utils, 'execute')
    def test_reread_partitions_ok(self, mock_exec):
        pu.reread_partitions('/dev/fake', out='')
        self.assertEqual(mock_exec.call_args_list, [])

    @mock.patch.object(utils, 'udevadm_settle')
    @mock.patch.object(time, 'sleep')
    @mock.patch.object(utils, 'execute')
    def test_reread_partitions_device_busy(self, mock_exec, mock_sleep,
                                           mock_udev):
        mock_exec.return_value = ('', '')
        pu.reread_partitions('/dev/fake', out='_Device or resource busy_')
        mock_exec_expected = [
            mock.call('partprobe', '/dev/fake', check_exit_code=[0, 1]),
        ]
        self.assertEqual(mock_exec.call_args_list, mock_exec_expected)
        mock_sleep.assert_called_once_with(2)
        mock_udev.assert_called_once_with()

    @mock.patch.object(utils, 'execute')
    def test_reread_partitions_timeout(self, mock_exec):
        self.assertRaises(errors.BaseError, pu.reread_partitions,
                          '/dev/fake', out='Device or resource busy',
                          timeout=-40)


_info_lsblk_output = """\
vda  11811160064     512     512
vda1    25165824     512     512
vda2  4194304000     512     512 c5c1e495-a44e-4ee5-ab49-ed8012ae456e ext4
vda3  2097152000     512     512 e0f19b74-c80a-4cef-bc3f-0a51f54b23a6 swap
vda4  4089446400     512     512 1ebab003-f31b-43d8-9bff-fe23eb86c1db ext4
"""

_info_parted_output = """\
BYT;
/dev/vda:11264MiB:virtblk:512:512:gpt:Virtio Block Device:;
1:0.02MiB:1.00MiB:0.98MiB:free;
1:1.00MiB:25.0MiB:24.0MiB::primary:bios_grub;
2:25.0MiB:4025MiB:4000MiB:ext4:primary:;
3:4025MiB:6025MiB:2000MiB:linux-swap(v1):primary:;
4:6025MiB:9925MiB:3900MiB:ext4:primary:;
1:9925MiB:11264MiB:1339MiB:free;
"""

_info_file_output = ('DOS/MBR boot sector DOS/MBR boot sector DOS executable '
                     '(COM), boot code\012- data')
