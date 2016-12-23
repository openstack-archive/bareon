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

import time

import mock
import unittest2

from bareon import errors
from bareon.tests import utils as test_utils
from bareon.utils import block_device
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
    @mock.patch('bareon.utils.block_device.Disk.new_by_device_scan')
    @mock.patch.object(utils, 'execute')
    def test_make_partition(self, mock_exec, mock_disk, mock_rerd, mock_udev):
        # should run parted OS command
        # in order to create new partition
        mock_exec.return_value = ('out', '')

        mock_disk.return_value = disk_instance = mock.Mock()
        disk_instance.allocate.return_value = block_device.Partition(
            disk_instance,
            block_device._BlockDevice(None, 100, 512),
            100, None, None)

        pu.make_partition('/dev/fake', 100, 200, 'primary')
        mock_exec.assert_called_once_with(
            'parted', '-a', 'optimal', '-s', '/dev/fake',
            'unit', 's', 'mkpart', 'primary', '100', '199')
        mock_udev.assert_called_once_with()
        mock_rerd.assert_called_once_with('/dev/fake', out='out')

    @mock.patch.object(utils, 'udevadm_settle')
    @mock.patch.object(pu, 'reread_partitions')
    @mock.patch('bareon.utils.block_device.Disk.new_by_device_scan')
    @mock.patch.object(utils, 'execute')
    def test_make_partition_minimal(self, mock_exec, mock_disk, mock_rerd,
                                    mock_udev):
        # should run parted OS command
        # in order to create new partition
        mock_exec.return_value = ('out', '')

        mock_disk.return_value = disk_instance = mock.Mock()
        disk_instance.allocate.return_value = block_device.Partition(
            disk_instance,
            block_device._BlockDevice(None, 100, 512),
            100, None, None)

        pu.make_partition('/dev/fake', 100, 200, 'primary',
                          alignment='minimal')
        mock_exec.assert_called_once_with(
            'parted', '-a', 'minimal', '-s', '/dev/fake',
            'unit', 's', 'mkpart', 'primary', '100', '199')
        mock_udev.assert_called_once_with()
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

    # FIXME(dbogun): do this kind of test on utils.block_device.Disk level
    @mock.patch('bareon.utils.block_device.Disk.new_by_device_scan')
    @mock.patch.object(utils, 'execute')
    def test_make_partition_overlaps_other_parts(
            self, mock_exec, disk_factory):
        # should check if begin or end overlap other partitions
        # should raise exception if it does

        mock_exec.return_value = 'mock: exec-stdout', 'mock: exec-stderr'

        disk = block_device.Disk(
            block_device._BlockDevice('/dev/fake', 300, 512), 'gpt')
        disk.register(
            block_device.Partition(
                disk, block_device._BlockDevice(None, 100, 512),
                100, 1, 0x8300))

        disk_factory.return_value = disk

        self.assertRaises(errors.BlockDeviceSchemeError, pu.make_partition,
                          '/dev/fake', 0 * 512, 102 * 512, 'primary')

    @mock.patch.object(utils, 'udevadm_settle')
    @mock.patch.object(pu, 'reread_partitions')
    @mock.patch.object(pu, 'scan_device')
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

    @mock.patch.object(pu, 'scan_device')
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
    def test_scan_device(self):
        with test_utils.BlockDeviceMock('sample1'):
            disk = pu.scan_device('/dev/sda')

        expected = {
            'generic': {
                'dev': '/dev/sda',
                'table': 'mbr',
                'has_bootloader': True,
                'block_size': 512
            },
            "parts": [
                {
                    'size': 1015808,
                    'begin': 32768,
                    'end': 1048575,
                    'fstype': 'free',
                    'master_dev': '/dev/sda'
                },
                {
                    'num': 1,
                    'size': 25598885888,
                    'begin': 1048576,
                    'end': 25599934463,
                    'fstype': 'ext4',
                    'guid': '70D0A7D8-FA3B-4FF0-922D-1DFDBF1072F2',
                    'uuid': 'd48c3dcf-73df-4c6d-864f-1c758469ee41',
                    'type': "primary",
                    'name': '/dev/sda1',
                    'master_dev': '/dev/sda',
                    'flags': []
                },
                {
                    'num': 3,
                    'size': 25599934464,
                    'begin': 25599934464,
                    'end': 51199868927,
                    'fstype': 'ext4',
                    'guid': '6B4A0679-831E-44C9-8500-90905960F797',
                    'uuid': '07429a83-8583-49c9-91f1-abe4d78d163f',
                    'type': 'primary',
                    'name': '/dev/sda3',
                    'master_dev': '/dev/sda',
                    'flags': []
                },
                {
                    'size': 1048576,
                    'begin': 51199868928,
                    'end': 51200917503,
                    'fstype': 'free',
                    'master_dev': '/dev/sda'
                },
                {
                    'num': 5,
                    'size': 10239344640,
                    'begin': 51200917504,
                    'end': 61440262143,
                    'fstype': 'swap',
                    'guid': '04003F45-3426-47EA-BAA0-0220F2CC6B6C',
                    'uuid': 'ad5b5e5e-b999-468f-8ebb-032c7281e2bd',
                    'type': "logical",
                    'name': '/dev/sda5',
                    'master_dev': '/dev/sda',
                    'flags': []
                },
                {
                    'size': 1048576,
                    'begin': 61440262144,
                    'end': 61441310719,
                    'fstype': 'free',
                    'master_dev': '/dev/sda'
                },
                {
                    'num': 6,
                    'size': 930172895232,
                    'begin': 61441310720,
                    'end': 991614205951,
                    'fstype': 'ext4',
                    'guid': 'BC2B584B-4A02-47A3-ACA5-9764F6CC5A40',
                    'uuid': 'd625c5de-a050-49a1-bdba-b02bb5cf726e',
                    'type': 'logical',
                    'name': '/dev/sda6',
                    'master_dev': '/dev/sda',
                    'flags': []
                },
                {
                    'size': 1048576,
                    'begin': 991614205952,
                    'end': 991615254527,
                    'fstype': 'free',
                    'master_dev': '/dev/sda'
                },
                {
                    'num': 7,
                    'size': 8588886016,
                    'begin': 991615254528,
                    'end': 1000204140543,
                    'fstype': 'ext4',
                    'guid': '28404402-736E-46D3-B623-F6F2868079B8',
                    'uuid': '746baf3e-e06d-4057-b165-be4a49f195e4',
                    'type': 'logical',
                    'name': '/dev/sda7',
                    'master_dev': '/dev/sda',
                    'flags': []
                },
                {
                    'size': 745984,
                    'begin': 1000204140544,
                    'end': 1000204886527,
                    'fstype': 'free',
                    'master_dev': '/dev/sda'
                }
            ]
        }
        self.assertDictEqual(disk, expected)

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
