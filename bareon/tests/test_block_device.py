#
# Copyright 2015 Cray Inc.  All Rights Reserved.
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

import errno
import unittest2

import mock

from bareon import errors
from bareon.utils import block_device


class TestBlockDevice(unittest2.TestCase):
    def setUp(self):
        super(TestBlockDevice, self).setUp()

        self.mock = {
            'os.stat': mock.Mock(),
            'bareon.utils.utils.execute': mock.Mock()
        }

        self.patches = {}
        for target, dummy in self.mock.items():
            self.patches[target] = p = mock.patch(target, dummy)
            p.start()
            self.addCleanup(p.stop)

    def test_disk_scan(self):
        self.mock['os.stat'].side_effect = _FakeOSStat({
            '/dev/sda': True,
            '/dev/sda1': True,
            '/dev/sda3': True,
            '/dev/sda5': True,
            '/dev/sda6': True,
            '/dev/sda7': True})

        exec_mapping = {
            ('sgdisk', '--print', '/dev/sda'): sgdisk_print_sda,
            ('sgdisk', '--info', '1', '/dev/sda'): sgdisk_info_sda1,
            ('sgdisk', '--info', '3', '/dev/sda'): sgdisk_info_sda3,
            ('sgdisk', '--info', '5', '/dev/sda'): sgdisk_info_sda5,
            ('sgdisk', '--info', '6', '/dev/sda'): sgdisk_info_sda6,
            ('sgdisk', '--info', '7', '/dev/sda'): sgdisk_info_sda7,

            ('file', '--brief', '--keep-going', '--special-files',
             '/dev/sda'): file_dev_sda,
            ('file', '--brief', '--keep-going', '--special-files',
             '/dev/sda1'): file_dev_sda1,
            ('file', '--brief', '--keep-going', '--special-files',
             '/dev/sda3'): file_dev_sda3,
            ('file', '--brief', '--keep-going', '--special-files',
             '/dev/sda5'): file_dev_sda5,
            ('file', '--brief', '--keep-going', '--special-files',
             '/dev/sda6'): file_dev_sda6,
            ('file', '--brief', '--keep-going', '--special-files',
             '/dev/sda7'): file_dev_sda7
        }
        self.mock['bareon.utils.utils.execute'].side_effect = _FakeExecute(
            exec_mapping)

        disk = block_device.Disk.new_by_device_scan('/dev/sda')

        expect = [
            (1, 2048, 49999871, 0x8300,
             '70D0A7D8-FA3B-4FF0-922D-1DFDBF1072F2'),
            (3, 49999872, 99999743, 0x8300,
             '6B4A0679-831E-44C9-8500-90905960F797'),
            (5, 100001792, 120000511, 0x8200,
             '04003F45-3426-47EA-BAA0-0220F2CC6B6C'),
            (6, 120002560, 1936746495, 0x8300,
             'BC2B584B-4A02-47A3-ACA5-9764F6CC5A40'),
            (7, 1936748544, 1953523711, 0x8300,
             '28404402-736E-46D3-B623-F6F2868079B8')]
        actual = [
            (p.index, p.begin, p.end, p.code, p.guid)
            for p in disk.partitions]

        self.assertEqual(expect, actual)


class _FakeOSStat(object):
    def __init__(self, fs):
        self.fs = fs

    def __call__(self, path):
        try:
            entry = self.fs[path]
        except KeyError:
            raise OSError(
                errno.ENOENT, 'FAKE: No such file or directory',  path)
        return entry


class _FakeExecute(object):
    def __init__(self, mapping):
        self.mapping = mapping

    def __call__(self, *cmd, **kwargs):
        try:
            result = self.mapping[cmd]
        except KeyError:
            raise errors.ProcessExecutionError(
                cmd=cmd, description='FAKE: There is no definition for this '
                                     'command')

        return result, ''


sgdisk_print_sda = ("""

***************************************************************
Found invalid GPT and valid MBR; converting MBR to GPT format
in memory.
***************************************************************

Disk /dev/sda: 1953525168 sectors, 931.5 GiB
Logical sector size: 512 bytes
Disk identifier (GUID): E5ECD5B2-ED89-48AA-8601-DC040CBC3B31
Partition table holds up to 128 entries
First usable sector is 34, last usable sector is 1953525134
Partitions will be aligned on 2048-sector boundaries
Total free space is 9581 sectors (4.7 MiB)

Number  Start (sector)    End (sector)  Size       Code  Name
   1            2048        49999871   23.8 GiB    8300  Linux filesystem
   3        49999872        99999743   23.8 GiB    8300  Linux filesystem
   5       100001792       120000511   9.5 GiB     8200  Linux swap
   6       120002560      1936746495   866.3 GiB   8300  Linux filesystem
   7      1936748544      1953523711   8.0 GiB     8300  Linux filesystem

""")

sgdisk_info_sda1 = ("""

***************************************************************
Found invalid GPT and valid MBR; converting MBR to GPT format
in memory.
***************************************************************

Partition GUID code: 0FC63DAF-8483-4772-8E79-3D69D8477DE4 (Linux filesystem)
Partition unique GUID: 70D0A7D8-FA3B-4FF0-922D-1DFDBF1072F2
First sector: 2048 (at 1024.0 KiB)
Last sector: 49999871 (at 23.8 GiB)
Partition size: 49997824 sectors (23.8 GiB)
Attribute flags: 0000000000000000
Partition name: 'Linux filesystem'

""")

sgdisk_info_sda3 = ("""

***************************************************************
Found invalid GPT and valid MBR; converting MBR to GPT format
in memory.
***************************************************************

Partition GUID code: 0FC63DAF-8483-4772-8E79-3D69D8477DE4 (Linux filesystem)
Partition unique GUID: 6B4A0679-831E-44C9-8500-90905960F797
First sector: 49999872 (at 23.8 GiB)
Last sector: 99999743 (at 47.7 GiB)
Partition size: 49999872 sectors (23.8 GiB)
Attribute flags: 0000000000000000
Partition name: 'Linux filesystem'

""")

sgdisk_info_sda5 = ("""

***************************************************************
Found invalid GPT and valid MBR; converting MBR to GPT format
in memory.
***************************************************************

Partition GUID code: 0657FD6D-A4AB-43C4-84E5-0933C84B4F4F (Linux swap)
Partition unique GUID: 04003F45-3426-47EA-BAA0-0220F2CC6B6C
First sector: 100001792 (at 47.7 GiB)
Last sector: 120000511 (at 57.2 GiB)
Partition size: 19998720 sectors (9.5 GiB)
Attribute flags: 0000000000000000
Partition name: 'Linux swap'

""")

sgdisk_info_sda6 = ("""

***************************************************************
Found invalid GPT and valid MBR; converting MBR to GPT format
in memory.
***************************************************************

Partition GUID code: 0FC63DAF-8483-4772-8E79-3D69D8477DE4 (Linux filesystem)
Partition unique GUID: BC2B584B-4A02-47A3-ACA5-9764F6CC5A40
First sector: 120002560 (at 57.2 GiB)
Last sector: 1936746495 (at 923.5 GiB)
Partition size: 1816743936 sectors (866.3 GiB)
Attribute flags: 0000000000000000
Partition name: 'Linux filesystem'
""")

sgdisk_info_sda7 = ("""

***************************************************************
Found invalid GPT and valid MBR; converting MBR to GPT format
in memory.
***************************************************************

Partition GUID code: 0FC63DAF-8483-4772-8E79-3D69D8477DE4 (Linux filesystem)
Partition unique GUID: 28404402-736E-46D3-B623-F6F2868079B8
First sector: 1936748544 (at 923.5 GiB)
Last sector: 1953523711 (at 931.5 GiB)
Partition size: 16775168 sectors (8.0 GiB)
Attribute flags: 0000000000000000
Partition name: 'Linux filesystem'

dbogun@dbogun-pc ~/devel/cray/fuel-agent
""")

file_dev_sda = (
    'DOS/MBR boot sector DOS/MBR boot sector DOS executable (COM), boot '
    'code\012- data')

file_dev_sda1 = (
    'Linux rev 1.0 ext4 filesystem data, '
    'UUID=d48c3dcf-73df-4c6d-864f-1c758469ee41 (extents) (large files) '
    '(huge files)\012- data')

file_dev_sda3 = (
    'Linux rev 1.0 ext4 filesystem data, '
    'UUID=07429a83-8583-49c9-91f1-abe4d78d163f, volume name "g-/" '
    '(needs journal recovery) (extents) (large files) (huge files)\012- data')

file_dev_sda5 = (
    'Linux/i386 swap file (new style), version 1 (4K pages), '
    'size 2499839 pages, no label, '
    'UUID=ad5b5e5e-b999-468f-8ebb-032c7281e2bd\012- data')

file_dev_sda6 = (
    'Linux rev 1.0 ext4 filesystem data, '
    'UUID=d625c5de-a050-49a1-bdba-b02bb5cf726e, volume name "home" '
    '(needs journal recovery) (extents) (large files) (huge files)\012- data')

file_dev_sda7 = (
    'Linux rev 1.0 ext4 filesystem data, '
    'UUID=746baf3e-e06d-4057-b165-be4a49f195e4, volume name "g-/var" '
    '(needs journal recovery) (extents) (large files) (huge files)\012- data')
