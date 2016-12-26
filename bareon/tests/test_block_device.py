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

import unittest2

from bareon import errors
from bareon.tests import utils
from bareon.utils import block_device

sector = 512
KiB = 1024
MiB = KiB * KiB


class TestSizeUnit(unittest2.TestCase):
    def test_all_suffixes(self):
        for value, suffix, expect in (
                (25.5, '%', ('25.5 %', None)),
                (5, 's', ('5 s', 512 * 5)),
                (200, 'B', ('200 B', 200)),
                (2, 'KB', ('2 KB', 2 * 1000)),
                (2, 'MB', ('2 MB', 2 * 1000 ** 2)),
                (2, 'GB', ('2 GB', 2 * 1000 ** 3)),
                (2, 'TB', ('2 TB', 2 * 1000 ** 4)),
                (2, 'PB', ('2 PB', 2 * 1000 ** 5)),
                (2, 'EB', ('2 EB', 2 * 1000 ** 6)),
                (2, 'ZB', ('2 ZB', 2 * 1000 ** 7)),
                (2, 'YB', ('2 YB', 2 * 1000 ** 8)),
                (2, 'KiB', ('2 KiB', 2 * 1024)),
                (2, 'MiB', ('2 MiB', 2 * 1024 ** 2)),
                (2, 'GiB', ('2 GiB', 2 * 1024 ** 3)),
                (2, 'TiB', ('2 TiB', 2 * 1024 ** 4)),
                (2, 'PiB', ('2 PiB', 2 * 1024 ** 5)),
                (2, 'EiB', ('2 EiB', 2 * 1024 ** 6)),
                (2, 'ZiB', ('2 ZiB', 2 * 1024 ** 7)),
                (2, 'YiB', ('2 YiB', 2 * 1024 ** 8))):
            for glue in '', ' ':
                raw = glue.join(str(x) for x in (value, suffix))
                size = block_device.SizeUnit.new_by_string(raw)
                self.assertEqual(expect, (str(size), size.bytes))

    def test_fraction(self):
        size = block_device.SizeUnit.new_by_string('2.5 KiB')
        self.assertEqual(
            ('2.5 KiB', 1024 * 2 + 512),
            (str(size), size.bytes))

    def test_in_unit(self):
        size = block_device.SizeUnit.new_by_string('2.2 YiB')
        for unit, expect in (
                ('B',   2659636803152184399101952),
                ('s',   5194603131156610154496),
                ('KiB', 2597301565578305077248),
                ('MiB', 2536427310135063552),
                ('GiB', 2476979795053773),
                ('TiB', 2418925581107.2),
                ('PiB', 2362232012.8),
                ('EiB', 2306867.2),
                ('ZiB', 2252.8),
                ('YiB', 2.2)):
            other = size.in_unit(unit)
            self.assertEqual(size.bytes, other.bytes)
            self.assertEqual(expect, other.value)

    def test_invalid_value(self):
        self.assertRaises(
            ValueError, block_device.SizeUnit.new_by_string, 'invalid')

    def test_invalid_value_with_defaults(self):
        self.assertRaises(
            ValueError, block_device.SizeUnit.new_by_string,
            'invalid', default_unit='KiB')

    def test_invalid_suffix(self):
        self.assertRaises(
            ValueError, block_device.SizeUnit.new_by_string, '2 unknown')

    def test_invalid_suffix_with_default(self):
        self.assertRaises(
            ValueError, block_device.SizeUnit.new_by_string,
            '2 unknown', default_unit='KiB')

    def test_default_unit(self):
        size = block_device.SizeUnit.new_by_string('4', default_unit='KiB')
        self.assertEqual(
            ('4 KiB', 4 * 1024),
            (str(size), size.bytes))

    def test_ignore_default_unit(self):
        size = block_device.SizeUnit.new_by_string('4 B', default_unit='KiB')
        self.assertEqual(
            ('4 B', 4),
            (str(size), size.bytes))

    def test_invalid_default_unit(self):
        self.assertRaises(
            ValueError,
            block_device.SizeUnit.new_by_string, '4', default_unit='unknown')


class TestBlockDevice(unittest2.TestCase):
    def test_disk_scan(self):
        expect = {
            'sample0': [
                (1, 2048, 51199, 0xEF02,
                 'D1950C77-BD81-405B-99AF-997CCCF42C3A'),
                (2, 51200, 8243199, 0x0700,
                 'FFBAB7FC-7E92-441B-9E0B-1E2BDCE2DF6F'),
                (3, 8243200, 12339199, 0x0700,
                 'C45EBDFB-5C67-4035-A00C-624A5AD775B1'),
                (4, 12339200, 20326399, 0x0700,
                 'E3147173-AD70-443E-8D07-9203C89CA0CC')],
            'sample1': [
                (1, 2048, 49999871, 0x8300,
                 '70D0A7D8-FA3B-4FF0-922D-1DFDBF1072F2'),
                (3, 49999872, 99999743, 0x8300,
                 '6B4A0679-831E-44C9-8500-90905960F797'),
                (5, 100001792, 120000511, 0x8200,
                 '04003F45-3426-47EA-BAA0-0220F2CC6B6C'),
                (6, 120002560, 1936746495, 0x8300,
                 'BC2B584B-4A02-47A3-ACA5-9764F6CC5A40'),
                (7, 1936748544, 1953523711, 0x8300,
                 '28404402-736E-46D3-B623-F6F2868079B8')]}

        for sample, target in (
                ('sample0', '/dev/vda'),
                ('sample1', '/dev/sda')):
            with utils.BlockDeviceMock(sample):
                disk = block_device.Disk.new_by_device_scan(target)

            actual = [
                (p.index, p.begin, p.end, p.code, p.guid)
                for p in disk.partitions]

            self.assertEqual(expect[sample], actual)

    def test_allocate(self):
        with utils.BlockDeviceMock('empty-1024MiB'):
            # disk use alignment 2048 sectors
            disk = block_device.Disk.new_by_device_scan('/dev/loop0')
            partitions = [
                disk.allocate(size) for size in (
                    (2048 - 512) * sector,
                    2048 * 8 * sector,
                    2048 * 8 * sector)]

        actual = [
            (p.begin, p.end, p.size, p.index, p.code)
            for p in partitions]

        expect = [
            (2048, 3583, 1536, None, None),
            (4096, 20479, 16384, None, None),
            (20480, 36863, 16384, None, None)]

        self.assertEqual(expect, actual)

    def test_allocate_no_space_left(self):
        with utils.BlockDeviceMock('empty-1024MiB'):
            disk = block_device.Disk.new_by_device_scan('/dev/loop0')

            self.assertRaises(
                errors.BlockDeviceAllocationError, disk.allocate, 1024 * MiB)
