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

from bareon.tests import utils
from bareon.utils import block_device


class TestBlockDevice(unittest2.TestCase):
    def test_disk_scan(self):
        with utils.BlockDeviceMock('sample1'):
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
