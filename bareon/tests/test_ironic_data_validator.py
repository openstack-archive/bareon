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
import difflib

import mock
import unittest2

from bareon.drivers.data import ironic
from bareon import errors
from bareon import objects
from bareon.utils import block_device

SAMPLE_CHUNK_PARTITIONS = [
    {
        "id": {
            "type": "name",
            "value": "sda"
        },
        "name": "sda",
        "volumes": [
            {
                "type": "boot",
                "size": "300"
            },
            {
                "mount": "/boot",
                "size": "200",
                "type": "raid",
                "file_system": "ext2",
                "name": "Boot"
            },
            {
                "type": "lvm_meta_pool",
                "size": "0"
            },
            {
                "size": "19438",
                "type": "pv",
                "lvm_meta_size": "64",
                "vg": "os"
            },
            {
                "size": "45573",
                "type": "pv",
                "lvm_meta_size": "64",
                "vg": "image"
            }
        ],
        "type": "disk",
        "size": "65535"
    },
    {
        "id": {
            "type": "name",
            "value": "sdb"
        },
        "name": "sdb",
        "volumes": [
            {
                "type": "boot",
                "size": "300"
            },
            {
                "mount": "/boot",
                "size": "200",
                "type": "raid",
                "file_system": "ext2",
                "name": "Boot"
            },
            {
                "type": "lvm_meta_pool",
                "size": "64"
            },
            {
                "size": "0",
                "type": "pv",
                "lvm_meta_size": "0",
                "vg": "os"
            },
            {
                "size": "64947",
                "type": "pv",
                "lvm_meta_size": "64",
                "vg": "image"
            }
        ],
        "type": "disk",
        "size": "65535"
    },
    {
        "name": "sdc",
        "volumes": [
            {
                "type": "boot",
                "size": "300"
            },
            {
                "mount": "/boot",
                "size": "200",
                "type": "raid",
                "file_system": "ext2",
                "name": "Boot"
            },
            {
                "type": "lvm_meta_pool",
                "size": "64"
            },
            {
                "size": "0",
                "type": "pv",
                "lvm_meta_size": "0",
                "vg": "os"
            },
            {
                "size": "64947",
                "type": "pv",
                "lvm_meta_size": "64",
                "vg": "image"
            }
        ],
        "type": "disk",
        "id": {
            "type": "path",
            "value": "disk/by-path/pci-0000:00:0d.0-scsi-0:0:0:0"
        },
        "size": "65535"
    },
    {
        "_allocate_size": "min",
        "label": "Base System",
        "min_size": 19374,
        "volumes": [
            {
                "mount": "/",
                "size": "15360",
                "type": "lv",
                "name": "root",
                "file_system": "ext4"
            },
            {
                "mount": "swap",
                "size": "4014",
                "type": "lv",
                "name": "swap",
                "file_system": "swap"
            }
        ],
        "type": "vg",
        "id": "os"
    },
    {
        "_allocate_size": "all",
        "label": "Image Storage",
        "min_size": 5120,
        "volumes": [
            {
                "mount": "/var/lib/glance",
                "size": "175275",
                "type": "lv",
                "name": "glance",
                "file_system": "xfs"
            }
        ],
        "type": "vg",
        "id": "image"
    }
]

PAYLOAD_SAMPLE0 = {
    'partitions': SAMPLE_CHUNK_PARTITIONS,
    'images': []
}


class TestIronicDataValidator(unittest2.TestCase):
    def setUp(self):
        super(TestIronicDataValidator, self).setUp()
        self.payload = copy.deepcopy(PAYLOAD_SAMPLE0)

    def test_no_error(self):
        ironic.Ironic.validate_data(self.payload)

    def test_fail(self):
        self.assertRaises(
            errors.WrongInputDataError, ironic.Ironic.validate_data, [{}])

    def test_required_fields(self):
        for field in ['partitions']:
            payload = copy.deepcopy(self.payload)
            del payload[field]
            self.assertRaises(
                errors.WrongInputDataError, ironic.Ironic.validate_data,
                payload)

    def test_disks_no_disks_fail(self):
        partitions = self.payload['partitions']
        partitions[:-2] = []
        self.assertRaises(
            errors.WrongInputDataError, ironic.Ironic.validate_data,
            self.payload)

    @unittest2.skip(
        'FIXME(dbogun): Invalid test - failed because invalid data '
        'type(expect sting in size field) but not because illegal partition '
        'size')
    def test_disks_16T_root_volume_fail(self):
        partitions = self.payload['partitions']
        partitions[3]['volumes'][0]['size'] = 16777216 + 1
        self.assertRaises(
            errors.WrongInputDataError, ironic.Ironic.validate_data,
            self.payload)

    def test_disks_volume_type_fail(self):
        incorrect_values_for_type = [
            False, True, 0, 1, None, object
        ]
        partitions = self.payload['partitions']
        for value in incorrect_values_for_type:
            partitions[0]['volumes'][1]['type'] = value
            self.assertRaises(
                errors.WrongInputDataError, ironic.Ironic.validate_data,
                self.payload)

    def test_disks_volume_size_fail(self):
        incorrect_values_for_size = [
            False, True, 0, 1, None, object
        ]
        partitions = self.payload['partitions']
        for value in incorrect_values_for_size:
            partitions[0]['volumes'][1]['size'] = value
            self.assertRaises(
                errors.WrongInputDataError, ironic.Ironic.validate_data,
                self.payload)

    def test_disks_device_id_fail(self):
        incorrect_values_for_id = [
            False, True, 0, 1, None, object
        ]
        partitions = self.payload['partitions']
        for value in incorrect_values_for_id:
            partitions[0]['id'] = value
            self.assertRaises(
                errors.WrongInputDataError, ironic.Ironic.validate_data,
                self.payload)

    def test_disks_missed_property_fail(self):
        required = ['id', 'size', 'volumes', 'type']
        for prop in required:
            fake = copy.deepcopy(self.payload)
            partitions = fake['partitions']
            del partitions[0][prop]
            self.assertRaises(
                errors.WrongInputDataError, ironic.Ironic.validate_data, fake)

    def test_validate_missed_volume_property(self):
        required = ['type', 'size', 'vg']
        for prop in required:
            fake = copy.deepcopy(self.payload)
            partitions = fake['partitions']
            del partitions[0]['volumes'][3][prop]
            self.assertRaises(
                errors.WrongInputDataError, ironic.Ironic.validate_data, fake)

    def test_disks_keep_data_flag_type(self):
        partitions = self.payload['partitions']
        partitions[0]['volumes'][1]['keep_data'] = "True"
        self.assertRaises(
            errors.WrongInputDataError, ironic.Ironic.validate_data,
            self.payload)

    @staticmethod
    def _get_disks(payload):
        return payload['partitions']


class TestIronicDataModel(unittest2.TestCase):
    def setUp(self):
        super(TestIronicDataModel, self).setUp()

        self.block_device_list = mock.Mock()
        self.device_info = mock.Mock()
        self.device_finder = mock.Mock()

        for path, m in (
                ('bareon.utils.hardware.'
                 'get_block_data_from_udev', self.block_device_list),
                ('bareon.utils.hardware.'
                 'get_device_info', self.device_info)):
            patch = mock.patch(path, m)
            patch.start()
            self.addCleanup(patch.stop)

    def test_sample0(self):
        self.block_device_list.side_effect = [
            ['/dev/sda', '/dev/sdb', '/dev/sdc'],
            []]
        self.device_info.side_effect = [
            {'uspec': {'DEVNAME': '/dev/sda', 'DEVLINKS': []}},
            {'uspec': {'DEVNAME': '/dev/sdb', 'DEVLINKS': []}},
            {'uspec': {
                'DEVNAME': '/dev/sdc',
                'DEVLINKS': ['/dev/disk/by-path/pci-0000:00:0d.0-'
                             'scsi-0:0:0:0']}}]
        device_finder = block_device.DeviceFinder()
        patch = mock.patch(
            'bareon.utils.block_device.DeviceFinder', self.device_finder)
        patch.start()
        self.addCleanup(patch.stop)

        self.device_finder.return_value = device_finder

        expect_storage_claim = objects.block_device.StorageSubsystem()

        idnr = objects.block_device.DevIdnr('name', 'sda')
        idnr(device_finder)
        sda = objects.block_device.Disk(
            idnr, self._size(65535, 'MiB'), name='sda')
        sda.add(objects.block_device.Partition(
            self._size(24, 'MiB'), guid_code=0xEF02, is_service=True))
        sda.add(objects.block_device.Partition(
            self._size(300, 'MiB'), is_boot=True))
        sda.add(objects.block_device.MDDev(
            self._size(200, 'MiB')))
        sda.add(objects.block_device.LVMpv(
            'os', self._size(19438, 'MiB'),
            lvm_meta_size=self._size(64, 'MiB')))
        sda.add(objects.block_device.LVMpv(
            'image', self._size(45573, 'MiB'),
            lvm_meta_size=self._size(64, 'MiB')))
        expect_storage_claim.add(sda)

        idnr = objects.block_device.DevIdnr('name', 'sdb')
        idnr(device_finder)
        sdb = objects.block_device.Disk(
            idnr, self._size(65535, 'MiB'), name='sdb')
        sdb.add(objects.block_device.Partition(
            self._size(24, 'MiB'), guid_code=0xEF02, is_service=True))
        sdb.add(objects.block_device.Partition(
            self._size(300, 'MiB'), is_boot=True))
        sdb.add(objects.block_device.MDDev(
            self._size(200, 'MiB')))
        sdb.add(objects.block_device.LVMpv(
            'os', self._size(0, 'MiB'), lvm_meta_size=self._size(0, 'MiB')))
        sdb.add(objects.block_device.LVMpv(
            'image', self._size(64947, 'MiB'),
            lvm_meta_size=self._size(64, 'MiB')))
        expect_storage_claim.add(sdb)

        idnr = objects.block_device.DevIdnr(
            'path', 'disk/by-path/pci-0000:00:0d.0-scsi-0:0:0:0')
        idnr(device_finder)
        sdc = objects.block_device.Disk(
            idnr, self._size(65535, 'MiB'), name='sdc')
        sdc.add(objects.block_device.Partition(
            self._size(24, 'MiB'), guid_code=0xEF02, is_service=True))
        sdc.add(objects.block_device.Partition(
            self._size(300, 'MiB'), is_boot=True))
        sdc.add(objects.block_device.MDDev(
            self._size(200, 'MiB')))
        sdc.add(objects.block_device.LVMpv(
            'os', self._size(0, 'MiB'), lvm_meta_size=self._size(0, 'MiB')))
        sdc.add(objects.block_device.LVMpv(
            'image', self._size(64947, 'MiB'),
            lvm_meta_size=self._size(64, 'MiB')))
        expect_storage_claim.add(sdc)

        lvm_os = objects.block_device.LVMvg(
            'os', _allocate_size='min', label='Base System', min_size=19374)
        lvm_os.add(objects.block_device.LVMlv(
            'root', self._size(15360, 'MiB'), mount='/', file_system='ext4'))
        lvm_os.add(objects.block_device.LVMlv(
            'swap', self._size(4014, 'MiB'), mount='swap', file_system='swap'))
        for component in expect_storage_claim.items_by_kind(
                objects.block_device.LVMpv, recursion=True):
            if component.vg_idnr != 'os':
                continue
            lvm_os.add(component)
        expect_storage_claim.add(lvm_os)

        lvm_image = objects.block_device.LVMvg(
            'image',
            _allocate_size='all',
            label='Image Storage',
            min_size=5120)
        lvm_image.add(objects.block_device.LVMlv(
            'glance', self._size(175275, 'MiB'),
            mount='/var/lib/glance', file_system='xfs'))
        for component in expect_storage_claim.items_by_kind(
                objects.block_device.LVMpv, recursion=True):
            if component.vg_idnr != 'image':
                continue
            lvm_image.add(component)
        expect_storage_claim.add(lvm_image)

        md_boot = objects.block_device.MDRaid(
            '/dev/Boot', file_system='ext2', mount='/boot')
        for component in expect_storage_claim.items_by_kind(
                objects.block_device.MDDev, recursion=True):
            md_boot.add(component)
        expect_storage_claim.add(md_boot)

        data_driver = ironic.Ironic(PAYLOAD_SAMPLE0)
        if expect_storage_claim != data_driver.storage_claim:
            diff = difflib.unified_diff(
                repr(data_driver.storage_claim).splitlines(True),
                repr(expect_storage_claim).splitlines(True),
                'actual', 'expect')
            raise AssertionError(
                'Parsed storage claim is not match expected value:\n'
                '{}'.format(''.join(diff)))

    @staticmethod
    def _size(value, unit):
        value = block_device.SizeUnit(value, unit)
        return block_device.SpaceClaim.new_by_sizeunit(value)
