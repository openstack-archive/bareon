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

import mock
import unittest2

from bareon.drivers.data import ironic

PROVISION_SAMPLE_DATA_SWIFT = {
    "partitions": [
        {
            "id": {
                "type": "name",
                "value": "sda"
            },
            "volumes": [
                {
                    "mount": "/boot",
                    "size": "200",
                    "type": "raid",
                    "file_system": "ext2",
                    "name": "Boot"
                },
                {
                    "mount": "/tmp",
                    "size": "200",
                    "type": "partition",
                    "file_system": "ext2",
                    "partition_guid": "fake_guid",
                    "name": "TMP"
                },
                {
                    "size": "19438",
                    "type": "pv",
                    "lvm_meta_size": "64",
                    "vg": "os"
                },
                {
                    "size": "45597",
                    "type": "pv",
                    "lvm_meta_size": "64",
                    "vg": "image"
                }
            ],
            "type": "disk",
            "size": "65587"
        },
        {
            "id": {
                "type": "scsi",
                "value": "1:0:0:0"
            },
            "volumes": [
                {
                    "mount": "/boot",
                    "size": "200",
                    "type": "raid",
                    "file_system": "ext2",
                    "name": "Boot"
                },
                {
                    "size": "0",
                    "type": "pv",
                    "lvm_meta_size": "10",
                    "vg": "os"
                },
                {
                    "size": "64971",
                    "type": "pv",
                    "lvm_meta_size": "64",
                    "vg": "image"
                }
            ],
            "type": "disk",
            "size": "65587"
        },
        {
            'id': {
                'type': 'path',
                'value': 'disk/by-path/pci-0000:00:0d.0-scsi-0:0:0:0'
            },
            "volumes": [
                {
                    "mount": "/boot",
                    "size": "200",
                    "type": "raid",
                    "file_system": "ext2",
                    "name": "Boot"
                },
                {
                    "size": "19374",
                    "type": "pv",
                    "lvm_meta_size": "10",
                    "vg": "os"
                },
                {
                    "size": "175347",
                    "type": "pv",
                    "lvm_meta_size": "64",
                    "vg": "image"
                }
            ],
            "type": "disk",
            "size": "195019"
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
                    "size": "175347",
                    "type": "lv",
                    "name": "glance",
                    "file_system": "xfs"
                }
            ],
            "type": "vg",
            "id": "image"
        },
        {
            "id": {
                "type": "name",
                "value": "sdd"
            },
            "volumes": [
                {
                    "mount": "/var",
                    "size": "0",
                    "type": "raid",
                    "file_system": "ext2",
                    "name": "Boot"
                },
                {
                    "mount": "/tmp",
                    "size": "0",
                    "type": "partition",
                    "file_system": "ext2",
                    "partition_guid": "fake_guid",
                    "name": "TMP"
                }
            ],
            "type": "disk",
            "size": "65587"
        }
    ]
}

PROVISION_SAMPLE_DATA_RSYNC = {
    'ks_meta': {
        'rsync_root_path': "10.10.10.1::testroot/path",
        "pm_data": {
            "kernel_params": "console=ttyS0,9600 console=tty0 rootdelay=90 "
                             "nomodeset", },
        'profile': ''
    }
}

LIST_BLOCK_DEVICES_SAMPLE = [
    {'scsi': '0:0:0:0',
     'name': 'sda',
     'device': '/dev/sda',
     'path': [
         '/dev/disk/by-id/ata-VBOX_HARDDISK',
         '/dev/disk/by-id/scsi-SATA_VBOX_HARDDISK',
         '/dev/disk/by-id/wwn-fake_wwn_1']},
    {'scsi': '1:0:0:0',
     'name': 'sdb',
     'device': '/dev/sdb',
     'path': [
         '/dev/disk/by-id/ata-VBOX_HARDDISK_VBf2923215-708af674',
         '/dev/disk/by-id/scsi-SATA_VBOX_HARDDISK_VBf2923215-708af674',
         '/dev/disk/by-id/wwn-fake_wwn_2']},
    {'scsi': '2:0:0:0',
     'name': 'sdc',
     'device': '/dev/sdc',
     'path': [
         '/dev/disk/by-id/ata-VBOX_HARDDISK_VB50ee61eb-84e74fdf',
         '/dev/disk/by-id/scsi-SATA_VBOX_HARDDISK_VB50ee61eb-84e74fdf',
         '/dev/disk/by-id/wwn-fake_wwn_3',
         '/dev/disk/by-path/pci-0000:00:0d.0-scsi-0:0:0:0']},
    {'scsi': '3:0:0:0',
     'name': 'sdd',
     'device': '/dev/sdd',
     'path': [
         '/dev/disk/by-id/ata-VBOX_HARDDISK_VB50ee61eb-84fdf',
         '/dev/disk/by-id/scsi-SATA_VBOX_HARDDISK_VB50e74fdf',
         '/dev/disk/by-id/wwn-fake_wwn_3',
         '/dev/disk/by-path/pci-0000:00:0d.0-scsi3:0:0:0']},
]


class TestIronicMatch(unittest2.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestIronicMatch, self).__init__(*args, **kwargs)
        self.data_driver = ironic.Ironic('')

    def test_match_device_by_scsi_matches(self):
        # matches by scsi address
        fake_ks_disk = {
            'id': {
                'type': 'scsi',
                'value': '0:0:0:1'
            }
        }
        fake_hu_disk = {
            'scsi': '0:0:0:1'
        }
        self.assertTrue(
            self.data_driver._match_device(fake_hu_disk, fake_ks_disk))

    def test_match_device_by_scsi_not_matches(self):
        # matches by scsi address
        fake_ks_disk = {
            'id': {
                'type': 'scsi',
                'value': '0:0:0:1'
            }
        }
        fake_hu_disk = {
            'scsi': '5:0:0:1'
        }
        self.assertFalse(
            self.data_driver._match_device(fake_hu_disk, fake_ks_disk))

    def test_match_device_by_path_matches(self):
        fake_ks_disk = {
            'id': {
                'type': 'path',
                'value': 'disk/by-path/pci-0000:00:07.0-virtio-pci-virtio3'
            }
        }
        fake_hu_disk = {
            'path': [
                "/dev/disk/by-path/pci-0000:00:07.0-virtio-pci-virtio3",
                "/dev/disk/by-path/fake_path",
                "/dev/sdd"
            ]
        }
        self.assertTrue(
            self.data_driver._match_device(fake_hu_disk, fake_ks_disk))

    def test_match_device_by_path_not_matches(self):
        fake_ks_disk = {
            'id': {
                'type': 'path',
                'value': 'disk/by-path/pci-0000:00:07.0-virtio-pci-virtio3'
            }
        }
        fake_hu_disk = {
            'path': [
                "/dev/disk/by-path/fake_path",
                "/dev/sdd"
            ]
        }
        self.assertFalse(
            self.data_driver._match_device(fake_hu_disk, fake_ks_disk))

    def test_match_device_by_name_matches(self):
        fake_ks_disk = {
            'id': {
                'type': 'name',
                'value': 'sda'
            }
        }
        fake_hu_disk = {
            'name': '/dev/sda'
        }
        self.assertTrue(
            self.data_driver._match_device(fake_hu_disk, fake_ks_disk))

    def test_match_device_by_name_not_matches(self):
        fake_ks_disk = {
            'id': {
                'type': 'name',
                'value': 'sda'
            }
        }
        fake_hu_disk = {
            'name': '/dev/sdd'
        }
        self.assertFalse(
            self.data_driver._match_device(fake_hu_disk, fake_ks_disk))


@mock.patch('bareon.drivers.data.ironic.hu.scsi_address')
class TestNailgunMockedMeta(unittest2.TestCase):
    def test_partition_scheme(self, mock_scsi_address):
        data_driver = ironic.Ironic(PROVISION_SAMPLE_DATA_SWIFT)

        data_driver.get_image_ids = mock.MagicMock
        mock_devices = data_driver._get_block_devices = mock.MagicMock()
        mock_devices.return_value = LIST_BLOCK_DEVICES_SAMPLE

        p_scheme = data_driver.partition_scheme
        self.assertEqual(5, len(p_scheme.fss))
        self.assertEqual(5, len(p_scheme.pvs))
        self.assertEqual(3, len(p_scheme.lvs))
        self.assertEqual(2, len(p_scheme.vgs))
        self.assertEqual(3, len(p_scheme.parteds))


@mock.patch('bareon.drivers.data.ironic.hu.get_block_devices_from_udev_db')
class TestGetBlockDevices(unittest2.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestGetBlockDevices, self).__init__(*args, **kwargs)
        self.driver = ironic.Ironic('')
        self.mock_devices = mock.MagicMock()
        self.driver._get_block_device_info = self.mock_devices

    def test_no_devices(self, mock_get_block_devices_from_udev_db):
        mock_get_block_devices_from_udev_db.return_value = []

        result = self.driver._get_block_devices()
        self.assertEqual(result, [])
        mock_get_block_devices_from_udev_db.assert_called_once_with()
        self.assertEqual(self.mock_devices.call_count, 0)

    def test_device_info(self, mock_get_block_devices_from_udev_db):
        data = {'test': 'fake'}

        mock_get_block_devices_from_udev_db.return_value = [data]
        self.mock_devices.return_value = block_device = 'test_value'
        result = self.driver._get_block_devices()
        self.assertEqual(result, [block_device])
        mock_get_block_devices_from_udev_db.assert_called_once_with()
        self.mock_devices.assert_called_once_with(data)


@mock.patch('bareon.drivers.data.ironic.hu.get_device_ids')
@mock.patch('bareon.drivers.data.ironic.hu.get_device_info')
@mock.patch('bareon.drivers.data.ironic.hu.scsi_address')
class TestGetBlockDevice(unittest2.TestCase):
    def test_no_device_info(self, mock_scsi_address, mock_get_device_info,
                            mock_get_device_ids):
        data_driver = ironic.Ironic('')
        device = 'fake_device'

        mock_scsi_address.return_value = None
        mock_get_device_info.return_value = {}
        mock_get_device_ids.return_value = []

        result = data_driver._get_block_device_info(device)

        self.assertEqual(result, {'name': 'fake_device'})

    def test_device_info(self, mock_scsi_address, mock_get_device_info,
                         mock_get_device_ids):
        data_driver = ironic.Ironic('')
        device = 'fake_device'
        devpath = ['test/devpath']
        uspec = {'DEVPATH': devpath}

        mock_get_device_info.return_value = {
            'uspec': uspec
        }
        mock_scsi_address.return_value = scsi_address = '1:0:0:0'
        mock_get_device_ids.return_value = devpath

        desired = {'path': devpath, 'name': device, 'scsi': scsi_address,
                   'uspec': uspec}

        result = data_driver._get_block_device_info(device)
        self.assertEqual(result, desired)
        mock_get_device_info.assert_called_once_with(device)
        mock_scsi_address.assert_called_once_with(device)


class TestGetGrub(unittest2.TestCase):

    @mock.patch('bareon.utils.utils.parse_kernel_cmdline')
    def test_kernel_params(self, cmdline_mock):
        data = {'deploy_data': {'kernel_params': "test_param=test_val",
                                'other_data': 'test'},
                'partitions': 'fake_shema'}
        cmdline_mock.return_value = {
            "BOOTIF": "01-52-54-00-a5-55-58",
            "extrastuff": "test123"
        }

        data_driver = ironic.Ironic(data)

        self.assertEqual('test_param=test_val BOOTIF=01-52-54-00-a5-55-58',
                         data_driver.grub.kernel_params)

    def test_no_kernel_params(self):
        data = {'deploy_data': {'other_data': "test"},
                'partitions': 'fake_shema'}
        data_driver = ironic.Ironic(data)

        self.assertEqual('', data_driver.grub.kernel_params)


class TestPartitionsPolicy(unittest2.TestCase):

    def test_partitions_policy(self):
        data = {'partitions_policy': "test_value",
                'partitions': 'fake_shema'}

        data_driver = ironic.Ironic(data)

        self.assertEqual('test_value', data_driver.partitions_policy)

    def test_partitions_policy_default(self):
        data = {'partitions': 'fake_shema'}

        data_driver = ironic.Ironic(data)

        self.assertEqual('verify', data_driver.partitions_policy)
