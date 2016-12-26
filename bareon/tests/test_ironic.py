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


class _IronicTest(unittest2.TestCase):
    _dummy_deployment_config = {
        'partitions': []
    }

    def setUp(self):
        super(_IronicTest, self).setUp()
        with mock.patch.object(ironic.Ironic, 'validate_data'),\
                mock.patch('bareon.objects.ironic.block_device.'
                           'StorageSubsystem'):
            self.data_driver = ironic.Ironic(self._dummy_deployment_config)


@mock.patch('bareon.drivers.data.ironic.Ironic.validate_data', mock.Mock())
@mock.patch(
    'bareon.drivers.data.ironic.Ironic._get_image_scheme', mock.Mock())
class TestGetGrub(unittest2.TestCase):
    @mock.patch('bareon.utils.utils.parse_kernel_cmdline')
    def test_kernel_params(self, cmdline_mock):
        data = {'deploy_data': {'kernel_params': "test_param=test_val",
                                'other_data': 'test'},
                'partitions': {}}
        cmdline_mock.return_value = {
            "BOOTIF": "01-52-54-00-a5-55-58",
            "extrastuff": "test123"
        }

        with mock.patch('bareon.drivers.data.ironic.StorageParser'):
            data_driver = ironic.Ironic(data)

        self.assertEqual('test_param=test_val BOOTIF=01-52-54-00-a5-55-58',
                         data_driver.grub.kernel_params)

    @mock.patch('bareon.utils.utils.parse_kernel_cmdline')
    def test_no_kernel_params(self, cmdline_mock):
        data = {'deploy_data': {'other_data': "test"},
                'partitions': {}}
        cmdline_mock.return_value = {}

        with mock.patch('bareon.drivers.data.ironic.StorageParser'):
            data_driver = ironic.Ironic(data)

        self.assertEqual('', data_driver.grub.kernel_params)


@mock.patch('bareon.drivers.data.ironic.Ironic.validate_data', mock.Mock())
@mock.patch(
    'bareon.drivers.data.ironic.Ironic._get_image_scheme', mock.Mock())
class TestPartitionsPolicy(unittest2.TestCase):
    def test_partitions_policy(self):
        data = {'partitions_policy': "test_value",
                'partitions': {}}

        with mock.patch('bareon.drivers.data.ironic.StorageParser'):
            data_driver = ironic.Ironic(data)

        self.assertEqual('test_value', data_driver.partitions_policy)

    def test_partitions_policy_default(self):
        data = {'partitions': {}}

        with mock.patch('bareon.drivers.data.ironic.StorageParser'):
            data_driver = ironic.Ironic(data)

        self.assertEqual('verify', data_driver.partitions_policy)
