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

import json
import utils
import uuid

from bareon import tests_functional


class SingleProvisioningTestCase(tests_functional.TestCase):
    def test_provision_two_disks_swift(self):
        DEPLOY_DRIVER = 'swift'
        deploy_conf = {
            "images": [
                {
                    "name": "test",
                    "boot": True,
                    "target": "/",
                    "image_pull_url": self.env.get_url_for_image(
                        'centos-7.1.1503.fpa_func_test.raw',
                        DEPLOY_DRIVER),
                }
            ],
            "partitions_policy": "clean",
            "partitions": [
                {
                    "id": {"type": "name", "value": "vda"},
                    "size": "4000",
                    "type": "disk",
                    "volumes": [
                        {
                            "mount": "/",
                            "type": "partition",
                            "file_system": "ext4",
                            "size": "3000"
                        }
                    ],

                },
                {
                    "id": {"type": "name", "value": "vdb"},
                    "size": "2000",
                    "type": "disk",
                    "volumes": [
                        {
                            "mount": "/home",
                            "type": "partition",
                            "file_system": "ext3",
                            "size": "1000"
                        }
                    ],

                }
            ]
        }
        self.env.setup(node_template="two_disks.xml",
                       deploy_config=deploy_conf)
        node = self.env.node

        node.run_cmd('bareon-provision --data_driver ironic '
                     '--deploy_driver %s' % DEPLOY_DRIVER,
                     check_ret_code=True,
                     get_bareon_log=True)

        actual = node.run_cmd('parted -l')[0]
        expected = """
Model: Virtio Block Device (virtblk)
Disk /dev/vda: 4295MB
Sector size (logical/physical): 512B/512B
Partition Table: gpt
Disk Flags:

Number  Start   End     Size    File system  Name     Flags
 1      1049kB  26.2MB  25.2MB               primary  bios_grub
 2      26.2MB  3172MB  3146MB  ext4         primary


Model: Virtio Block Device (virtblk)
Disk /dev/vdb: 2147MB
Sector size (logical/physical): 512B/512B
Partition Table: gpt
Disk Flags:

Number  Start   End     Size    File system  Name     Flags
 1      1049kB  26.2MB  25.2MB               primary  bios_grub
 2      26.2MB  1075MB  1049MB  ext3         primary
"""

        utils.assertNoDiff(expected, actual)

        node.reboot_to_hdd()
        node.wait_for_boot()

        # Set node.ssh_key to "path to tenant key"
        # (if tenant key is different than deploy key)
        node.ssh_login = "centos"
        actual = node.run_cmd('uname -a')[0]
        expected = ('Linux fpa-func-test-tenant-vm 3.10.0-229.20.1.el7.x86_64'
                    ' #1 SMP Tue Nov 3 19:10:07 UTC 2015 x86_64 x86_64 x86_64'
                    ' GNU/Linux\n')

        utils.assertNoDiff(expected, actual)

    def test_provision_rsync(self):
        DEPLOY_DRIVER = 'rsync'
        deploy_conf = {
            "images": [
                {
                    "name": "test",
                    "boot": True,
                    "target": "/",
                    "image_pull_url": self.env.get_url_for_image(
                        'centos-7.1.1503.fpa_func_test.raw',
                        DEPLOY_DRIVER),
                }
            ],
            "partitions_policy": "clean",
            "partitions": [
                {
                    "id": {"type": "name", "value": "vda"},
                    "size": "3000",
                    "type": "disk",
                    "volumes": [
                        {
                            "mount": "/",
                            "type": "partition",
                            "file_system": "ext4",
                            "size": "1400"
                        },
                        {
                            "mount": "/usr",
                            "type": "partition",
                            "file_system": "ext4",
                            "size": "1500"
                        }
                    ],

                }
            ]
        }
        self.env.setup(node_template="one_disk.xml",
                       deploy_config=deploy_conf)
        node = self.env.node

        node.run_cmd('bareon-provision --data_driver ironic '
                     '--deploy_driver %s' % DEPLOY_DRIVER,
                     check_ret_code=True,
                     get_bareon_log=True)

        actual = node.run_cmd('parted -l')[0]
        expected = """
Model: Virtio Block Device (virtblk)
Disk /dev/vda: 3221MB
Sector size (logical/physical): 512B/512B
Partition Table: gpt
Disk Flags:

Number  Start   End     Size    File system  Name     Flags
 1      1049kB  26.2MB  25.2MB               primary  bios_grub
 2      26.2MB  1494MB  1468MB  ext4         primary
 3      1494MB  3067MB  1573MB  ext4         primary
"""

        utils.assertNoDiff(expected, actual)

        node.reboot_to_hdd()
        node.wait_for_boot()

        # Set node.ssh_key to "path to tenant key"
        # (if tenant key is different than deploy key)
        node.ssh_login = "centos"
        actual = node.run_cmd('uname -a')[0]
        expected = ('Linux fpa-func-test-tenant-vm 3.10.0-229.20.1.el7.x86_64'
                    ' #1 SMP Tue Nov 3 19:10:07 UTC 2015 x86_64 x86_64 x86_64'
                    ' GNU/Linux\n')

        utils.assertNoDiff(expected, actual)


class MultipleProvisioningTestCase(tests_functional.TestCase):
    def test_multiple_provisioning(self):
        DEPLOY_DRIVER = 'swift'
        image_url = self.env.get_url_for_image(
            "centos-7.1.1503.fpa_func_test.raw", DEPLOY_DRIVER)

        deploy_conf = {
            "images": [
                {
                    "name": "centos",
                    "boot": True,
                    "target": "/",
                    "image_pull_url": image_url,
                },
                {
                    "name": "ubuntu",
                    "boot": False,
                    "target": "/",
                    "image_pull_url": image_url,
                },
            ],
            "partitions_policy": "clean",
            "partitions": [
                {
                    "id": {"type": "name", "value": "vda"},
                    "size": "4000",
                    "type": "disk",
                    "volumes": [
                        {
                            "mount": "/",
                            "images": [
                                "centos"
                            ],
                            "type": "partition",
                            "file_system": "ext4",
                            "size": "3000"
                        }
                    ],

                },
                {
                    "id": {"type": "name", "value": "vdb"},
                    "size": "4000",
                    "type": "disk",
                    "volumes": [
                        {
                            "mount": "/",
                            "images": [
                                "ubuntu"
                            ],
                            "type": "partition",
                            "file_system": "ext4",
                            "size": "3000"
                        }
                    ],

                }
            ]
        }
        self.env.setup(node_template="two_disks_multiboot.xml",
                       deploy_config=deploy_conf)

        node = self.env.node

        node.run_cmd('bareon-provision --data_driver ironic '
                     '--deploy_driver %s' % DEPLOY_DRIVER,
                     check_ret_code=True,
                     get_bareon_log=True)

        actual = node.run_cmd('cat /tmp/boot_entries.json')[0]

        actual_data = json.loads(actual)
        self.assertEqual(len(actual_data.get('elements')), 2)
        uuid.UUID(actual_data.get('multiboot_partition'))
