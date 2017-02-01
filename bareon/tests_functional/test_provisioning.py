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
import uuid

import os
import pytest

from bareon import tests_functional
from bareon.tests_functional import utils


class SingleProvisioningTestCase(tests_functional.TestCase):
    node_ssh_login = 'centos'

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

        actual = node.run_cmd('parted -l')[0]
        utils.assertNoDiff(expected, actual)

        self._update_cloud_conf(node)

        node.reboot_to_hdd()
        node.ssh_login = self.node_ssh_login
        node.wait_for_boot()

        # Set node.ssh_key to "path to tenant key"
        # (if tenant key is different than deploy key)
        actual = node.run_cmd('uname -a')[0]
        expected = ('Linux fpa-func-test-tenant-vm 3.10.0-229.20.1.el7.x86_64'
                    ' #1 SMP Tue Nov 3 19:10:07 UTC 2015 x86_64 x86_64 x86_64'
                    ' GNU/Linux\n')

        utils.assertNoDiff(expected, actual)

    def test_provision_rsync(self):
        self.env.deploy_driver = 'rsync'

        deploy_conf = {
            "images": [
                {
                    "name": "test",
                    "boot": True,
                    "target": "/",
                    "image_pull_url": self.env.get_url_for_image(
                        'centos-7.1.1503.fpa_func_test.raw',
                        self.env.deploy_driver),
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
                     '--deploy_driver %s' % self.env.deploy_driver,
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

        self._update_cloud_conf(node, part='vda2')

        node.reboot_to_hdd()
        node.ssh_login = self.node_ssh_login
        node.wait_for_boot()

        # Set node.ssh_key to "path to tenant key"
        # (if tenant key is different than deploy key)
        actual = node.run_cmd('uname -a')[0]
        expected = ('Linux fpa-func-test-tenant-vm 3.10.0-229.20.1.el7.x86_64'
                    ' #1 SMP Tue Nov 3 19:10:07 UTC 2015 x86_64 x86_64 x86_64'
                    ' GNU/Linux\n')

        utils.assertNoDiff(expected, actual)

    def test_provision_rsync_disk_by_id(self):
        self.env.deploy_driver = 'rsync'

        deploy_conf = {
            "images": [
                {
                    "name": "test",
                    "boot": True,
                    "target": "/",
                    "image_pull_url": self.env.get_url_for_image(
                        'centos-7.1.1503.fpa_func_test.raw',
                        self.env.deploy_driver),
                }
            ],
            "partitions_policy": "clean",
            "partitions": [
                {
                    "id": {"type": "path",
                           "value": "disk/by-id/"
                                    "ata-QEMU_HARDDISK_SomeSerialNumber"},
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

        self.env.setup(
            'one_disk_persistent.xml', deploy_conf)
        node = self.env.node

        node.run_cmd(
            'bareon-provision --data_driver ironic --deploy_driver {}'.format(
                self.env.deploy_driver),
            check_ret_code=True, get_bareon_log=True)

        actual = node.run_cmd('parted -l')[0]
        expected = """
Model: ATA QEMU HARDDISK (scsi)
Disk /dev/sda: 3221MB
Sector size (logical/physical): 512B/512B
Partition Table: gpt
Disk Flags:

Number  Start   End     Size    File system  Name     Flags
 1      1049kB  26.2MB  25.2MB               primary  bios_grub
 2      26.2MB  1494MB  1468MB  ext4         primary
 3      1494MB  3067MB  1573MB  ext4         primary
"""

        utils.assertNoDiff(expected, actual)

        self._update_cloud_conf(node, part='vda2')

        node.reboot_to_hdd()
        node.ssh_login = self.node_ssh_login
        node.wait_for_boot()

        actual = node.run_cmd('uname -a')[0]
        expected = (
            'Linux fpa-func-test-tenant-vm 3.10.0-229.20.1.el7.x86_64'
            ' #1 SMP Tue Nov 3 19:10:07 UTC 2015 x86_64 x86_64 x86_64'
            ' GNU/Linux\n')

        utils.assertNoDiff(expected, actual)

    @pytest.mark.xfail
    def test_provision_swift_verify(self):
        """Test the behaviour of the verify partitions policy with Swift.

        First deploy a node using the clean policy, then change and create some
        files on the provisioned file system. Finally, redeploy the same node
        using the verify partitions policy. Following the redeployment,
        modified files should be returned to their initial state, and newly
        created files should be removed (due to block-level copy).
        """
        self.env.deploy_driver = 'swift'

        deploy_conf = {
            "images": [
                {
                    "name": "test",
                    "boot": True,
                    "target": "/",
                    "image_pull_url": self.env.get_url_for_image(
                        'centos-7.1.1503.fpa_func_test.raw',
                        self.env.deploy_driver),
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
                            "size": "2900"
                        }
                    ],

                }
            ]
        }
        self.env.setup('one_disk.xml', deploy_conf)
        node = self.env.node

        node.run_cmd(
            'bareon-provision --data_driver ironic --deploy_driver {}'.format(
                self.env.deploy_driver),
            check_ret_code=True, get_bareon_log=True)

        # Modify a file that isalready present on the image.
        node.write_file("/dev/vda2", "etc/centos-release", "bogus release")

        # Write a file that don't exist in the image.
        node.write_file("/dev/vda2", "new-file", "new content")

        # Update the deploy config to use the verify partitions policy.
        deploy_conf["partitions_policy"] = "verify"
        self.env.update_deploy_config(deploy_conf)

        # Deploy again.
        node.run_cmd(
            'bareon-provision --data_driver ironic --deploy_driver {}'.format(
                self.env.deploy_driver),
            check_ret_code=True, get_bareon_log=True)

        # Verify that file present on the image is updated to its original
        # state, and that changes are discarded.
        actual_vda = node.read_file("/dev/vda2", "etc/centos-release")
        expected_vda = "CentOS Linux release 7.1.1503 (Core) \n"
        utils.assertNoDiff(expected_vda, actual_vda)

        # Verify that newly created file not on the image is not modified
        # by the deployment.
        actual_vda = node.read_file("/dev/vda2", "new-file")
        expected_vda = "new content"
        utils.assertNoDiff(expected_vda, actual_vda)

    def test_provision_rsync_verify(self):
        """Test the behaviour of the verify partitions policy using rsync.

        First deploy a node using the clean policy, then change and create some
        files on the provisioned file system. Finally, redeploy the same node
        using the verify partitions policy. Following the redeployment,
        modified files should be returned to their initial state, and newly
        created files should be unchanged.
        """
        self.env.deploy_driver = 'rsync'

        deploy_conf = {
            "images": [
                {
                    "name": "test",
                    "boot": True,
                    "target": "/",
                    "image_pull_url": self.env.get_url_for_image(
                        'centos-7.1.1503.fpa_func_test.raw',
                        self.env.deploy_driver),
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
        self.env.setup('one_disk.xml', deploy_conf)
        node = self.env.node

        node.run_cmd(
            'bareon-provision --data_driver ironic --deploy_driver {}'.format(
                self.env.deploy_driver),
            check_ret_code=True, get_bareon_log=True)

        # Modify some files that are already present on the image in each
        # partition.
        node.write_file("/dev/vda2", "etc/centos-release", "bogus release")
        node.write_file("/dev/vda3", "share/centos-release/EULA",
                        "bogus EULA")

        # Write some files that don't exist in the image on each partition.
        node.write_file("/dev/vda2", "new-file", "new / content")
        node.write_file("/dev/vda3", "new-file", "new /usr content")

        # Update the deploy config to use the verify partitions policy.
        deploy_conf["partitions_policy"] = "verify"
        self.env.update_deploy_config(deploy_conf)

        # Deploy again.
        node.run_cmd(
            'bareon-provision --data_driver ironic --deploy_driver {}'.format(
                self.env.deploy_driver),
            check_ret_code=True, get_bareon_log=True)

        # Verify that files present on the image are updated to their original
        # state, and that changes are discarded.
        actual_vda_root = node.read_file("/dev/vda2", "etc/centos-release")
        expected_vda_root = "CentOS Linux release 7.1.1503 (Core)"
        utils.assertNoDiff(expected_vda_root, actual_vda_root)

        expected_vda_usr = ("""
CentOS-7 EULA

CentOS-7 comes with no guarantees or warranties of any sorts,
either written or implied.

The Distribution is released as GPLv2. Individual packages in the
distribution come with their own licences. A copy of the GPLv2 license
is included with the distribution media.
        """)
        actual_vda_usr = node.read_file("/dev/vda3",
                                        "share/centos-release/EULA")
        utils.assertNoDiff(expected_vda_usr, actual_vda_usr)

        # Verify that newly created files not on the image are not modified
        # by the deployment.
        actual_vda_root = node.read_file("/dev/vda2", "new-file")
        expected_vda_root = "new / content"
        utils.assertNoDiff(expected_vda_root, actual_vda_root)

        actual_vda_usr = node.read_file("/dev/vda3", "new-file")
        expected_vda_usr = "new /usr content"
        utils.assertNoDiff(expected_vda_usr, actual_vda_usr)

    def test_provision_rsync_verify_dry_run(self):
        """Test the behaviour of the verify policy using an rsync dry run.

        First deploy a node using the clean policy, then change and create some
        files on the provisioned file system. Finally, redeploy the same node
        using the verify partitions policy and the --dry-run rsync flag.
        Following the redeployment, modified and newly created files should be
        unchanged.
        """
        self.env.deploy_driver = 'rsync'

        deploy_conf = {
            "images": [
                {
                    "name": "test",
                    "boot": True,
                    "target": "/",
                    "image_pull_url": self.env.get_url_for_image(
                        'centos-7.1.1503.fpa_func_test.raw',
                        self.env.deploy_driver),
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
        self.env.setup('one_disk.xml', deploy_conf)
        node = self.env.node

        node.run_cmd(
            'bareon-provision --data_driver ironic --deploy_driver {}'.format(
                self.env.deploy_driver),
            check_ret_code=True, get_bareon_log=True)

        # Modify some files that are already present on the image in each
        # partition.
        node.write_file("/dev/vda2", "etc/centos-release", "bogus release")
        node.write_file("/dev/vda3", "share/centos-release/EULA",
                        "bogus EULA")

        # Write some files that don't exist in the image on each partition.
        node.write_file("/dev/vda2", "new-file", "new / content")
        node.write_file("/dev/vda3", "new-file", "new /usr content")

        # Update the deploy config to use the verify partitions policy and the
        # --dry-run rsync flag.
        deploy_conf["partitions_policy"] = "verify"
        deploy_conf["image_deploy_flags"] = {
            "rsync_flags": "-a -A -X --timeout 300 --dry-run"
        }
        self.env.update_deploy_config(deploy_conf)

        # Deploy again.
        node.run_cmd(
            'bareon-provision --data_driver ironic --deploy_driver {}'.format(
                self.env.deploy_driver),
            check_ret_code=True, get_bareon_log=True)

        # Verify that files present on the image are not modified by the
        # deployment.
        actual_vda_root = node.read_file("/dev/vda2", "etc/centos-release")
        expected_vda_root = "bogus release"
        utils.assertNoDiff(expected_vda_root, actual_vda_root)
        expected_vda_usr = "bogus EULA"
        actual_vda_usr = node.read_file("/dev/vda3",
                                        "share/centos-release/EULA")
        utils.assertNoDiff(expected_vda_usr, actual_vda_usr)

        # Verify that newly created files not on the image are not modified
        # by the deployment.
        actual_vda_root = node.read_file("/dev/vda2", "new-file")
        expected_vda_root = "new / content"
        utils.assertNoDiff(expected_vda_root, actual_vda_root)
        actual_vda_usr = node.read_file("/dev/vda3", "new-file")
        expected_vda_usr = "new /usr content"
        utils.assertNoDiff(expected_vda_usr, actual_vda_usr)

    def _update_cloud_conf(self, node, part='vda2'):
        # Update the cloud config in the tenant image to contain the
        # correct SSH public key. Normally this would be done from Ironic
        # using deploy actions, or as part of cloud init.
        cloud_cfg_path = os.path.join(node.workdir, "cloud.cfg")
        node.put_file(cloud_cfg_path, '/tmp/cloud.cfg')
        node.run_cmd('mkdir /tmp/{0}'.format(part))
        node.run_cmd('mount -t ext4 /dev/{0} /tmp/{0}'.format(part))
        node.run_cmd('cp -f /tmp/cloud.cfg /tmp/{0}/etc/cloud/cloud.cfg'
                     .format(part))
        node.run_cmd('umount /tmp/{0}'.format(part))


class MultipleProvisioningTestCase(tests_functional.TestCase):
    def test_multiple_provisioning(self):
        self.env.deploy_driver = 'swift'
        image_url = self.env.get_url_for_image(
            "centos-7.1.1503.fpa_func_test.raw", self.env.deploy_driver)

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
                     '--deploy_driver %s' % self.env.deploy_driver,
                     check_ret_code=True,
                     get_bareon_log=True)

        actual = node.run_cmd('cat /tmp/boot_entries.json')[0]

        actual_data = json.loads(actual)
        self.assertEqual(len(actual_data.get('elements')), 2)
        uuid.UUID(actual_data.get('multiboot_partition'))
