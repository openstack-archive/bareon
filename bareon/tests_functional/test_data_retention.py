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

import copy
import json
import os
import tempfile

from bareon import tests_functional
from bareon.tests_functional import utils


class DataRetentionTestCase(tests_functional.TestCase):
    def setUp(self):
        super(DataRetentionTestCase, self).setUp()
        self.images = [
            {
                "name": "test",
                "boot": True,
                "target": "/",
                "image_pull_url": "",
            }
        ]
        self.golden_image_schema = [
            {
                "type": "disk",
                "id": {
                    "type": "name",
                    "value": "vda"
                },
                "size": "10000 MiB",
                "volumes": [
                    {
                        "images": [
                            "test"
                        ],
                        "type": "partition",
                        "mount": "/",
                        "file_system": "ext4",
                        "size": "4000 MiB",
                        "name": "test1"
                    },
                    {
                        "images": [
                            "test"
                        ],
                        "type": "partition",
                        "mount": "swap",
                        "file_system": "swap",
                        "size": "2000 MiB",
                        "name": "swap"
                    },
                    {
                        "images": [
                            "test"
                        ],
                        "type": "partition",
                        "mount": "/usr",
                        "file_system": "ext4",
                        "size": "3900 MiB",
                        "name": "test2"
                    }
                ]
            }
        ]

        self.golden_image_parted_output = """
Model: Virtio Block Device (virtblk)
Disk /dev/vda: 11.8GB
Sector size (logical/physical): 512B/512B
Partition Table: gpt
Disk Flags:

Number  Start   End     Size    File system     Name     Flags
 1      1049kB  26.2MB  25.2MB                  primary  bios_grub
 2      26.2MB  4221MB  4194MB  ext4            primary
 3      4221MB  6318MB  2097MB  linux-swap(v1)  primary
 4      6318MB  10.4GB  4089MB  ext4            primary


Model: Virtio Block Device (virtblk)
Disk /dev/vdb: 106MB
Sector size (logical/physical): 512B/512B
Partition Table: loop
Disk Flags:

Number  Start  End    Size   File system  Flags
 1      0.00B  106MB  106MB  ext4
"""

    def _assert_vda_equal_to_goldenimage(self, node):
        self._assert_vda_root_equal_to_goldenimage(node)
        self._assert_vda_usr_equal_to_goldenimage(node)

    def _assert_vda_root_equal_to_goldenimage(self, node):
        # Roughly checking that vda / partition not changed
        actual_vda = node.read_file("/dev/vda2", "etc/centos-release")
        expected_vda = "CentOS Linux release 7.1.1503 (Core)"
        utils.assertNoDiff(expected_vda, actual_vda)

    def _assert_vda_usr_equal_to_goldenimage(self, node):
        # Roughly checking that vda /usr partition not changed
        actual_vda = node.read_file("/dev/vda4", "share/centos-release/EULA")
        expected_vda = """
CentOS-7 EULA

CentOS-7 comes with no guarantees or warranties of any sorts,
either written or implied.

The Distribution is released as GPLv2. Individual packages in the
distribution come with their own licences. A copy of the GPLv2 license
is included with the distribution media.
"""
        utils.assertNoDiff(expected_vda, actual_vda)

    def _assert_vdb_equal_to_goldenimage(self, node):
        # Checking that vdb golden image contents are not erased
        actual_vdb = node.read_file("/dev/vdb", "test-content")
        expected_vdb = "test content"
        utils.assertNoDiff(expected_vdb, actual_vdb)

    def test_verify_policy_match(self):
        deploy_conf = {
            "partitions": self.golden_image_schema,
            "partitions_policy": "verify"
        }
        self.env.patch_config_images(deploy_conf, 'test')
        self.env.setup(node_template="data_retention.xml",
                       deploy_config=deploy_conf)
        node = self.env.node

        node.run_cmd('bareon-partition --data_driver ironic '
                     '--deploy_driver swift --debug',
                     check_ret_code=True,
                     get_bareon_log=True)

        # Check that schema did not change after partitioning with
        # verify policy
        # Check that extra disk (vdb) has not been verified/changed since not
        # mentioned in schema (0 return code)
        actual = node.run_cmd('parted -l')[0]
        expected = self.golden_image_parted_output
        utils.assertNoDiff(expected, actual)

        self._assert_vda_equal_to_goldenimage(node)
        self._assert_vdb_equal_to_goldenimage(node)

    def test_verify_policy_match_with_unlabelled_disk(self):
        deploy_conf = {
            "partitions": self.golden_image_schema,
            "partitions_policy": "verify"
        }

        self.env.patch_config_images(deploy_conf, 'test')
        self.env.init_unlabelled_disk()
        self.env.setup('data_retention_with_unlabelled_disk.xml', deploy_conf)
        node = self.env.node

        node.run_cmd(
            'bareon-partition --debug '
            '--data_driver ironic --deploy_driver swift',
            check_ret_code=True, get_bareon_log=True)

        # Check that schema did not change after partitioning with verify
        # policy. Check that extra disk (vdb) has not been verified/changed
        # since not mentioned in schema (0 return code)
        actual = node.run_cmd('parted -l')[0]
        expected = """
Model: Virtio Block Device (virtblk)
Disk /dev/vda: 11.8GB
Sector size (logical/physical): 512B/512B
Partition Table: gpt
Disk Flags:

Number  Start   End     Size    File system     Name     Flags
 1      1049kB  26.2MB  25.2MB                  primary  bios_grub
 2      26.2MB  4221MB  4194MB  ext4            primary
 3      4221MB  6318MB  2097MB  linux-swap(v1)  primary
 4      6318MB  10.4GB  4089MB  ext4            primary


Model: Virtio Block Device (virtblk)
Disk /dev/vdb: 106MB
Sector size (logical/physical): 512B/512B
Partition Table: loop
Disk Flags:

Number  Start  End    Size   File system  Flags
 1      0.00B  106MB  106MB  ext4


Model: Virtio Block Device (virtblk)
Disk /dev/vdc: 1049kB
Sector size (logical/physical): 512B/512B
Partition Table: unknown
Disk Flags:
"""  # noqa
        utils.assertNoDiff(expected, actual)

        self._assert_vda_equal_to_goldenimage(node)
        self._assert_vdb_equal_to_goldenimage(node)

    def test_verify_policy_match_blank_primary(self):
        # Deploy an image to /dev/vdb, with a second disk, not mentioned in
        # the deploy schema, containing a blank primary partition located
        # at /dev/vda.
        deploy_conf = {
            "partitions": self.golden_image_schema,
            "partitions_policy": "verify"
        }
        deploy_conf['partitions'][0]['id']['value'] = 'vdb'

        self.env.patch_config_images(deploy_conf, 'test')
        self.env.setup('data_retention_blank_primary.xml', deploy_conf)
        node = self.env.node

        node.run_cmd(
            'bareon-partition --debug '
            '--data_driver ironic --deploy_driver swift',
            check_ret_code=True, get_bareon_log=True)

        # Check that schema did not change after partitioning with
        # verify policy
        actual = node.run_cmd('parted -l')[0]
        expected = """
Model: Virtio Block Device (virtblk)
Disk /dev/vda: 5243kB
Sector size (logical/physical): 512B/512B
Partition Table: gpt
Disk Flags:

Number  Start   End     Size    File system  Name     Flags
 1      17.4kB  5226kB  5209kB  ext4         primary


Model: Virtio Block Device (virtblk)
Disk /dev/vdb: 11.8GB
Sector size (logical/physical): 512B/512B
Partition Table: gpt
Disk Flags:

Number  Start   End     Size    File system     Name     Flags
 1      1049kB  26.2MB  25.2MB                  primary  bios_grub
 2      26.2MB  4221MB  4194MB  ext4            primary
 3      4221MB  6318MB  2097MB  linux-swap(v1)  primary
 4      6318MB  10.4GB  4089MB  ext4            primary
"""
        utils.assertNoDiff(expected, actual)

    def test_verify_policy_mismatch_extra_partition_in_schema(self):
        deploy_conf = {
            "partitions": copy.deepcopy(self.golden_image_schema),
            "partitions_policy": "verify"
        }
        deploy_conf['partitions'][0]['volumes'].append({
            "images": ["test"],
            "mount": "/tmp",
            "type": "partition",
            "file_system": "ext3",
            "size": "2000"
        })

        self.env.patch_config_images(deploy_conf, 'test')
        self.env.setup(node_template="data_retention.xml",
                       deploy_config=deploy_conf)
        node = self.env.node

        out, ret_code = node.run_cmd(
            'bareon-partition --data_driver ironic '
            '--deploy_driver swift --debug',
            get_bareon_log=True)
        self.assertEqual(255, ret_code)

        # Check that schema did not change after partitioning with
        # verify policy
        actual = node.run_cmd('parted -l')[0]
        expected = self.golden_image_parted_output
        utils.assertNoDiff(expected, actual)

        self._assert_vda_equal_to_goldenimage(node)
        self._assert_vdb_equal_to_goldenimage(node)

    def test_verify_policy_mismatch_extra_partition_on_hw(self):
        deploy_conf = {
            "partitions": copy.deepcopy(self.golden_image_schema),
            "partitions_policy": "verify"
        }
        deploy_conf['partitions'][0]['volumes'].pop()

        self.env.patch_config_images(deploy_conf, 'test')
        self.env.setup(node_template="data_retention.xml",
                       deploy_config=deploy_conf)
        node = self.env.node

        out, ret_code = node.run_cmd(
            'bareon-partition --data_driver ironic '
            '--deploy_driver swift --debug',
            get_bareon_log=True)
        self.assertEqual(255, ret_code)

        # Check that schema did not change after partitioning with
        # verify policy
        actual = node.run_cmd('parted -l')[0]
        expected = self.golden_image_parted_output
        utils.assertNoDiff(expected, actual)

        self._assert_vda_equal_to_goldenimage(node)
        self._assert_vdb_equal_to_goldenimage(node)

    def test_verify_policy_match_and_clean_one_of_filesystems(self):
        deploy_conf = {
            "partitions": copy.deepcopy(self.golden_image_schema),
            "partitions_policy": "verify"
        }
        usr_partition = deploy_conf['partitions'][0]['volumes'][2]
        usr_partition['keep_data'] = False
        self.assertEqual('/usr', usr_partition['mount'])

        self.env.patch_config_images(deploy_conf, 'test')
        self.env.setup(node_template="data_retention.xml",
                       deploy_config=deploy_conf)
        node = self.env.node

        node.run_cmd('bareon-partition --data_driver ironic '
                     '--deploy_driver swift --debug',
                     get_bareon_log=True)

        # Check that schema did not change after partitioning with
        # verify policy
        actual = node.run_cmd('parted -l')[0]
        expected = self.golden_image_parted_output
        utils.assertNoDiff(expected, actual)

        self._assert_vda_root_equal_to_goldenimage(node)
        # Check vda /usr has been erased
        out, ret_code = node.run_cmd('mount -t ext4 /dev/vda4 /mnt && '
                                     'ls /mnt && '
                                     'umount /mnt')
        utils.assertNoDiff("lost+found", out)

        self._assert_vdb_equal_to_goldenimage(node)

    def test_verify_policy_preserve_fstab(self):
        image = 'centos-7.1.1503.fpa_func_test.raw'

        deploy_conf = {
            'partitions_policy': 'clean',
            'partitions': [
                {
                    "type": "disk",
                    "id": {
                        "type": "name",
                        "value": "vda"
                    },
                    "size": "10000 MiB",
                    "volumes": [
                        {
                            "images": [image],
                            "type": "partition",
                            "mount": "/",
                            "file_system": "ext4",
                            "size": "4000 MiB",
                            "name": "test1"
                        }
                    ]
                }
            ]
        }

        self.env.patch_config_images(deploy_conf, 'test')
        self.env.patch_config_images(deploy_conf, image)
        self.env.setup('data_retention.xml', deploy_conf)
        node = self.env.node

        node.run_cmd(
            'bareon-provision --debug '
            '--data_driver ironic --deploy_driver swift',
            check_ret_code=True, get_bareon_log=True)

        extra_record = 'ftest-tmp /var/run tmpfs nodev,nosuid,noexec 0 0\n'
        prefix = '/tmp/target'
        fstab = os.path.join(prefix, 'etc/fstab')

        node.run_cmd('mount /dev/vda2 {}'.format(prefix), check_ret_code=True)
        with tempfile.NamedTemporaryFile() as tmp:
            node.get_file(fstab, tmp.name)

            tmp.seek(0, os.SEEK_END)
            tmp.write('\n')
            tmp.write(extra_record)
            tmp.flush()

            node.put_file(tmp.name, fstab)

        with tempfile.NamedTemporaryFile() as tmp:
            node.get_file('/tmp/provision.json', tmp.name)
            tmp.seek(0, os.SEEK_SET)
            deploy_conf = json.load(tmp)
            deploy_conf['partitions_policy'] = 'verify'

            tmp.seek(0, os.SEEK_SET)
            tmp.truncate()
            json.dump(deploy_conf, tmp)
            tmp.flush()

            node.put_file(tmp.name, '/tmp/provision.json')

        node.run_cmd('umount {}'.format(prefix), check_ret_code=True)

        node.run_cmd(
            'bareon-provision --debug '
            '--data_driver ironic --deploy_driver swift',
            check_ret_code=True, get_bareon_log=True)

        node.run_cmd('mount /dev/vda2 {}'.format(prefix), check_ret_code=True)
        with tempfile.NamedTemporaryFile() as tmp:
            node.get_file(fstab, tmp.name)

            tmp.seek(0, os.SEEK_SET)
            payload = tmp.readlines()
            self.assertIn(extra_record, payload)

    def test_clean_policy(self):
        deploy_conf = {
            "partitions": [
                {
                    "type": "disk",
                    "id": {
                        "type": "name",
                        "value": "vda"
                    },
                    "size": "10000",
                    "volumes": [
                        {
                            "images": [
                                "test"
                            ],
                            "type": "partition",
                            "mount": "/",
                            "file_system": "ext3",
                            "size": "6000",
                            "name": "test1"
                        }
                    ]
                }
            ],
            "partitions_policy": "clean"
        }

        self.env.patch_config_images(deploy_conf, 'test')
        self.env.setup(node_template="data_retention.xml",
                       deploy_config=deploy_conf)
        node = self.env.node

        node.run_cmd('bareon-partition --data_driver ironic '
                     '--deploy_driver swift --debug',
                     check_ret_code=True,
                     get_bareon_log=True)

        # Check that schema has been applied (to vda only)
        actual = node.run_cmd('parted -l')[0]
        expected = """
Model: Virtio Block Device (virtblk)
Disk /dev/vda: 11.8GB
Sector size (logical/physical): 512B/512B
Partition Table: gpt
Disk Flags:

Number  Start   End     Size    File system  Name     Flags
 1      1049kB  26.2MB  25.2MB               primary  bios_grub
 2      26.2MB  6318MB  6291MB  ext3         primary


Model: Virtio Block Device (virtblk)
Disk /dev/vdb: 106MB
Sector size (logical/physical): 512B/512B
Partition Table: loop
Disk Flags:

Number  Start  End    Size   File system  Flags
 1      0.00B  106MB  106MB  ext4
"""
        utils.assertNoDiff(expected, actual)

        self._assert_vdb_equal_to_goldenimage(node)

    def test_clean_policy_disk_too_small(self):
        # Tries to deploy to a disk which is too small for the schema.
        # The Fuel agent should throw:
        # NotEnoughSpaceError: Partition scheme for: /dev/vdb exceeds the size
        # of the disk. Scheme size is 150 MB, and disk size is 106.303488 MB.
        deploy_conf = {
            "partitions": [
                {
                    "type": "disk",
                    "id": {
                        "type": "name",
                        "value": "vdb"
                    },
                    "size": "150",
                    "volumes": [
                        {
                            "images": [
                                "test"
                            ],
                            "type": "partition",
                            "mount": "/",
                            "file_system": "ext3",
                            "size": "100",
                            "name": "test1"
                        }
                    ]
                }
            ],
            "partitions_policy": "clean"
        }

        self.env.patch_config_images(deploy_conf, 'test')
        self.env.setup(node_template="data_retention.xml",
                       deploy_config=deploy_conf)
        node = self.env.node

        # Return code should be 255 due to the agent throwing an exception
        out, ret_code = node.run_cmd(
            'bareon-partition --data_driver ironic '
            '--deploy_driver swift --debug',
            check_ret_code=False, get_bareon_log=True)
        self.assertEqual(255, ret_code)

        # Nothing should have changed
        self._assert_vdb_equal_to_goldenimage(node)
