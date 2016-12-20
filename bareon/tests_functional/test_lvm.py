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

import utils

from bareon import tests_functional


class LvmTestCase(tests_functional.TestCase):
    def test_multi_volume_multi_group(self):
        deploy_conf = {
            "images": [
                {
                    "name": "test",
                    "boot": True,
                    "target": "/",
                    "image_pull_url": "",
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
                            "vg": "fpa_test_vg_1",
                            "type": "pv",
                            "size": "2000"
                        },
                        {
                            "vg": "fpa_test_vg_2",
                            "type": "pv",
                            "size": "976"  # 1000 - 24 (GRUB stage 1.5)
                        }
                    ],
                },
                {
                    "id": {"type": "name", "value": "vdb"},
                    "size": "2000",
                    "type": "disk",
                    "volumes": [
                        {
                            "images": [
                                "test"
                            ],
                            "vg": "fpa_test_vg_2",
                            "type": "pv",
                            "size": "1976"  # 2000 - 24 (GRUB stage 1.5)
                        },
                    ],
                },
                {
                    "type": "vg",
                    "id": "fpa_test_vg_1",
                    "volumes": [
                        {
                            "images": [
                                "test"
                            ],
                            "type": "lv",
                            "name": "fpa_root_vol",
                            "mount": "/",
                            "size": "1000",
                            "file_system": "ext4"
                        },
                        {
                            "images": [
                                "test"
                            ],
                            "type": "lv",
                            "name": "fpa_var_vol",
                            "mount": "/var",
                            "size": "936",  # (2000-1000)- 1*64 (lvm meta)
                            "file_system": "ext3"
                        }
                    ]
                },
                {
                    "type": "vg",
                    "id": "fpa_test_vg_2",
                    "volumes": [
                        {
                            "images": [
                                "test"
                            ],
                            "type": "lv",
                            "name": "fpa_usr_vol",
                            "mount": "/usr",
                            "size": "2000",
                            "file_system": "ext4"
                        },
                        {
                            "images": [
                                "test"
                            ],
                            "type": "lv",
                            "name": "fpa_etc_vol",
                            "mount": "/etc",
                            "size": "824",  # (976+1976)-2000-2*64 (lvm meta)
                            "file_system": "ext3"
                        }
                    ]
                }
            ]
        }
        self.env.setup(node_template="two_disks.xml",
                       deploy_config=deploy_conf)
        node = self.env.node

        node.run_cmd('bareon-partition --data_driver ironic '
                     '--deploy_driver swift --debug',
                     check_ret_code=True,
                     get_bareon_log=True)

        actual = node.run_cmd('parted -lm && pvs && lvs')[0]
        expected = """
BYT;
/dev/mapper/fpa_test_vg_2-fpa_etc_vol:864MB:dm:512:512:loop:Linux device-mapper (linear):;
1:0.00B:864MB:864MB:ext3::;

BYT;
/dev/mapper/fpa_test_vg_2-fpa_usr_vol:2097MB:dm:512:512:loop:Linux device-mapper (linear):;
1:0.00B:2097MB:2097MB:ext4::;

BYT;
/dev/mapper/fpa_test_vg_1-fpa_var_vol:981MB:dm:512:512:loop:Linux device-mapper (linear):;
1:0.00B:981MB:981MB:ext3::;

BYT;
/dev/mapper/fpa_test_vg_1-fpa_root_vol:1049MB:dm:512:512:loop:Linux device-mapper (linear):;
1:0.00B:1049MB:1049MB:ext4::;

BYT;
/dev/vda:4295MB:virtblk:512:512:gpt:Virtio Block Device:;
1:1049kB:26.2MB:25.2MB::primary:bios_grub;
2:26.2MB:2123MB:2097MB::primary:;
3:2123MB:3147MB:1023MB::primary:;

BYT;
/dev/vdb:2147MB:virtblk:512:512:gpt:Virtio Block Device:;
1:1049kB:26.2MB:25.2MB::primary:bios_grub;
2:26.2MB:2098MB:2072MB::primary:;

  PV         VG            Fmt  Attr PSize   PFree
  /dev/vda2  fpa_test_vg_1 lvm2 a--    1.89g 4.00m
  /dev/vda3  fpa_test_vg_2 lvm2 a--  916.00m 8.00m
  /dev/vdb2  fpa_test_vg_2 lvm2 a--    1.87g    0
  LV           VG            Attr       LSize    Pool Origin Data%  Meta%  Move Log Cpy%Sync Convert
  fpa_root_vol fpa_test_vg_1 -wi-a----- 1000.00m
  fpa_var_vol  fpa_test_vg_1 -wi-a-----  936.00m
  fpa_etc_vol  fpa_test_vg_2 -wi-a-----  824.00m
  fpa_usr_vol  fpa_test_vg_2 -wi-a-----    1.95g
"""""  # noqa
        utils.assertNoDiff(expected, actual)

    def test_mixed_partitions_and_lvs(self):
        deploy_conf = {
            "images": [
                {
                    "name": "test",
                    "boot": True,
                    "target": "/",
                    "image_pull_url": "",
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
                            "images": [
                                "test"
                            ],
                            "mount": "/",
                            "type": "partition",
                            "file_system": "ext4",
                            "size": "2476"  # 2500 - 24 (GRUB stage 1.5)
                        },
                        {
                            "vg": "fpa_test_vg_1",
                            "type": "pv",
                            "size": "500"
                        }
                    ],
                },
                {
                    "id": {"type": "name", "value": "vdb"},
                    "size": "2000",
                    "type": "disk",
                    "volumes": [
                        {
                            "vg": "fpa_test_vg_1",
                            "type": "pv",
                            "size": "976"  # 1000 - 24 (GRUB stage 1.5)
                        },
                        {
                            "vg": "fpa_test_vg_2",
                            "type": "pv",
                            "size": "1000"
                        },
                    ],
                },
                {
                    "type": "vg",
                    "id": "fpa_test_vg_1",
                    "volumes": [
                        {
                            "images": [
                                "test"
                            ],
                            "type": "lv",
                            "name": "fpa_usr_vol",
                            "mount": "/usr",
                            "size": "1348",  # (976+500) - 2*64 (lvm meta)
                            "file_system": "ext3"
                        }
                    ]
                },
                {
                    "type": "vg",
                    "id": "fpa_test_vg_2",
                    "volumes": [
                        {
                            "images": [
                                "test"
                            ],
                            "type": "lv",
                            "name": "fpa_opt_vol",
                            "mount": "/opt",
                            "size": "936",  # 1000 - 1*64 (lvm meta)
                            "file_system": "ext4"
                        }
                    ]
                }
            ]
        }
        self.env.setup(node_template="two_disks.xml",
                       deploy_config=deploy_conf)
        node = self.env.node

        node.run_cmd('bareon-partition --data_driver ironic '
                     '--deploy_driver swift --debug',
                     check_ret_code=True,
                     get_bareon_log=True)

        actual = node.run_cmd('parted -lm && pvs && lvs')[0]
        expected = """
BYT;
/dev/mapper/fpa_test_vg_2-fpa_opt_vol:981MB:dm:512:512:loop:Linux device-mapper (linear):;
1:0.00B:981MB:981MB:ext4::;

BYT;
/dev/mapper/fpa_test_vg_1-fpa_usr_vol:1413MB:dm:512:512:loop:Linux device-mapper (linear):;
1:0.00B:1413MB:1413MB:ext3::;

BYT;
/dev/vda:4295MB:virtblk:512:512:gpt:Virtio Block Device:;
1:1049kB:26.2MB:25.2MB::primary:bios_grub;
2:26.2MB:2622MB:2596MB:ext4:primary:;
3:2622MB:3147MB:524MB::primary:;

BYT;
/dev/vdb:2147MB:virtblk:512:512:gpt:Virtio Block Device:;
1:1049kB:26.2MB:25.2MB::primary:bios_grub;
2:26.2MB:1050MB:1023MB::primary:;
3:1050MB:2098MB:1049MB::primary:;

  PV         VG            Fmt  Attr PSize   PFree
  /dev/vda3  fpa_test_vg_1 lvm2 a--  440.00m 8.00m
  /dev/vdb2  fpa_test_vg_1 lvm2 a--  916.00m    0
  /dev/vdb3  fpa_test_vg_2 lvm2 a--  940.00m 4.00m
  LV          VG            Attr       LSize   Pool Origin Data%  Meta%  Move Log Cpy%Sync Convert
  fpa_usr_vol fpa_test_vg_1 -wi-a-----   1.32g
  fpa_opt_vol fpa_test_vg_2 -wi-a----- 936.00m
"""  # noqa
        utils.assertNoDiff(expected, actual)
