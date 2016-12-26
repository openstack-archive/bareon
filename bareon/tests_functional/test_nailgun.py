#
# Copyright 2016 Cray Inc.  All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from copy import deepcopy

import utils

from bareon import tests_functional


PROVISION_SAMPLE_DATA = {
    "profile": "pro_fi-le",
    "name_servers_search": "\"domain.tld\"",
    "uid": "1",
    "interfaces": {
        "eth2": {
            "static": "0",
            "mac_address": "08:00:27:b1:d7:15"
        },
        "eth1": {
            "static": "0",
            "mac_address": "08:00:27:46:43:60"
        },
        "eth0": {
            "ip_address": "10.20.0.3",
            "dns_name": "node-1.domain.tld",
            "netmask": "255.255.255.0",
            "static": "0",
            "mac_address": "08:00:27:79:da:80"
        }
    },
    "interfaces_extra": {
        "eth2": {
            "onboot": "no",
            "peerdns": "no"
        },
        "eth1": {
            "onboot": "no",
            "peerdns": "no"
        },
        "eth0": {
            "onboot": "yes",
            "peerdns": "no"
        }
    },
    "power_type": "ssh",
    "power_user": "root",
    "kernel_options": {
        "udevrules": "08:00:27:79:da:80_eth0,08:00:27:46:43:60_eth1,"
                     "08:00:27:b1:d7:15_eth2",
        "netcfg/choose_interface": "08:00:27:79:da:80"
    },
    "power_address": "10.20.0.253",
    "name_servers": "\"10.20.0.2\"",
    "ks_meta": {
        "gw": "10.20.0.1",
        "image_data": {
            "/": {
                "uri": "",
                "format": "ext4",
                "container": "raw"
            }
        },
        "timezone": "America/Los_Angeles",
        "master_ip": "10.20.0.2",
        "mco_enable": 1,
        "mco_vhost": "mcollective",
        "mco_pskey": "unset",
        "mco_user": "mcollective",
        "puppet_enable": 0,
        "fuel_version": "5.0.1",
        "install_log_2_syslog": 1,
        "mco_password": "marionette",
        "puppet_auto_setup": 1,
        "puppet_master": "fuel.domain.tld",
        "mco_auto_setup": 1,
        "auth_key": "fake_auth_key",
        "authorized_keys": ["fake_authorized_key1", "fake_authorized_key2"],
        "repo_setup": {
            "repos": [
                {
                    "name": "repo1",
                    "type": "deb",
                    "uri": "uri1",
                    "suite": "suite",
                    "section": "section",
                    "priority": 1001
                },
                {
                    "name": "repo2",
                    "type": "deb",
                    "uri": "uri2",
                    "suite": "suite",
                    "section": "section",
                    "priority": 1001
                }
            ]
        },
        "pm_data": {
            "kernel_params": "console=ttyS0,9600 console=tty0 rootdelay=90 "
                             "nomodeset",
            "ks_spaces": [
                {
                    "name": "vda",
                    "extra": [],
                    "free_space": 4000,
                    "volumes": [
                        {
                            "size": 2600,
                            "mount": "/",
                            "type": "partition",
                            "file_system": "ext4",
                            "name": "root"
                        },
                        {
                            "mount": "/tmp",
                            "size": 200,
                            "type": "partition",
                            "file_system": "ext2",
                            "name": "TMP"
                        },
                        {
                            "type": "lvm_meta_pool",
                            "size": 0
                        },
                        {
                            "size": 1000,
                            "type": "pv",
                            "lvm_meta_size": 64,
                            "vg": "image"
                        }
                    ],
                    "type": "disk",
                    "id": "vda",
                    "size": 4000
                },
                {
                    "name": "vdb",
                    "extra": [],
                    "free_space": 2000,
                    "volumes": [
                        {
                            "type": "lvm_meta_pool",
                            "size": 64
                        },
                        {
                            "size": 500,
                            "type": "pv",
                            "lvm_meta_size": 64,
                            "vg": "os"
                        },
                        {
                            "size": 1300,
                            "type": "pv",
                            "lvm_meta_size": 64,
                            "vg": "image"
                        }
                    ],
                    "type": "disk",
                    "id": "sdb",
                    "size": 2000
                },
                {
                    "_allocate_size": "min",
                    "label": "Base System",
                    "min_size": 400,
                    "volumes": [
                        {
                            "mount": "swap",
                            "size": 400,
                            "type": "lv",
                            "name": "swap",
                            "file_system": "swap"
                        }
                    ],
                    "type": "vg",
                    "id": "os"
                },
                {
                    "_allocate_size": "min",
                    "label": "Zero size volume",
                    "min_size": 0,
                    "volumes": [
                        {
                            "mount": "none",
                            "size": 0,
                            "type": "lv",
                            "name": "zero_size",
                            "file_system": "xfs"
                        }
                    ],
                    "type": "vg",
                    "id": "zero_size"
                },
                {
                    "_allocate_size": "all",
                    "label": "Image Storage",
                    "min_size": 2100,
                    "volumes": [
                        {
                            "mount": "/var/lib/glance",
                            "size": 2100,
                            "type": "lv",
                            "name": "glance",
                            "file_system": "xfs"
                        }
                    ],
                    "type": "vg",
                    "id": "image"
                }
            ]
        },
        "mco_connector": "rabbitmq",
        "mco_host": "10.20.0.2",
        "mco_identity": 1
    },
    "name": "node-1",
    "hostname": "node-1.domain.tld",
    "slave_name": "node-1",
    "power_pass": "/root/.ssh/bootstrap.rsa",
    "netboot_enabled": "1"
}


class TestNailgun(tests_functional.TestCase):
    def test_provision(self):
        data = deepcopy(PROVISION_SAMPLE_DATA)
        data['ks_meta']['image_data']['/']['uri'] = self.env.get_url_for_image(
            'centos-7.1.1503.fpa_func_test.raw',
            'swift')

        self.env.setup(node_template="two_disks.xml",
                       deploy_config=data)
        node = self.env.node

        node.run_cmd('bareon-provision --data_driver nailgun '
                     '--deploy_driver nailgun',
                     check_ret_code=True,
                     get_bareon_log=True)

        actual = node.run_cmd('parted -lm && pvs && lvs')[0]
        expected = """
BYT;
/dev/mapper/image-glance:2202MB:dm:512:512:loop:Linux device-mapper (linear):;
1:0.00B:2202MB:2202MB:xfs::;

BYT;
/dev/mapper/os-swap:419MB:dm:512:512:loop:Linux device-mapper (linear):;
1:0.00B:419MB:419MB:linux-swap(v1)::;

BYT;
/dev/vda:4295MB:virtblk:512:512:gpt:Virtio Block Device:;
1:1049kB:26.2MB:25.2MB::primary:bios_grub;
2:26.2MB:236MB:210MB::primary:;
3:236MB:2962MB:2726MB:ext4:primary:;
4:2962MB:3172MB:210MB:ext2:primary:;
5:3172MB:4221MB:1049MB::primary:;

BYT;
/dev/vdb:2147MB:virtblk:512:512:gpt:Virtio Block Device:;
1:1049kB:26.2MB:25.2MB::primary:bios_grub;
2:26.2MB:236MB:210MB::primary:;
3:236MB:760MB:524MB::primary:;
4:760MB:2123MB:1363MB::primary:;

  PV         VG    Fmt  Attr PSize   PFree
  /dev/vda5  image lvm2 a--  940.00m 80.00m
  /dev/vdb3  os    lvm2 a--  440.00m 40.00m
  /dev/vdb4  image lvm2 a--    1.21g     0
  LV     VG    Attr       LSize   Pool Origin Data%  Meta%  Move Log Cpy%Sync Convert
  glance image -wi-a-----   2.05g
  swap   os    -wi-a----- 400.00m
"""  # noqa
        utils.assertNoDiff(expected, actual)

        # TODO(lobur): Cloud init failure (readonly filesystem /var/lib/..)
        # Thus no ssh key added, cannot check further

        # node.reboot_to_hdd()
        # node.wait_for_boot()
        #
        # node.ssh_login = "centos"
        # actual = node.run_cmd('uname -a')[0]
        # expected = (
        # 'Linux rft-func-test-tenant-vm 3.10.0-229.20.1.el7.x86_64'
        # ' #1 SMP Tue Nov 3 19:10:07 UTC 2015 x86_64 x86_64 x86_64'
        # ' GNU/Linux\n')
        #
        # utils.assertNoDiff(expected, actual)
