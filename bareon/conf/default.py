#
# Copyright 2017 Cray Inc.  All Rights Reserved.
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

from oslo_config import cfg

action_opts = [
    cfg.IntOpt(
        'timeout',
        default=10,
        help='Timeout in secs for GRUB'
    ),
    cfg.BoolOpt(
        'fix_udev_net_rules',
        default=True,
        help='Add udev rules for NIC remapping'
    ),
    cfg.ListOpt(
        'lvm_filter_for_mpath',
        default=['r|^/dev/disk/.*|',
                 'a|^/dev/mapper/.*|',
                 'r/.*/'],
        help='Extra filters for lvm.conf to force LVM works with partitions '
             'on multipath devices properly.'
    ),
    cfg.ListOpt(
        'mpath_lvm_preferred_names',
        default=['^/dev/mapper/'],
        help='List of devlinks patterns which are preffered for LVM. If '
             'multipath device has a few devlinks, LVM will use the one '
             'matching to the given pattern.'
    ),
    cfg.ListOpt(
        'mpath_lvm_scan_dirs',
        default=['/dev/disk/', '/dev/mapper/'],
        help='List of directories to scan recursively for LVM physical '
             'volumes. Devices in directories outside this hierarchy will be '
             'ignored.'
    ),
    cfg.StrOpt(
        'lvm_conf_path',
        default='/etc/lvm/lvm.conf',
        help='Path to LVM configuration file'
    ),
    cfg.StrOpt(
        'nc_template_path',
        default='/usr/share/bareon/cloud-init-templates',
        help='Path to directory with cloud init templates',
    ),
    cfg.StrOpt(
        'tmp_path',
        default='/tmp',
        help='Temporary directory for file manipulations',
    ),
    cfg.StrOpt(
        'config_drive_path',
        default='/tmp/config-drive.img',
        help='Path where to store generated config drive image',
    ),
    cfg.BoolOpt(
        'prepare_configdrive',
        default=True,
        help='Create configdrive file, use pre-builded if set to False'
    ),
    cfg.StrOpt(
        'udev_rules_dir',
        default='/etc/udev/rules.d',
        help='Path where to store actual rules for udev daemon',
    ),
    cfg.StrOpt(
        'udev_rules_lib_dir',
        default='/lib/udev/rules.d',
        help='Path where to store default rules for udev daemon',
    ),
    cfg.StrOpt(
        'udev_rename_substr',
        default='.renamedrule',
        help='Substring to which file extension .rules be renamed',
    ),
    cfg.StrOpt(
        'udev_empty_rule',
        default='empty_rule',
        help='Correct empty rule for udev daemon',
    ),
    cfg.BoolOpt(
        'skip_md_containers',
        default=True,
        help='Allow to skip MD containers (fake raid leftovers) while '
             'cleaning the rest of MDs',
    ),
    cfg.StrOpt(
        'partition_alignment',
        default='optimal',
        help='Set alignment for newly created partitions, valid alignment '
             'types are: none, cylinder, minimal, optimal'
    ),
]

generic_deploy_opts = [
    cfg.StrOpt(
        'udev_rules_dir',
        default='/etc/udev/rules.d',
        help='Path where to store actual rules for udev daemon',
    ),
    cfg.StrOpt(
        'udev_rules_lib_dir',
        default='/lib/udev/rules.d',
        help='Path where to store default rules for udev daemon',
    ),
    cfg.StrOpt(
        'udev_rename_substr',
        default='.renamedrule',
        help='Substring to which file extension .rules be renamed',
    ),
    cfg.StrOpt(
        'udev_empty_rule',
        default='empty_rule',
        help='Correct empty rule for udev daemon',
    ),
    cfg.IntOpt(
        'grub_timeout',
        default=5,
        help='Timeout in secs for GRUB'
    ),
    cfg.StrOpt(
        'default_root_password',
        default='r00tme',
        help='Default password for root user',
    ),
]

swift_deploy_opts = [
    cfg.StrOpt(
        'image_build_dir',
        default='/tmp',
        help='Directory where the image is supposed to be built',
    ),
    cfg.StrOpt(
        'image_build_suffix',
        default='.fuel-agent-image',
        help='Suffix which is used while creating temporary files',
    ),
    cfg.IntOpt(
        'max_loop_devices_count',
        default=255,
        # NOTE(agordeev): up to 256 loop devices could be allocated up to
        # kernel version 2.6.23, and the limit (from version 2.6.24 onwards)
        # isn't theoretically present anymore.
        help='Maximum allowed loop devices count to use'
    ),
    cfg.IntOpt(
        'sparse_file_size',
        # XXX: Apparently Fuel configures the node root filesystem to span
        # the whole hard drive. However 2 GB filesystem created with default
        # options can grow at most to 2 TB (1024x its initial size). This
        # maximal size can be configured by mke2fs -E resize=NNN option,
        # however the version of e2fsprogs shipped with CentOS 6.[65] seems
        # to silently ignore the `resize' option. Therefore make the initial
        # filesystem a bit bigger so it can grow to 8 TB.
        default=8192,
        help='Size of sparse file in MiBs'
    ),
    cfg.IntOpt(
        'loop_device_major_number',
        default=7,
        help='System-wide major number for loop device'
    ),
    cfg.IntOpt(
        'fetch_packages_attempts',
        default=10,
        help='Maximum allowed debootstrap/apt-get attempts to execute'
    ),
    cfg.StrOpt(
        'allow_unsigned_file',
        default='allow_unsigned_packages',
        help='File where to store apt setting for unsigned packages'
    ),
    cfg.StrOpt(
        'force_ipv4_file',
        default='force_ipv4',
        help='File where to store apt setting for forcing IPv4 usage'
    ),
    cfg.IntOpt(
        'max_allowed_attempts_attach_image',
        default=10,
        help='Maximum allowed attempts to attach image file to loop device'
    ),
]

utils_opts = [
    cfg.IntOpt(
        'data_chunk_size',
        default=1048576,
        help='Size of data chunk to operate with images'
    ),
    cfg.IntOpt(
        'http_max_retries',
        default=30,
        help='Maximum retries count for http requests. 0 means infinite',
    ),
    cfg.FloatOpt(
        'http_request_timeout',
        # Setting it to 10 secs will allow fuel-agent to overcome the momentary
        # peak loads when network bandwidth becomes as low as 0.1MiB/s, thus
        # preventing of wasting too much retries on such false positives.
        default=10.0,
        help='Http request timeout in seconds',
    ),
    cfg.FloatOpt(
        'http_retry_delay',
        default=2.0,
        help='Delay in seconds before the next http request retry',
    ),
    cfg.IntOpt(
        'read_chunk_size',
        default=1048576,
        help='Block size of data to read for calculating checksum',
    ),
    cfg.FloatOpt(
        'execute_retry_delay',
        default=2.0,
        help='Delay in seconds before the next exectuion will retry',
    ),
    cfg.IntOpt(
        'partition_udev_settle_attempts',
        default=10,
        help='How many times udev settle will be called after partitioning'
    ),
]


def register_opts(conf):
    conf.register_opts(action_opts)
    conf.register_opts(generic_deploy_opts)
    conf.register_opts(swift_deploy_opts)
    conf.register_opts(utils_opts)
