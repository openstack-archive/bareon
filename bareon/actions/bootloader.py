# Copyright 2016 Mirantis, Inc.
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


from io import open
import os

from oslo_config import cfg
from oslo_log import log as logging

from bareon.actions import base
from bareon.drivers.deploy import mixins
from bareon import errors
from bareon.utils import build as bu
from bareon.utils import grub as gu
from bareon.utils import hardware as hw
from bareon.utils import utils

CONF = cfg.CONF
LOG = logging.getLogger(__name__)


# TODO(agordeev): rename to GrubBootLoaderAction ?
class BootLoaderAction(base.BaseAction, mixins.MountableMixin):
    """BootLoaderAction

    installs and configures bootloader
    """

    def validate(self):
        # TODO(agordeev): implement validate for bootloader
        pass

    def execute(self):
        self.do_bootloader()

    def _override_lvm_config(self, chroot):
        # NOTE(sslypushenko) Due to possible races between LVM and multipath,
        # we need to adjust LVM devices filter.
        # This code is required only for Ubuntu 14.04, because in trusty,
        # LVM filters, does not recognize partions on multipath devices
        # out of the box. It is fixed in latest LVM versions
        multipath_devs = [parted.name
                          for parted in self.driver.partition_scheme.parteds
                          if hw.is_multipath_device(parted.name)]
        # If there are no multipath devices on the node, we should not do
        # anything to prevent regression.
        if multipath_devs:
            # We need to explicitly whitelist each non-mutlipath device
            lvm_filter = []
            for parted in self.driver.partition_scheme.parteds:
                device = parted.name
                if device in multipath_devs:
                    continue
                # We use devlinks from /dev/disk/by-id instead of /dev/sd*,
                # because the first one are persistent.
                devlinks_by_id = [
                    link for link in hw.udevreport(device).get('DEVLINKS', [])
                    if link.startswith('/dev/disk/by-id/')]
                for link in devlinks_by_id:
                    lvm_filter.append(
                        'a|^{}(p)?(-part)?[0-9]*|'.format(link))

            # Multipath devices should be whitelisted. All other devlinks
            # should be blacklisted, to prevent LVM from grubbing underlying
            # multipath devices.
            lvm_filter.extend(CONF.lvm_filter_for_mpath)
            # Setting devices/preferred_names also helps LVM to find devices by
            # the proper devlinks
            bu.override_lvm_config(
                chroot,
                {'devices': {
                    'scan': CONF.mpath_lvm_scan_dirs,
                    'global_filter': lvm_filter,
                    'preferred_names': CONF.mpath_lvm_preferred_names}},
                lvm_conf_path=CONF.lvm_conf_path,
                update_initramfs=True)

    def do_bootloader(self):
        LOG.debug('--- Installing bootloader (do_bootloader) ---')
        chroot = '/tmp/target'
        partition_scheme = self.driver.partition_scheme
        with self.mount_target(chroot):
            mount2uuid = {}
            for fs in partition_scheme.fss:
                mount2uuid[fs.mount] = utils.execute(
                    'blkid', '-c', '/dev/null', '-o', 'value', '-s', 'UUID',
                    fs.device, check_exit_code=[0])[0].strip()

            if '/' not in mount2uuid:
                raise errors.WrongPartitionSchemeError(
                    'Error: device with / mountpoint has not been found')

            self._override_lvm_config(chroot)
            grub = self.driver.grub

            guessed_version = gu.guess_grub_version(chroot=chroot)
            if guessed_version != grub.version:
                grub.version = guessed_version
                LOG.warning('Grub version differs from which the operating '
                            'system should have by default. Found version in '
                            'image: %s', guessed_version)
            boot_device = partition_scheme.boot_device(grub.version)
            install_devices = [d.name for d in partition_scheme.parteds
                               if d.install_bootloader]

            grub.append_kernel_params('root=UUID=%s ' % mount2uuid['/'])

            kernel = grub.kernel_name or \
                gu.guess_kernel(chroot=chroot, regexp=grub.kernel_regexp)

            initrd = grub.initrd_name or \
                gu.guess_initrd(chroot=chroot, regexp=grub.initrd_regexp)

            if grub.version == 1:
                gu.grub1_cfg(kernel=kernel, initrd=initrd,
                             kernel_params=grub.kernel_params, chroot=chroot,
                             grub_timeout=CONF.timeout)
                gu.grub1_install(install_devices, boot_device, chroot=chroot)
            else:
                # TODO(kozhukalov): implement which kernel to use by default
                # Currently only grub1_cfg accepts kernel and initrd
                # parameters.
                gu.grub2_cfg(kernel_params=grub.kernel_params, chroot=chroot,
                             grub_timeout=CONF.timeout)
                gu.grub2_install(install_devices, chroot=chroot)

            # TODO(agordeev): move to separate actions?

            if CONF.fix_udev_net_rules:
                # FIXME(agordeev) There's no convenient way to perfrom NIC
                # remapping in Ubuntu, so injecting files prior the first boot
                # should work
                with open(chroot + '/etc/udev/rules.d/70-persistent-net.rules',
                          'wt', encoding='utf-8') as f:
                    f.write(u'# Generated by bareon during provisioning: '
                            u'BEGIN\n')
                    # pattern is aa:bb:cc:dd:ee:ff_eth0,aa:bb:cc:dd:ee:ff_eth1
                    for mapping in self.driver.configdrive_scheme. \
                            common.udevrules.split(','):
                        mac_addr, nic_name = mapping.split('_')
                        f.write(u'SUBSYSTEM=="net", ACTION=="add", '
                                u'DRIVERS=="?*", ATTR{address}=="%s", '
                                u'ATTR{type}=="1", KERNEL=="eth*", '
                                u'NAME="%s"\n' % (mac_addr, nic_name))
                    f.write(
                        u'# Generated by bareon during provisioning: END\n')
                # FIXME(agordeev): Disable net-generator that adds new entries
                # to 70-persistent-net.rules
                with open(chroot + '/etc/udev/rules.d/'
                                   '75-persistent-net-generator.rules', 'wt',
                          encoding='utf-8') as f:
                    f.write(u'# Generated by bareon during provisioning:\n'
                            u'# DO NOT DELETE. It is needed to disable '
                            u'net-generator\n')

            # FIXME(kozhukalov): Prevent nailgun-agent from doing anything.
            # This ugly hack is to be used together with the command removing
            # this lock file not earlier than /etc/rc.local
            # The reason for this hack to appear is to prevent nailgun-agent
            # from changing mcollective config at the same time when cloud-init
            # does the same. Otherwise, we can end up with corrupted
            # mcollective config.
            # For details see https://bugs.launchpad.net/fuel/+bug/1449186
            LOG.debug('Preventing nailgun-agent from doing '
                      'anything until it is unlocked')
            utils.makedirs_if_not_exists(os.path.join(chroot,
                                                      'etc/nailgun-agent'))
            with open(os.path.join(chroot, 'etc/nailgun-agent/nodiscover'),
                      'w'):
                pass

            with open(chroot + '/etc/fstab', 'wt', encoding='utf-8') as f:
                for fs in self.driver.partition_scheme.fss:
                    # TODO(kozhukalov): Think of improving the logic so as to
                    # insert a meaningful fsck order value which is last zero
                    # at fstab line. Currently we set it into 0 which means
                    # a corresponding file system will never be checked. We
                    # assume puppet or other configuration tool will care of
                    # it.
                    if fs.mount == '/':
                        f.write(u'UUID=%s %s %s defaults,errors=panic 0 0\n' %
                                (mount2uuid[fs.mount], fs.mount, fs.type))
                    else:
                        f.write(u'UUID=%s %s %s defaults 0 0\n' %
                                (mount2uuid[fs.mount], fs.mount, fs.type))
