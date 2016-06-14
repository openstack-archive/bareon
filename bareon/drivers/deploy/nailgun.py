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

from io import open
import os
import shutil
import signal

from oslo_config import cfg
import six
import yaml

from bareon.actions import bootloader
from bareon.actions import configdrive
from bareon.actions import copyimage
from bareon.actions import partitioning
from bareon.drivers.deploy.base import BaseDeployDriver
from bareon import errors
from bareon.openstack.common import log as logging
from bareon.utils import build as bu
from bareon.utils import fs as fu
from bareon.utils import utils

opts = [
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
    cfg.StrOpt(
        'image_build_suffix',
        default='.bareon-image',
        help='Suffix which is used while creating temporary files',
    ),
    cfg.IntOpt(
        'grub_timeout',
        default=5,
        help='Timeout in secs for GRUB'
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
        'max_allowed_attempts_attach_image',
        default=10,
        help='Maximum allowed attempts to attach image file to loop device'
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
    cfg.BoolOpt(
        'prepare_configdrive',
        default=True,
        help='Create configdrive file, use pre-builded if set to False'
    ),
    cfg.BoolOpt(
        'fix_udev_net_rules',
        default=True,
        help='Add udev rules for NIC remapping'
    ),
]

cli_opts = [
    cfg.StrOpt(
        'image_build_dir',
        default='/tmp',
        help='Directory where the image is supposed to be built',
    ),
]

CONF = cfg.CONF
CONF.register_opts(opts)
CONF.register_cli_opts(cli_opts)

LOG = logging.getLogger(__name__)


class Manager(BaseDeployDriver):

    def do_partitioning(self):
        partitioning.PartitioningAction(self.driver).execute()

    def do_configdrive(self):
        configdrive.ConfigDriveAction(self.driver).execute()

    def do_copyimage(self):
        copyimage.CopyImageAction(self.driver).execute()

    def do_bootloader(self):
        bootloader.BootLoaderAction(self.driver).execute()

    @staticmethod
    def _update_metadata_with_repos(metadata, repos):
        """Update action metadata with information about repositories

        :param metadata: dict contains action metadata
        :param repos:  list of Repo objects
        :return:
        """

        for repo in repos:
            metadata.setdefault('repos', []).append({
                'type': 'deb',
                'name': repo.name,
                'uri': repo.uri,
                'suite': repo.suite,
                'section': repo.section,
                'priority': repo.priority,
                'meta': repo.meta})

    @staticmethod
    def _set_apt_repos(chroot, repos, proxies=None, direct_repo_addrs=None):
        """Configure APT to use the specified repositories

        Set apt-sources for chroot and update metadata in Manager.

        :param chroot: path to OS to operate on
        :param repos: list of DEBRepo objects
        :param proxies: dict protocol:uri format
        :param direct_repo_addrs: list of addreses which should be bypassed by
                                  proxy
        """
        LOG.debug("For set apt repositories will be used proxies: %s and"
                  " no_proxy: %s", proxies, direct_repo_addrs)
        for repo in repos:
            LOG.debug(
                'Adding repository source: name={name}, uri={uri}, '
                'suite={suite}, section={section}'.format(
                    name=repo.name,
                    uri=repo.uri,
                    suite=repo.suite,
                    section=repo.section))
            bu.add_apt_source(name=repo.name, uri=repo.uri, suite=repo.suite,
                              section=repo.section, chroot=chroot)
            LOG.debug(
                'Adding repository preference: name={name}, '
                'priority={priority}'.format(name=repo.name,
                                             priority=repo.priority))
            if repo.priority is not None:
                bu.add_apt_preference(
                    name=repo.name, priority=repo.priority, suite=repo.suite,
                    section=repo.section, chroot=chroot, uri=repo.uri,
                    proxies=proxies, direct_repo_addrs=direct_repo_addrs)

    def mount_target(self, chroot, treat_mtab=True, pseudo=True):
        """Mount a set of file systems into a chroot

        :param chroot: Directory where to mount file systems
        :param treat_mtab: If mtab needs to be actualized (Default: True)
        :param pseudo: If pseudo file systems
        need to be mounted (Default: True)
        """
        LOG.debug('Mounting target file systems: %s', chroot)
        # Here we are going to mount all file systems in partition scheme.
        for fs in self.driver.partition_scheme.fs_sorted_by_depth():
            if fs.mount == 'swap':
                continue
            mount = chroot + fs.mount
            utils.makedirs_if_not_exists(mount)
            fu.mount_fs(fs.type, str(fs.device), mount)

        if pseudo:
            for path in ('/sys', '/dev', '/proc'):
                utils.makedirs_if_not_exists(chroot + path)
                fu.mount_bind(chroot, path)

        if treat_mtab:
            mtab = utils.execute(
                'chroot', chroot, 'grep', '-v', 'rootfs', '/proc/mounts')[0]
            mtab_path = chroot + '/etc/mtab'
            if os.path.islink(mtab_path):
                os.remove(mtab_path)
            with open(mtab_path, 'wt', encoding='utf-8') as f:
                f.write(six.text_type(mtab))

    def umount_target(self, chroot, pseudo=True):
        LOG.debug('Umounting target file systems: %s', chroot)
        if pseudo:
            # umount fusectl (typically mounted at /sys/fs/fuse/connections)
            for path in ('/proc', '/dev', '/sys/fs/fuse/connections', '/sys'):
                fu.umount_fs(chroot + path)
        for fs in self.driver.partition_scheme.fs_sorted_by_depth(
                reverse=True):
            if fs.mount == 'swap':
                continue
            fu.umount_fs(chroot + fs.mount)

    def install_base_os(self, chroot):
        """Bootstrap a basic Linux system

        :param chroot directory where the installed OS can be found
        For now only Ubuntu is supported.
        Note: the data gets written to a different location (a set of
        ext4 images  located in the image_build_dir directory)
        Includes the following steps
        1) create temporary sparse files for all images (truncate)
        2) attach temporary files to loop devices (losetup)
        3) create file systems on these loop devices
        4) create temporary chroot directory
        5) mount loop devices into chroot directory
        6) install operating system (debootstrap and apt-get)
        """
        LOG.info('*** Preparing image space ***')
        for image in self.driver.image_scheme.images:
            LOG.debug('Creating temporary sparsed file for the '
                      'image: %s', image.uri)
            img_tmp_file = bu.create_sparse_tmp_file(
                dir=CONF.image_build_dir, suffix=CONF.image_build_suffix,
                size=CONF.sparse_file_size)
            LOG.debug('Temporary file: %s', img_tmp_file)

            # we need to remember those files
            # to be able to shrink them and move in the end
            image.img_tmp_file = img_tmp_file

            image.target_device.name = \
                bu.attach_file_to_free_loop_device(
                    img_tmp_file,
                    max_loop_devices_count=CONF.max_loop_devices_count,
                    loop_device_major_number=CONF.loop_device_major_number,
                    max_attempts=CONF.max_allowed_attempts_attach_image)

            # find fs with the same loop device object
            # as image.target_device
            fs = self.driver.partition_scheme.fs_by_device(
                image.target_device)

            LOG.debug('Creating file system on the image')
            fu.make_fs(
                fs_type=fs.type,
                fs_options=fs.options,
                fs_label=fs.label,
                dev=six.text_type(fs.device))
            if fs.type == 'ext4':
                LOG.debug('Trying to disable journaling for ext4 '
                          'in order to speed up the build')
                utils.execute('tune2fs', '-O', '^has_journal',
                              six.text_type(fs.device))

        # mounting all images into chroot tree
        self.mount_target(chroot, treat_mtab=False, pseudo=False)
        LOG.info('Installing BASE operating system into image')
        # FIXME(kozhukalov): !!! we need this part to be OS agnostic

        # DEBOOTSTRAP
        # we use first repo as the main mirror
        uri = self.driver.operating_system.repos[0].uri
        suite = self.driver.operating_system.repos[0].suite
        proxies = self.driver.operating_system.proxies

        LOG.debug('Preventing services from being get started')
        bu.suppress_services_start(chroot)
        LOG.debug('Installing base operating system using debootstrap')
        bu.run_debootstrap(uri=uri, suite=suite, chroot=chroot,
                           attempts=CONF.fetch_packages_attempts,
                           proxies=proxies.proxies,
                           direct_repo_addr=proxies.direct_repo_addr_list)

        # APT-GET
        LOG.debug('Configuring apt inside chroot')
        LOG.debug('Setting environment variables')
        bu.set_apt_get_env()
        LOG.debug('Allowing unauthenticated repos')
        bu.pre_apt_get(chroot,
                       allow_unsigned_file=CONF.allow_unsigned_file,
                       force_ipv4_file=CONF.force_ipv4_file,
                       proxies=proxies.proxies,
                       direct_repo_addr=proxies.direct_repo_addr_list)

        # we need /proc to be mounted for apt-get success
        LOG.debug('Preventing services from being get started')
        bu.suppress_services_start(chroot)
        utils.makedirs_if_not_exists(os.path.join(chroot, 'proc'))

        # we need /proc to be mounted for apt-get success
        fu.mount_bind(chroot, '/proc')
        bu.populate_basic_dev(chroot)

    def destroy_chroot(self, chroot):
        # Umount chroot tree and remove images tmp files
        if not bu.stop_chrooted_processes(chroot, signal=signal.SIGTERM):
            bu.stop_chrooted_processes(chroot, signal=signal.SIGKILL)
        LOG.debug('Finally: umounting procfs %s', os.path.join(chroot, 'proc'))
        fu.umount_fs(os.path.join(chroot, 'proc'))
        LOG.debug('Finally: umounting chroot tree %s', chroot)
        self.umount_target(chroot, pseudo=False)
        for image in self.driver.image_scheme.images:
            if image.target_device.name:
                LOG.debug('Finally: detaching loop device: %s',
                          image.target_device.name)
                try:
                    bu.deattach_loop(image.target_device.name)
                except errors.ProcessExecutionError as e:
                    LOG.warning('Error occured while trying to detach '
                                'loop device %s. Error message: %s',
                                image.target_device.name, e)
            if image.img_tmp_file:
                LOG.debug('Finally: removing temporary file: %s',
                          image.img_tmp_file)
                try:
                    os.unlink(image.img_tmp_file)
                except OSError:
                    LOG.debug('Finally: file %s seems does not exist '
                              'or can not be removed', image.img_tmp_file)
        try:
            os.rmdir(chroot)
        except OSError:
            LOG.debug('Finally: directory %s seems does not exist '
                      'or can not be removed', chroot)

    def dump_mkbootstrap_meta(self, metadata, c_dir, bootstrap_scheme):
        """Dump mkbootstrap metadata to yaml file

        :param metadata: dict with meta
        :param file:
        :return:

        1)Process module files
        2)Collect data from do_mkbootstrap metadata
        3)Collect somedata from driver
        4_Drop result dict 'drop_data' to yaml file
        """
        meta_file = os.path.join(
            c_dir, bootstrap_scheme.container.meta_file)
        drop_data = {'modules': {}}
        for module in bootstrap_scheme.modules:
            fname = os.path.basename(module.uri)
            fs_file = os.path.join(c_dir, fname)
            try:
                raw_size = os.path.getsize(fs_file)
            except IOError as exc:
                LOG.error('There was an error while getting file'
                          ' size: {0}'.format(exc))
                raise
            raw_md5 = utils.calculate_md5(fs_file, raw_size)
            drop_data['modules'][module.name] = {
                'raw_md5': raw_md5,
                'raw_size': raw_size,
                'file': fname,
                'uri': module.uri
            }
        drop_data['uuid'] = bootstrap_scheme.uuid
        drop_data['extend_kopts'] = bootstrap_scheme.extend_kopts
        drop_data['os'] = metadata['os']
        drop_data['all_packages'] = metadata['all_packages']
        drop_data['repos'] = metadata['repos']
        drop_data['label'] = bootstrap_scheme.label

        LOG.debug('Image metadata: %s', drop_data)
        with open(meta_file, 'wt') as f:
            yaml.safe_dump(drop_data, stream=f, encoding='utf-8')

    def do_reboot(self):
        LOG.debug('--- Rebooting node (do_reboot) ---')
        utils.execute('reboot')

    def do_multiboot_bootloader(self):
        pass

    def do_install_os(self):
        pass

    def do_provisioning(self):
        LOG.debug('--- Provisioning (do_provisioning) ---')
        self.do_partitioning()
        self.do_configdrive()
        self.do_copyimage()
        self.do_bootloader()
        LOG.debug('--- Provisioning END (do_provisioning) ---')

    def do_mkbootstrap(self):
        """Building bootstrap image

        Currently supports only Ubuntu-Trusty
        Includes the following steps
        1) Allocate and configure debootstrap.
        2) Install packages
        3) Run user-post script(is defined)
        4) populate squashfs\init\vmlinuz files
        5) create metadata.yaml and pack thats all into tar.gz
        """
        LOG.info('--- Building bootstrap image (do_mkbootstrap) ---')
        driver_os = self.driver.operating_system
        # c_dir = output container directory, where all builded files will
        # be stored, before packaging into archive
        LOG.debug('Creating bootstrap container folder')
        c_dir = bu.mkdtemp_smart(CONF.image_build_dir,
                                 CONF.image_build_suffix + '_container')
        try:
            chroot = bu.mkdtemp_smart(
                CONF.image_build_dir, CONF.image_build_suffix)
            self.install_base_os(chroot)
            bs_scheme = self.driver.bootstrap_scheme
            # init modules, needed for bootstrap. Currently
            #  we support only one scheme initrd + rootfs + kernel
            initrd = filter(lambda x: x.name == 'initrd',
                            bs_scheme.modules)[0]
            rootfs = filter(lambda x: x.name == 'rootfs',
                            bs_scheme.modules)[0]
            metadata = {}
            metadata['os'] = driver_os.to_dict()
            packages = driver_os.packages
            metadata['packages'] = packages

            self._set_apt_repos(
                chroot, driver_os.repos,
                proxies=driver_os.proxies.proxies,
                direct_repo_addrs=driver_os.proxies.direct_repo_addr_list)
            self._update_metadata_with_repos(
                metadata, driver_os.repos)
            LOG.debug('Installing packages using apt-get: %s',
                      ' '.join(packages))
            # disable hosts/resolv files
            bu.propagate_host_resolv_conf(chroot)
            # for case when https proxy is used we need to upload cert file
            # into chroot and update certificates
            if hasattr(bs_scheme, 'extra_files') and bs_scheme.extra_files:
                for extra in bs_scheme.extra_files:
                    bu.rsync_inject(extra, chroot)
                bu.update_certs(chroot)
            bu.run_apt_get(chroot, packages=packages,
                           attempts=CONF.fetch_packages_attempts)
            LOG.debug('Post-install OS configuration')
            if (hasattr(bs_scheme, 'root_ssh_authorized_file') and
                    bs_scheme.root_ssh_authorized_file):
                LOG.debug('Put ssh auth file %s',
                          bs_scheme.root_ssh_authorized_file)
                auth_file = os.path.join(chroot, 'root/.ssh/authorized_keys')
                utils.makedirs_if_not_exists(os.path.dirname(
                    auth_file), mode=0o700)
                shutil.copy(
                    bs_scheme.root_ssh_authorized_file,
                    auth_file)
                os.chmod(auth_file, 0o700)
            # Allow user to drop and run script inside chroot:
            if (hasattr(bs_scheme, 'post_script_file') and
                    bs_scheme.post_script_file):
                bu.run_script_in_chroot(
                    chroot, bs_scheme.post_script_file)
            # Save runtime_uuid into bootstrap
            bu.dump_runtime_uuid(bs_scheme.uuid,
                                 os.path.join(chroot,
                                              'etc/nailgun-agent/config.yaml'))
            bu.do_post_inst(chroot,
                            allow_unsigned_file=CONF.allow_unsigned_file,
                            force_ipv4_file=CONF.force_ipv4_file)
            # restore disabled hosts/resolv files
            bu.restore_resolv_conf(chroot)
            metadata['all_packages'] = bu.get_installed_packages(chroot)
            # We need to recompress initramfs with new compression:
            bu.recompress_initramfs(
                chroot,
                compress=initrd.compress_format)
            # Bootstrap nodes load the kernel and initramfs via the network,
            # therefore remove the kernel and initramfs located in root
            # filesystem to make the image smaller (and save the network
            # bandwidth and the boot time)
            bu.copy_kernel_initramfs(chroot, c_dir, clean=True)
            LOG.debug('Making sure there are no running processes '
                      'inside chroot before trying to umount chroot')
            if not bu.stop_chrooted_processes(chroot, signal=signal.SIGTERM):
                if not bu.stop_chrooted_processes(
                        chroot, signal=signal.SIGKILL):
                    raise errors.UnexpectedProcessError(
                        'Stopping chrooted processes failed. '
                        'There are some processes running in chroot %s',
                        chroot)
            bu.run_mksquashfs(
                chroot, os.path.join(c_dir, os.path.basename(rootfs.uri)),
                rootfs.compress_format)
            self.dump_mkbootstrap_meta(metadata, c_dir, bs_scheme)
            output = bu.save_bs_container(self.driver.output, c_dir,
                                          bs_scheme.container.format)
            LOG.info('--- Building bootstrap image END (do_mkbootstrap) ---')
            return output
        except Exception as exc:
            LOG.error('Failed to build bootstrap image: %s', exc)
            raise
        finally:
            LOG.info('Cleanup chroot')
            self.destroy_chroot(chroot)
            try:
                shutil.rmtree(c_dir)
            except OSError:
                LOG.debug('Finally: directory %s seems does not exist '
                          'or can not be removed', c_dir)
            # TODO(kozhukalov): Split this huge method

    # into a set of smaller ones
    # https://bugs.launchpad.net/fuel/+bug/1444090
    def do_build_image(self):
        """Building OS images

        Includes the following steps
        1) create temporary sparse files for all images (truncate)
        2) attach temporary files to loop devices (losetup)
        3) create file systems on these loop devices
        4) create temporary chroot directory
        5) install operating system (install_base_os)
        6) configure apt-get sources,and perform package install.
        7) configure OS (clean sources.list and preferences, etc.)
        8) umount loop devices
        9) resize file systems on loop devices
        10) shrink temporary sparse files (images)
        11) containerize (gzip) temporary sparse files
        12) move temporary gzipped files to their final location
        """
        LOG.info('--- Building image (do_build_image) ---')
        driver_os = self.driver.operating_system
        # TODO(kozhukalov): Implement metadata
        # as a pluggable data driver to avoid any fixed format.
        metadata = {}

        metadata['os'] = driver_os.to_dict()

        # TODO(kozhukalov): implement this using image metadata
        # we need to compare list of packages and repos
        LOG.info('*** Checking if image exists ***')
        if all([os.path.exists(img.uri.split('file://', 1)[1])
                for img in self.driver.image_scheme.images]):
            LOG.debug('All necessary images are available. '
                      'Nothing needs to be done.')
            return
        LOG.debug('At least one of the necessary images is unavailable. '
                  'Starting build process.')
        chroot = bu.mkdtemp_smart(
            CONF.image_build_dir, CONF.image_build_suffix)
        try:
            self.install_base_os(chroot)
            packages = driver_os.packages
            metadata['packages'] = packages

            self._set_apt_repos(
                chroot, driver_os.repos,
                proxies=driver_os.proxies.proxies,
                direct_repo_addrs=driver_os.proxies.direct_repo_addr_list)
            self._update_metadata_with_repos(
                metadata, driver_os.repos)

            LOG.debug('Installing packages using apt-get: %s',
                      ' '.join(packages))
            bu.run_apt_get(chroot, packages=packages,
                           attempts=CONF.fetch_packages_attempts)

            LOG.debug('Post-install OS configuration')
            bu.do_post_inst(chroot,
                            allow_unsigned_file=CONF.allow_unsigned_file,
                            force_ipv4_file=CONF.force_ipv4_file)

            LOG.debug('Making sure there are no running processes '
                      'inside chroot before trying to umount chroot')
            if not bu.stop_chrooted_processes(chroot, signal=signal.SIGTERM):
                if not bu.stop_chrooted_processes(
                        chroot, signal=signal.SIGKILL):
                    raise errors.UnexpectedProcessError(
                        'Stopping chrooted processes failed. '
                        'There are some processes running in chroot %s',
                        chroot)

            LOG.info('*** Finalizing image space ***')
            fu.umount_fs(os.path.join(chroot, 'proc'))
            # umounting all loop devices
            self.umount_target(chroot, pseudo=False)

            for image in self.driver.image_scheme.images:
                # find fs with the same loop device object
                # as image.target_device
                fs = self.driver.partition_scheme.fs_by_device(
                    image.target_device)

                if fs.type == 'ext4':
                    LOG.debug('Trying to re-enable journaling for ext4')
                    utils.execute('tune2fs', '-O', 'has_journal',
                                  str(fs.device))

                if image.target_device.name:
                    LOG.debug('Finally: detaching loop device: {0}'.format(
                        image.target_device.name))
                    try:
                        bu.deattach_loop(image.target_device.name)
                    except errors.ProcessExecutionError as e:
                        LOG.warning('Error occured while trying to detach '
                                    'loop device {0}. Error message: {1}'.
                                    format(image.target_device.name, e))

                LOG.debug('Shrinking temporary image file: %s',
                          image.img_tmp_file)
                bu.shrink_sparse_file(image.img_tmp_file)

                raw_size = os.path.getsize(image.img_tmp_file)
                raw_md5 = utils.calculate_md5(image.img_tmp_file, raw_size)

                LOG.debug('Containerizing temporary image file: %s',
                          image.img_tmp_file)
                img_tmp_containerized = bu.containerize(
                    image.img_tmp_file, image.container,
                    chunk_size=CONF.data_chunk_size)
                img_containerized = image.uri.split('file://', 1)[1]

                # NOTE(kozhukalov): implement abstract publisher
                LOG.debug('Moving image file to the final location: %s',
                          img_containerized)
                shutil.move(img_tmp_containerized, img_containerized)

                container_size = os.path.getsize(img_containerized)
                container_md5 = utils.calculate_md5(
                    img_containerized, container_size)

                metadata.setdefault('images', []).append({
                    'raw_md5': raw_md5,
                    'raw_size': raw_size,
                    'raw_name': None,
                    'container_name': os.path.basename(img_containerized),
                    'container_md5': container_md5,
                    'container_size': container_size,
                    'container': image.container,
                    'format': image.format})

            # NOTE(kozhukalov): implement abstract publisher
            LOG.debug('Image metadata: %s', metadata)
            with open(self.driver.metadata_uri.split('file://', 1)[1],
                      'wt', encoding='utf-8') as f:
                yaml.safe_dump(metadata, stream=f)
            LOG.info('--- Building image END (do_build_image) ---')
        except Exception as exc:
            LOG.error('Failed to build image: %s', exc)
            raise
        finally:
            LOG.info('Cleanup chroot')
            self.destroy_chroot(chroot)


def list_opts():
    """Returns a list of oslo.config options available in the library.

    The returned list includes all oslo.config options which may be registered
    at runtime by the library.

    Each element of the list is a tuple. The first element is the name of the
    group under which the list of elements in the second element will be
    registered. A group name of None corresponds to the [DEFAULT] group in
    config files.

    The purpose of this is to allow tools like the Oslo sample config file
    generator (oslo-config-generator) to discover the options exposed to users
    by this library.

    :returns: a list of (group_name, opts) tuples
    """
    return [(None, (opts))]
