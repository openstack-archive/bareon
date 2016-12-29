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


import os
import shutil

import six

from oslo_log import log as logging

from bareon.actions import base
from bareon import errors
from bareon.utils import artifact as au
from bareon.utils import fs as fu
from bareon.utils import hardware as hw
from bareon.utils import utils

LOG = logging.getLogger(__name__)


class CopyImageAction(base.BaseAction):
    """CopyImageAction

    copies all necessary images on disks
    """

    def validate(self):
        # TODO(agordeev): implement validate for copyimage
        pass

    def execute(self):
        self.do_copyimage()

    def move_files_to_their_places(self, remove_src=True):
        """Move files from mount points to where those files should be.

        :param remove_src: Remove source files after sync if True (default).
        """

        # NOTE(kozhukalov): The thing is that sometimes we
        # have file system images and mount point hierachies
        # which are not aligned. Let's say, we have root file system
        # image, while partition scheme says that two file systems should
        # be created on the node: / and /var.
        # In this case root image has /var directory with a set of files.
        # Obviously, we need to move all these files from /var directory
        # on the root file system to /var file system because /var
        # directory will be used as mount point.
        # In order to achieve this we mount all existent file
        # systems into a flat set of temporary directories. We then
        # try to find specific paths which correspond to mount points
        # and move all files from these paths to corresponding file systems.

        mount_map = self.mount_target_flat()
        for fs_mount in sorted(mount_map):
            head, tail = os.path.split(fs_mount)
            while head != fs_mount:
                LOG.debug('Trying to move files for %s file system', fs_mount)
                if head in mount_map:
                    LOG.debug('File system %s is mounted into %s',
                              head, mount_map[head])
                    check_path = os.path.join(mount_map[head], tail)
                    LOG.debug('Trying to check if path %s exists', check_path)
                    if os.path.exists(check_path):
                        LOG.debug('Path %s exists. Trying to sync all files '
                                  'from there to %s',
                                  check_path, mount_map[fs_mount])
                        src_path = check_path + '/'
                        utils.execute('rsync', '-avH', src_path,
                                      mount_map[fs_mount])
                        if remove_src:
                            shutil.rmtree(check_path)
                        break
                if head == '/':
                    break
                head, _tail = os.path.split(head)
                tail = os.path.join(_tail, tail)
        self.umount_target_flat(mount_map)

    def mount_target_flat(self):
        """Mount a set of file systems into a set of temporary directories

        :returns: Mount map dict
        """

        LOG.debug('Mounting target file systems into a flat set '
                  'of temporary directories')
        mount_map = {}
        for fs in self.driver.partition_scheme.fss:
            if fs.mount == 'swap':
                continue
            # It is an ugly hack to resolve python2/3 encoding issues and
            # should be removed after transistion to python3
            try:
                type(fs.mount) is unicode
                fs_mount = fs.mount.encode('ascii', 'ignore')
            except NameError:
                fs_mount = fs.mount
            mount_map[fs_mount] = fu.mount_fs_temp(fs.type, str(fs.device))
        LOG.debug('Flat mount map: %s', mount_map)
        return mount_map

    def umount_target_flat(self, mount_map):
        """Umount file systems previously mounted into temporary directories.

        :param mount_map: Mount map dict
        """

        for mount_point in six.itervalues(mount_map):
            fu.umount_fs(mount_point)
            shutil.rmtree(mount_point)

    def do_copyimage(self):
        LOG.debug('--- Copying images (do_copyimage) ---')
        for image in self.driver.image_scheme.images:
            LOG.debug('Processing image: %s' % image.uri)
            processing = au.Chain()

            LOG.debug('Appending uri processor: %s' % image.uri)
            processing.append(image.uri)

            if image.uri.startswith('http://'):
                LOG.debug('Appending HTTP processor')
                processing.append(au.HttpUrl)
            elif image.uri.startswith('file://'):
                LOG.debug('Appending FILE processor')
                processing.append(au.LocalFile)

            if image.container == 'gzip':
                LOG.debug('Appending GZIP processor')
                processing.append(au.GunzipStream)

            LOG.debug('Appending TARGET processor: %s' % image.target_device)

            error = None
            if not os.path.exists(image.target_device):
                error = "TARGET processor '{0}' does not exist."
            elif not hw.is_block_device(image.target_device):
                error = "TARGET processor '{0}' is not a block device."
            if error:
                error = error.format(image.target_device)
                LOG.error(error)
                raise errors.WrongDeviceError(error)

            processing.append(image.target_device)

            LOG.debug('Launching image processing chain')
            processing.process()

            if image.size and image.md5:
                LOG.debug('Trying to compare image checksum')
                actual_md5 = utils.calculate_md5(image.target_device,
                                                 image.size)
                if actual_md5 == image.md5:
                    LOG.debug('Checksum matches successfully: md5=%s' %
                              actual_md5)
                else:
                    raise errors.ImageChecksumMismatchError(
                        'Actual checksum %s mismatches with expected %s for '
                        'file %s' % (actual_md5, image.md5,
                                     image.target_device))
            else:
                LOG.debug('Skipping image checksum comparing. '
                          'Ether size or hash have been missed')

            # TODO(agordeev): separate to another action?
            LOG.debug('Extending image file systems')
            if image.format in ('ext2', 'ext3', 'ext4', 'xfs'):
                LOG.debug('Extending %s %s' %
                          (image.format, image.target_device))
                fu.extend_fs(image.format, image.target_device)
        self.move_files_to_their_places()
