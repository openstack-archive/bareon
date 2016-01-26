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

"""This file contains of helpers for manager.

   Helpers could be reused in multiple do actions like utils.
   But it's a piece of code which needs access to data objects from driver.
   That will effectively distinct a helper from low-level util.
   As utils should never use data object directly.
"""

from bareon.openstack.common import log as logging
from bareon.utils import fs as fu
from bareon.utils import utils


LOG = logging.getLogger(__name__)


def mount_target(driver, chroot, skip=['swap']):
    """Mount a set of file systems into a chroot

    :param driver: Instance of data driver object
    :param chroot: Directory where to mount file systems
    :param skip: A list of fs types to skip from mounting
    """
    LOG.debug('Mounting target file systems: %s', chroot)
    # Here we are going to mount all file systems in partition scheme.
    for fs in driver.partition_scheme.fs_sorted_by_depth():
        if fs.mount in skip:
            continue
        mount = chroot + fs.mount
        utils.makedirs_if_not_exists(mount)
        fu.mount_fs(fs.type, str(fs.device), mount)


def umount_target(driver, chroot, skip=['swap']):
    """Unmount a set of file systems inside a chroot

    :param driver: Instance of data driver object
    :param chroot: Directory where to umount file systems
    :param skip: A list of fs types to skip from umounting
    """
    for fs in driver.partition_scheme.fs_sorted_by_depth(reverse=True):
        if fs.mount in skip:
            continue
        fu.umount_fs(chroot + fs.mount)
