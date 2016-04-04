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

import os

from contextlib import contextmanager

from bareon import errors
from bareon.openstack.common import log as logging
from bareon.utils import fs as fu
from bareon.utils import partition as pu
from bareon.utils import utils

LOG = logging.getLogger(__name__)


class MountableMixin(object):

    def _mount_target(self, mount_dir, os_id, pseudo=True, treat_mtab=True):
        LOG.debug('Mounting target file systems: %s', mount_dir)
        # Here we are going to mount all file systems in partition schema.
        for fs in self.driver.partition_scheme.fs_sorted_by_depth(os_id):
            if fs.mount == 'swap':
                continue
            mount = os.path.join(mount_dir, fs.mount.strip(os.sep))
            utils.makedirs_if_not_exists(mount)
            fu.mount_fs(fs.type, str(fs.device), mount)

        if pseudo:
            for path in ('/sys', '/dev', '/proc'):
                utils.makedirs_if_not_exists(
                    os.path.join(mount_dir, path.strip(os.sep)))
                fu.mount_bind(mount_dir, path)

        if treat_mtab:
            mtab = utils.execute('chroot', mount_dir, 'grep', '-v', 'rootfs',
                                 '/proc/mounts')[0]
            mtab_path = os.path.join(mount_dir, 'etc/mtab')
            if os.path.islink(mtab_path):
                os.remove(mtab_path)
            with open(mtab_path, 'wb') as f:
                f.write(mtab)

    def _umount_target(self, mount_dir, os_id, pseudo=True):
        LOG.debug('Umounting target file systems: %s', mount_dir)
        if pseudo:
            for path in ('/proc', '/dev', '/sys'):
                fu.umount_fs(os.path.join(mount_dir, path.strip(os.sep)),
                             try_lazy_umount=True)
        for fs in self.driver.partition_scheme.fs_sorted_by_depth(os_id,
                                                                  True):
            if fs.mount == 'swap':
                continue
            fu.umount_fs(os.path.join(mount_dir, fs.mount.strip(os.sep)))

    @contextmanager
    def mount_target(self, mount_dir, os_id, pseudo=True, treat_mtab=True):
        self._mount_target(mount_dir, os_id, pseudo=pseudo,
                           treat_mtab=treat_mtab)
        try:
            yield
        finally:
            self._umount_target(mount_dir, os_id, pseudo)

    @contextmanager
    def _mount_bootloader(self, mount_dir):
        fs = filter(lambda fss: fss.mount == 'multiboot',
                    self.driver.partition_scheme.fss)
        if len(fs) > 1:
            raise errors.WrongPartitionSchemeError(
                'Multiple multiboot partitions found')

        utils.makedirs_if_not_exists(mount_dir)
        fu.mount_fs(fs[0].type, str(fs[0].device), mount_dir)

        yield pu.get_uuid(fs[0].device)

        fu.umount_fs(mount_dir)
