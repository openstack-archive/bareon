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

from oslo_config import cfg

from bareon import errors
from bareon.utils import utils

from bareon.drivers.data.base import BaseDataDriver
from bareon.drivers.data.base import ConfigDriveDataDriverMixin
from bareon.drivers.data.base import GrubBootloaderDataDriverMixin
from bareon.drivers.data.base import MultibootDeploymentMixin
from bareon.drivers.data.base import PartitioningDataDriverMixin
from bareon.drivers.data.base import ProvisioningDataDriverMixin


opts = [
    cfg.StrOpt(
        'config_drive_path',
        default='/tmp/config-drive.img',
        help='Path where to store generated config drive image',
    ),
]
CONF = cfg.CONF
CONF.register_opts(opts)


# TODO(lobur): This driver mostly copies nailgun driver. Need to merge them.
class GenericDataDriver(BaseDataDriver,
                        PartitioningDataDriverMixin,
                        ProvisioningDataDriverMixin,
                        ConfigDriveDataDriverMixin,
                        GrubBootloaderDataDriverMixin,
                        MultibootDeploymentMixin):

    def __init__(self, data):
        super(GenericDataDriver, self).__init__(data)
        # this var states whether boot partition
        # was already allocated on first matching volume
        # or not
        self._boot_partition_done = False
        # this var is used as a flag that /boot fs
        # has already been added. we need this to
        # get rid of md over all disks for /boot partition.
        self._boot_done = False

    @property
    def partition_scheme(self):
        if not hasattr(self, '_partition_scheme'):
            self._partition_scheme = self._get_partition_scheme()
        return self._partition_scheme

    @property
    def hw_partition_scheme(self):
        raise NotImplementedError

    @property
    def partitions_policy(self):
        """Returns string"""
        raise NotImplementedError

    @property
    def image_scheme(self):
        if not hasattr(self, '_image_scheme'):
            self._image_scheme = self._get_image_scheme()
        return self._image_scheme

    @property
    def image_meta(self):
        if not hasattr(self, '_image_meta'):
            self._image_meta = self._get_image_meta()
        return self._image_meta

    @property
    def grub(self):
        if not hasattr(self, '_grub'):
            self._grub = self._get_grub()
        return self._grub

    @property
    def operating_system(self):
        if not hasattr(self, '_operating_system'):
            self._operating_system = self._get_operating_system()
        return self._operating_system

    @property
    def configdrive_scheme(self):
        if not hasattr(self, '_configdrive_scheme'):
            self._configdrive_scheme = self._get_configdrive_scheme()
        return self._configdrive_scheme

    @property
    def is_configdrive_needed(self):
        raise NotImplementedError

    def create_configdrive(self):
        if self.is_configdrive_needed:
            self._create_configdrive()

    def _get_partition_scheme(self):
        raise NotImplementedError

    def _get_image_scheme(self):
        raise NotImplementedError

    def _get_image_meta(self):
        raise NotImplementedError

    def _get_grub(self):
        raise NotImplementedError

    def _get_operating_system(self):
        raise NotImplementedError

    def _get_configdrive_scheme(self):
        raise NotImplementedError

    def _create_configdrive(self):
        raise NotImplementedError

    def _add_configdrive_image(self):
        configdrive_device = self.partition_scheme.configdrive_device()
        if configdrive_device is None:
            raise errors.WrongPartitionSchemeError(
                'Error while trying to get configdrive device: '
                'configdrive device not found')
        size = os.path.getsize(CONF.config_drive_path)
        md5 = utils.calculate_md5(CONF.config_drive_path, size)
        self.image_scheme.add_image(
            uri='file://%s' % CONF.config_drive_path,
            target_device=configdrive_device,
            format='iso9660',
            container='raw',
            size=size,
            md5=md5,
        )

    @property
    def _ks_disks(self):
        return filter(lambda x: x['type'] == 'disk' and x['size'] > 0,
                      self._partition_data())

    @property
    def _ks_vgs(self):
        return filter(lambda x: x['type'] == 'vg', self._partition_data())

    def _getlabel(self, label):
        if not label:
            return ''
        # XFS will refuse to format a partition if the
        # disk label is > 12 characters.
        return ' -L {0} '.format(label[:12])

    @property
    def _small_ks_disks(self):
        """Get those disks which are smaller than 2T"""
        return [d for d in self._ks_disks if d['size'] <= 2 * 1024 * 1024]

    def get_os_ids(self):
        raise NotImplementedError
