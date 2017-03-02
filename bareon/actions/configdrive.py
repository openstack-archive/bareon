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
import tempfile

from oslo_config import cfg
from oslo_log import log as logging
import six

from bareon.actions import base
from bareon.drivers.data import base as datadrivers
from bareon import errors
from bareon.utils import fs as fu
from bareon.utils import utils

CONF = cfg.CONF
LOG = logging.getLogger(__name__)


class ConfigDriveAction(base.BaseAction):
    """ConfigDriveAction

    creates ConfigDrive datasource image for cloud-init
    """

    def validate(self):
        # TODO(agordeev): implement validate for configdrive
        pass

    def execute(self):
        if not isinstance(self.driver, datadrivers.ConfigDriveDataDriverMixin):
            return
        self.do_configdrive()

    def _make_configdrive_image(self, src_files):
        bs = 4096
        configdrive_device = self.driver.partition_scheme.configdrive_device()
        size = utils.execute('blockdev', '--getsize64', configdrive_device)[0]
        size = int(size.strip())

        utils.execute('truncate', '--size=%d' % size, CONF.config_drive_path)
        fu.make_fs(
            fs_type='ext2',
            fs_options=' -b %d -F ' % bs,
            fs_label='config-2',
            dev=six.text_type(CONF.config_drive_path))

        mount_point = tempfile.mkdtemp(dir=CONF.tmp_path)
        try:
            fu.mount_fs('ext2', CONF.config_drive_path, mount_point)
            for file_path in src_files:
                name = os.path.basename(file_path)
                if os.path.isdir(file_path):
                    shutil.copytree(file_path, os.path.join(mount_point, name))
                else:
                    shutil.copy2(file_path, mount_point)
        except Exception as exc:
            LOG.error('Error copying files to configdrive: %s', exc)
            raise
        finally:
            fu.umount_fs(mount_point)
            os.rmdir(mount_point)

    def _prepare_configdrive_files(self):
        # see data sources part of cloud-init documentation
        # for directory structure
        cd_root = tempfile.mkdtemp(dir=CONF.tmp_path)
        cd_latest = os.path.join(cd_root, 'openstack', 'latest')
        md_output_path = os.path.join(cd_latest, 'meta_data.json')
        ud_output_path = os.path.join(cd_latest, 'user_data')
        os.makedirs(cd_latest)

        cc_output_path = os.path.join(CONF.tmp_path, 'cloud_config.txt')
        bh_output_path = os.path.join(CONF.tmp_path, 'boothook.txt')

        tmpl_dir = CONF.nc_template_path
        utils.render_and_save(
            tmpl_dir,
            self.driver.configdrive_scheme.template_names('cloud_config'),
            self.driver.configdrive_scheme.template_data(),
            cc_output_path
        )
        utils.render_and_save(
            tmpl_dir,
            self.driver.configdrive_scheme.template_names('boothook'),
            self.driver.configdrive_scheme.template_data(),
            bh_output_path
        )
        utils.render_and_save(
            tmpl_dir,
            self.driver.configdrive_scheme.template_names('meta_data_json'),
            self.driver.configdrive_scheme.template_data(),
            md_output_path
        )

        utils.execute(
            'write-mime-multipart', '--output=%s' % ud_output_path,
            '%s:text/cloud-boothook' % bh_output_path,
            '%s:text/cloud-config' % cc_output_path)
        return [os.path.join(cd_root, 'openstack')]

    def do_configdrive(self):
        LOG.debug('--- Creating configdrive (do_configdrive) ---')
        if CONF.prepare_configdrive:
            files = self._prepare_configdrive_files()
            self._make_configdrive_image(files)

        if CONF.prepare_configdrive or os.path.isfile(CONF.config_drive_path):
            self._add_configdrive_image()

    def _add_configdrive_image(self):
        # TODO(agordeev): move to validate?
        configdrive_device = self.driver.partition_scheme.configdrive_device()
        if configdrive_device is None:
            raise errors.WrongPartitionSchemeError(
                'Error while trying to get configdrive device: '
                'configdrive device not found')
        size = os.path.getsize(CONF.config_drive_path)
        md5 = utils.calculate_md5(CONF.config_drive_path, size)
        fs_type = fu.get_fs_type(CONF.config_drive_path)
        self.driver.image_scheme.add_image(
            uri='file://%s' % CONF.config_drive_path,
            target_device=configdrive_device,
            format=fs_type,
            container='raw',
            size=size,
            md5=md5,
        )
