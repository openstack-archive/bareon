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

from oslo_config import cfg

from bareon.actions import base
from bareon import errors
from bareon.openstack.common import log as logging
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
    cfg.BoolOpt(
        'prepare_configdrive',
        default=True,
        help='Create configdrive file, use pre-builded if set to False'
    ),
]

CONF = cfg.CONF
CONF.register_opts(opts)

LOG = logging.getLogger(__name__)


class ConfigDriveAction(base.BaseAction):
    """ConfigDriveAction

    creates No cloud datasource image for cloud-init
    """

    def validate(self):
        # TODO(agordeev): implement validate for configdrive
        pass

    def execute(self):
        self.do_configdrive()

    def do_configdrive(self):
        LOG.debug('--- Creating configdrive (do_configdrive) ---')
        if CONF.prepare_configdrive:
            cc_output_path = os.path.join(CONF.tmp_path, 'cloud_config.txt')
            bh_output_path = os.path.join(CONF.tmp_path, 'boothook.txt')
            # NOTE:file should be strictly named as 'user-data'
            #      the same is for meta-data as well
            ud_output_path = os.path.join(CONF.tmp_path, 'user-data')
            md_output_path = os.path.join(CONF.tmp_path, 'meta-data')

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
                self.driver.configdrive_scheme.template_names('meta_data'),
                self.driver.configdrive_scheme.template_data(),
                md_output_path
            )

            utils.execute(
                'write-mime-multipart', '--output=%s' % ud_output_path,
                '%s:text/cloud-boothook' % bh_output_path,
                '%s:text/cloud-config' % cc_output_path)
            utils.execute('genisoimage', '-output', CONF.config_drive_path,
                          '-volid', 'cidata', '-joliet', '-rock',
                          ud_output_path, md_output_path)

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
        self.driver.image_scheme.add_image(
            uri='file://%s' % CONF.config_drive_path,
            target_device=configdrive_device,
            format='iso9660',
            container='raw',
            size=size,
            md5=md5,
        )
