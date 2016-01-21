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

import six
import unittest2

from oslo_config import cfg

from bareon.actions import configdrive
from bareon.drivers import nailgun
from bareon import errors
from bareon import objects

if six.PY2:
    import mock
elif six.PY3:
    import unittest.mock as mock

CONF = cfg.CONF


class TestConfigDriveAction(unittest2.TestCase):

    def setUp(self):
        super(TestConfigDriveAction, self).setUp()
        self.drv = mock.MagicMock(spec=nailgun.Nailgun)
        self.action = configdrive.ConfigDriveAction(self.drv)
        self.drv.configdrive_scheme = objects.ConfigDriveScheme(
            profile='pro_fi-le')
        self.drv.configdrive_scheme.template_data = mock.Mock()
        self.drv.image_scheme = objects.ImageScheme()

    @mock.patch.object(configdrive, 'os', autospec=True)
    @mock.patch.object(configdrive, 'utils', autospec=True)
    def test_do_configdrive(self, mock_utils, mock_os):
        self.drv.partition_scheme.configdrive_device.return_value = '/dev/sda7'
        mock_os.path.getsize.return_value = 123
        mock_os.path.join = lambda x, y: '%s/%s' % (x, y)
        mock_utils.calculate_md5.return_value = 'fakemd5'
        self.assertEqual(0, len(self.drv.image_scheme.images))
        self.action.execute()
        mock_u_ras_expected_calls = [
            mock.call(CONF.nc_template_path,
                      ['cloud_config_pro_fi-le.jinja2',
                       'cloud_config_pro.jinja2',
                       'cloud_config_pro_fi.jinja2',
                       'cloud_config.jinja2'],
                      mock.ANY, '%s/%s' % (CONF.tmp_path, 'cloud_config.txt')),
            mock.call(CONF.nc_template_path,
                      ['boothook_pro_fi-le.jinja2',
                       'boothook_pro.jinja2',
                       'boothook_pro_fi.jinja2',
                       'boothook.jinja2'],
                      mock.ANY, '%s/%s' % (CONF.tmp_path, 'boothook.txt')),
            mock.call(CONF.nc_template_path,
                      ['meta_data_pro_fi-le.jinja2',
                       'meta_data_pro.jinja2',
                       'meta_data_pro_fi.jinja2',
                       'meta_data.jinja2'],
                      mock.ANY, '%s/%s' % (CONF.tmp_path, 'meta-data'))]
        self.assertEqual(mock_u_ras_expected_calls,
                         mock_utils.render_and_save.call_args_list)

        mock_u_e_expected_calls = [
            mock.call('write-mime-multipart',
                      '--output=%s' % ('%s/%s' % (CONF.tmp_path, 'user-data')),
                      '%s:text/cloud-boothook' % ('%s/%s' % (CONF.tmp_path,
                                                             'boothook.txt')),
                      '%s:text/cloud-config' % ('%s/%s' % (CONF.tmp_path,
                                                           'cloud_config.txt'))
                      ),
            mock.call('genisoimage', '-output', CONF.config_drive_path,
                      '-volid', 'cidata', '-joliet', '-rock',
                      '%s/%s' % (CONF.tmp_path, 'user-data'),
                      '%s/%s' % (CONF.tmp_path, 'meta-data'))]
        self.assertEqual(mock_u_e_expected_calls,
                         mock_utils.execute.call_args_list)
        self.assertEqual(1, len(self.drv.image_scheme.images))
        cf_drv_img = self.drv.image_scheme.images[-1]
        self.assertEqual('file://%s' % CONF.config_drive_path, cf_drv_img.uri)
        self.assertEqual('/dev/sda7',
                         self.drv.partition_scheme.configdrive_device())
        self.assertEqual('iso9660', cf_drv_img.format)
        self.assertEqual('raw', cf_drv_img.container)
        self.assertEqual('fakemd5', cf_drv_img.md5)
        self.assertEqual(123, cf_drv_img.size)

    @mock.patch.object(configdrive, 'os', autospec=True)
    @mock.patch.object(configdrive, 'utils', autospec=True)
    def test_do_configdrive_no_configdrive_device(self, mock_utils, mock_os):
        self.drv.partition_scheme.configdrive_device.return_value = None
        self.assertRaises(errors.WrongPartitionSchemeError,
                          self.action.execute)
