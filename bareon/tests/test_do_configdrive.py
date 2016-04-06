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
import six
import unittest2

from oslo_config import cfg

from bareon.actions import configdrive
from bareon.drivers.data import nailgun
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
        self.drv.partition_scheme.configdrive_device.return_value = '/dev/sda7'

    def test_do_configdrive(self):
        with mock.patch.multiple(self.action,
                                 _prepare_configdrive_files=mock.DEFAULT,
                                 _make_configdrive_image=mock.DEFAULT,
                                 _add_configdrive_image=mock.DEFAULT) as mocks:
            mocks['_prepare_configdrive_files'].return_value = 'x'
            self.action.execute()
            mocks['_prepare_configdrive_files'].assert_called_once_with()
            mocks['_make_configdrive_image'].assert_called_once_with('x')
            mocks['_add_configdrive_image'].assert_called_once_with()

    @mock.patch.object(configdrive, 'tempfile', autospec=True)
    @mock.patch.object(configdrive, 'os', autospec=True)
    @mock.patch.object(configdrive, 'utils', autospec=True)
    def test_prepare_configdrive_files(self, mock_utils, mock_os, mock_temp):
        mock_os.path.join = os.path.join
        mock_temp.mkdtemp.return_value = '/tmp/qwe'
        ret = self.action._prepare_configdrive_files()
        self.assertEqual(ret, ['/tmp/qwe/openstack'])
        mock_temp.mkdtemp.assert_called_once_with(dir=CONF.tmp_path)
        mock_os.makedirs.assert_called_once_with('/tmp/qwe/openstack/latest')

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
                      ['meta_data_json_pro_fi-le.jinja2',
                       'meta_data_json_pro.jinja2',
                       'meta_data_json_pro_fi.jinja2',
                       'meta_data_json.jinja2'],
                      mock.ANY, '/tmp/qwe/openstack/latest/meta_data.json')]
        self.assertEqual(mock_u_ras_expected_calls,
                         mock_utils.render_and_save.call_args_list)

        mock_utils.execute.assert_called_once_with(
            'write-mime-multipart',
            '--output=/tmp/qwe/openstack/latest/user_data',
            '%s/%s:text/cloud-boothook' % (CONF.tmp_path, 'boothook.txt'),
            '%s/%s:text/cloud-config' % (CONF.tmp_path, 'cloud_config.txt'))

    @mock.patch.object(configdrive, 'tempfile', autospec=True)
    @mock.patch.object(configdrive, 'shutil', autospec=True)
    @mock.patch.object(configdrive, 'fu', autospec=True)
    @mock.patch.object(configdrive, 'os', autospec=True)
    @mock.patch.object(configdrive, 'utils', autospec=True)
    def test_make_configdrive_image(self, mock_utils, mock_os, mock_fu,
                                    mock_shutil, mock_temp):
        mock_utils.execute.side_effect = [(' 795648', ''), None]
        mock_os.path.isdir.side_effect = [True, False]
        mock_os.path.join = os.path.join
        mock_os.path.basename = os.path.basename

        mock_temp.mkdtemp.return_value = '/tmp/mount_point'

        self.action._make_configdrive_image(['/tmp/openstack',
                                             '/tmp/somefile'])

        mock_u_e_calls = [
            mock.call('blockdev', '--getsize64', '/dev/sda7'),
            mock.call('truncate', '--size=795648', CONF.config_drive_path)]

        self.assertEqual(mock_u_e_calls, mock_utils.execute.call_args_list,
                         str(mock_utils.execute.call_args_list))

        mock_fu.make_fs.assert_called_with(fs_type='ext2',
                                           fs_options=' -b 4096 -F ',
                                           fs_label='config-2',
                                           dev=CONF.config_drive_path)
        mock_fu.mount_fs.assert_called_with('ext2',
                                            CONF.config_drive_path,
                                            '/tmp/mount_point')
        mock_fu.umount_fs.assert_called_with('/tmp/mount_point')
        mock_os.rmdir.assert_called_with('/tmp/mount_point')
        mock_shutil.copy2.assert_called_with('/tmp/somefile',
                                             '/tmp/mount_point')
        mock_shutil.copytree.assert_called_with('/tmp/openstack',
                                                '/tmp/mount_point/openstack')

    @mock.patch.object(configdrive, 'fu', autospec=True)
    @mock.patch.object(configdrive, 'os', autospec=True)
    @mock.patch.object(configdrive, 'utils', autospec=True)
    def test_add_configdrive_image(self, mock_utils, mock_os, mock_fu):
        mock_fu.get_fs_type.return_value = 'ext999'
        mock_utils.calculate_md5.return_value = 'fakemd5'
        mock_os.path.getsize.return_value = 123

        self.action._add_configdrive_image()

        self.assertEqual(1, len(self.drv.image_scheme.images))
        cf_drv_img = self.drv.image_scheme.images[0]
        self.assertEqual('file://%s' % CONF.config_drive_path, cf_drv_img.uri)
        self.assertEqual('/dev/sda7', cf_drv_img.target_device)
        self.assertEqual('ext999', cf_drv_img.format)
        self.assertEqual('raw', cf_drv_img.container)
        self.assertEqual('fakemd5', cf_drv_img.md5)
        self.assertEqual(123, cf_drv_img.size)

    @mock.patch.object(configdrive, 'os', autospec=True)
    @mock.patch.object(configdrive, 'utils', autospec=True)
    def test_add_configdrive_image_no_configdrive_device(self, mock_utils,
                                                         mock_os):
        self.drv.partition_scheme.configdrive_device.return_value = None
        mock_utils.calculate_md5.return_value = 'fakemd5'
        mock_os.path.getsize.return_value = 123
        self.assertRaises(errors.WrongPartitionSchemeError,
                          self.action._add_configdrive_image)
