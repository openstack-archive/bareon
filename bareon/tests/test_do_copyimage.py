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

from oslo_config import cfg
import six
import unittest2

from bareon.actions import copyimage
from bareon.drivers.data import nailgun
from bareon import errors
from bareon import objects
from bareon.utils import artifact as au
from bareon.utils import fs as fu
from bareon.utils import hardware as hu
from bareon.utils import utils

if six.PY2:
    import mock
elif six.PY3:
    import unittest.mock as mock

CONF = cfg.CONF


class FakeChain(object):
    processors = []

    def append(self, thing):
        self.processors.append(thing)

    def process(self):
        pass


class TestCopyImageAction(unittest2.TestCase):

    def setUp(self):
        super(TestCopyImageAction, self).setUp()
        self.drv = mock.MagicMock(spec=nailgun.Nailgun)
        self.action = copyimage.CopyImageAction(self.drv)
        self.drv.image_scheme.images = [
            objects.Image('http://fake_uri', '/dev/mapper/os-root', 'ext4',
                          'gzip', size=1234),
            objects.Image('file:///fake_uri', '/tmp/config-drive.img',
                          'iso9660', 'raw', size=123)
        ]

    @mock.patch.object(copyimage.os.path, 'exists')
    @mock.patch.object(hu, 'is_block_device')
    @mock.patch.object(utils, 'calculate_md5')
    @mock.patch('os.path.getsize')
    @mock.patch.object(fu, 'extend_fs')
    @mock.patch.object(au, 'GunzipStream')
    @mock.patch.object(au, 'LocalFile')
    @mock.patch.object(au, 'HttpUrl')
    @mock.patch.object(au, 'Chain')
    @mock.patch.object(utils, 'execute')
    @mock.patch.object(utils, 'render_and_save')
    @mock.patch.object(hu, 'list_block_devices')
    def test_do_copyimage(self, mock_lbd, mock_u_ras, mock_u_e, mock_au_c,
                          mock_au_h, mock_au_l, mock_au_g, mock_fu_ef,
                          mock_get_size, mock_md5, mock_ibd, mock_os_path):
        mock_os_path.return_value = True
        mock_ibd.return_value = True
        mock_au_c.return_value = FakeChain()
        self.action.execute()
        imgs = self.drv.image_scheme.images
        self.assertEqual(2, len(imgs))
        expected_processors_list = []
        for img in imgs[:-1]:
            expected_processors_list += [
                img.uri,
                au.HttpUrl,
                au.GunzipStream,
                img.target_device
            ]
        expected_processors_list += [
            imgs[-1].uri,
            au.LocalFile,
            imgs[-1].target_device
        ]
        self.assertEqual(expected_processors_list,
                         mock_au_c.return_value.processors)
        mock_fu_ef_expected_calls = [
            mock.call('ext4', '/dev/mapper/os-root')]
        self.assertEqual(mock_fu_ef_expected_calls, mock_fu_ef.call_args_list)

    @mock.patch.object(copyimage.os.path, 'exists')
    @mock.patch.object(hu, 'is_block_device')
    @mock.patch.object(utils, 'calculate_md5')
    @mock.patch('os.path.getsize')
    @mock.patch.object(fu, 'extend_fs')
    @mock.patch.object(au, 'GunzipStream')
    @mock.patch.object(au, 'LocalFile')
    @mock.patch.object(au, 'HttpUrl')
    @mock.patch.object(au, 'Chain')
    @mock.patch.object(utils, 'execute')
    @mock.patch.object(utils, 'render_and_save')
    @mock.patch.object(hu, 'list_block_devices')
    def test_do_copyimage_target_doesnt_exist(self, mock_lbd, mock_u_ras,
                                              mock_u_e, mock_au_c, mock_au_h,
                                              mock_au_l, mock_au_g, mock_fu_ef,
                                              mock_get_size, mock_md5,
                                              mock_ibd, mock_os_path):
        mock_os_path.return_value = False
        mock_ibd.return_value = True
        mock_au_c.return_value = FakeChain()
        with self.assertRaisesRegexp(errors.WrongDeviceError,
                                     'TARGET processor .* does not exist'):
            self.action.execute()

    @mock.patch.object(copyimage.os.path, 'exists')
    @mock.patch.object(hu, 'is_block_device')
    @mock.patch.object(utils, 'calculate_md5')
    @mock.patch('os.path.getsize')
    @mock.patch('yaml.load')
    @mock.patch.object(utils, 'init_http_request')
    @mock.patch.object(fu, 'extend_fs')
    @mock.patch.object(au, 'GunzipStream')
    @mock.patch.object(au, 'LocalFile')
    @mock.patch.object(au, 'HttpUrl')
    @mock.patch.object(au, 'Chain')
    @mock.patch.object(utils, 'execute')
    @mock.patch.object(utils, 'render_and_save')
    @mock.patch.object(hu, 'list_block_devices')
    def test_do_copyimage_target_not_block_device(self, mock_lbd, mock_u_ras,
                                                  mock_u_e, mock_au_c,
                                                  mock_au_h, mock_au_l,
                                                  mock_au_g, mock_fu_ef,
                                                  mock_http_req, mock_yaml,
                                                  mock_get_size, mock_md5,
                                                  mock_ibd, mock_os_path):
        mock_os_path.return_value = True
        mock_ibd.return_value = False
        mock_au_c.return_value = FakeChain()
        msg = 'TARGET processor .* is not a block device'
        with self.assertRaisesRegexp(errors.WrongDeviceError, msg):
            self.action.execute()

    @mock.patch.object(copyimage.os.path, 'exists')
    @mock.patch.object(hu, 'is_block_device')
    @mock.patch.object(utils, 'calculate_md5')
    @mock.patch('os.path.getsize')
    @mock.patch('yaml.load')
    @mock.patch.object(utils, 'init_http_request')
    @mock.patch.object(fu, 'extend_fs')
    @mock.patch.object(au, 'GunzipStream')
    @mock.patch.object(au, 'LocalFile')
    @mock.patch.object(au, 'HttpUrl')
    @mock.patch.object(au, 'Chain')
    @mock.patch.object(utils, 'execute')
    @mock.patch.object(utils, 'render_and_save')
    @mock.patch.object(hu, 'list_block_devices')
    def test_do_copyimage_md5_matches(self, mock_lbd, mock_u_ras, mock_u_e,
                                      mock_au_c, mock_au_h, mock_au_l,
                                      mock_au_g, mock_fu_ef, mock_http_req,
                                      mock_yaml, mock_get_size, mock_md5,
                                      mock_ibd, mock_os_path):
        mock_os_path.return_value = True
        mock_ibd.return_value = True
        mock_md5.side_effect = ['really_fakemd5']
        mock_au_c.return_value = FakeChain()
        self.drv.image_scheme.images[0].md5 = 'really_fakemd5'
        self.assertEqual(2, len(self.drv.image_scheme.images))
        self.action.execute()
        expected_md5_calls = [mock.call('/dev/mapper/os-root', 1234)]
        self.assertEqual(expected_md5_calls, mock_md5.call_args_list)

    @mock.patch.object(hu, 'is_block_device')
    @mock.patch.object(copyimage.os.path, 'exists')
    @mock.patch.object(utils, 'calculate_md5')
    @mock.patch('os.path.getsize')
    @mock.patch('yaml.load')
    @mock.patch.object(utils, 'init_http_request')
    @mock.patch.object(fu, 'extend_fs')
    @mock.patch.object(au, 'GunzipStream')
    @mock.patch.object(au, 'LocalFile')
    @mock.patch.object(au, 'HttpUrl')
    @mock.patch.object(au, 'Chain')
    @mock.patch.object(utils, 'execute')
    @mock.patch.object(utils, 'render_and_save')
    @mock.patch.object(hu, 'list_block_devices')
    def test_do_copyimage_md5_mismatch(self, mock_lbd, mock_u_ras, mock_u_e,
                                       mock_au_c, mock_au_h, mock_au_l,
                                       mock_au_g, mock_fu_ef, mock_http_req,
                                       mock_yaml, mock_get_size, mock_md5,
                                       mock_os_path, mock_ibd):
        mock_os_path.return_value = True
        mock_ibd.return_value = True
        mock_md5.side_effect = ['really_fakemd5']
        mock_au_c.return_value = FakeChain()
        self.drv.image_scheme.images[0].size = 1234
        self.drv.image_scheme.images[0].md5 = 'fakemd5'
        self.assertEqual(2, len(self.drv.image_scheme.images))
        self.assertRaises(errors.ImageChecksumMismatchError,
                          self.action.execute)
