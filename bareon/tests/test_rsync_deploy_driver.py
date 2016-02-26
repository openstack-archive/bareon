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

import mock
import unittest2

from collections import namedtuple

from bareon.drivers.deploy import rsync


class TestDoCopyimage(unittest2.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestDoCopyimage, self).__init__(*args, **kwargs)
        self.mock_data = mock.MagicMock()
        self.driver = rsync.Rsync(self.mock_data)
        self.mock_mount = self.driver._mount_target = mock.MagicMock()
        self.mock_umount = self.driver._umount_target = mock.MagicMock()
        self.mock_grub = self.mock_data.grub

    @mock.patch('bareon.utils.utils.execute')
    def test_success(self, mock_execute):
        img = namedtuple('fs', 'uri deployment_flags target_device')
        chroot = '/tmp/target/'
        os_id = 'test'
        image = img(uri='uri', deployment_flags={'rsync_flags': 'r_flags'},
                    target_device='/')
        self.mock_data.image_scheme.get_os_images.return_value = (image,)

        result = self.driver.do_copyimage(os_id)

        self.assertEqual(result, None)
        self.mock_mount(chroot, pseudo=False, treat_mtab=False)
        self.mock_umount(chroot, pseudo=False)
        mock_execute.assert_called_once_with('rsync', 'r_flags',
                                             image.uri, chroot,
                                             check_exit_code=[0])
