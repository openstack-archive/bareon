# Copyright 2014 Mirantis, Inc.
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
import signal

from oslo_config import cfg
import six
import unittest2

from bareon.drivers.data import nailgun as nailgun_data
from bareon.drivers.deploy import nailgun as nailgun_deploy
from bareon import objects
from bareon.utils import utils

if six.PY2:
    import mock
elif six.PY3:
    import unittest.mock as mock

CONF = cfg.CONF


class TestImageBuild(unittest2.TestCase):

    @mock.patch('yaml.load')
    @mock.patch.object(utils, 'init_http_request')
    @mock.patch.object(utils, 'get_driver')
    def setUp(self, mock_driver, mock_http, mock_yaml):
        super(self.__class__, self).setUp()
        mock_driver.return_value = nailgun_data.NailgunBuildImage
        image_conf = {
            "image_data": {
                "/": {
                    "container": "gzip",
                    "format": "ext4",
                    "uri": "http:///centos_65_x86_64.img.gz",
                },
            },
            "output": "/var/www/nailgun/targetimages",
            "repos": [
                {
                    "name": "repo",
                    "uri": "http://some",
                    'type': 'deb',
                    'suite': '/',
                    'section': '',
                    'priority': 1001
                }
            ],
            "codename": "trusty"
        }
        self.mgr = nailgun_deploy.Manager(
            nailgun_data.NailgunBuildImage(image_conf))

    @mock.patch.object(nailgun_deploy.Manager, '_set_apt_repos')
    @mock.patch.object(nailgun_deploy, 'bu', autospec=True)
    @mock.patch.object(nailgun_deploy, 'fu', autospec=True)
    @mock.patch.object(nailgun_deploy, 'utils', autospec=True)
    @mock.patch.object(nailgun_deploy, 'os', autospec=True)
    @mock.patch.object(nailgun_deploy.shutil, 'move')
    @mock.patch.object(nailgun_deploy, 'open', create=True,
                       new_callable=mock.mock_open)
    @mock.patch.object(nailgun_deploy.yaml, 'safe_dump')
    @mock.patch.object(nailgun_deploy.Manager, 'mount_target')
    @mock.patch.object(nailgun_deploy.Manager, 'umount_target')
    def test_do_build_image(self, mock_umount_target, mock_mount_target,
                            mock_yaml_dump, mock_open, mock_shutil_move,
                            mock_os, mock_utils,
                            mock_fu, mock_bu, mock_set_apt_repos):

        loops = [objects.Loop(), objects.Loop()]
        self.mgr.driver._image_scheme = objects.ImageScheme([
            objects.Image('file:///fake/img.img.gz', loops[0], 'ext4', 'gzip'),
            objects.Image('file:///fake/img-boot.img.gz',
                          loops[1], 'ext2', 'gzip')])
        self.mgr.driver._partition_scheme = objects.PartitionScheme()
        self.mgr.driver.partition_scheme.add_fs(
            device=loops[0], mount='/', fs_type='ext4')
        self.mgr.driver.partition_scheme.add_fs(
            device=loops[1], mount='/boot', fs_type='ext2')
        self.mgr.driver.metadata_uri = 'file:///fake/img.yaml'
        self.mgr.driver._operating_system = objects.Ubuntu(
            repos=[
                objects.DEBRepo('ubuntu', 'http://fakeubuntu',
                                'trusty', 'fakesection', priority=900),
                objects.DEBRepo('ubuntu_zero', 'http://fakeubuntu_zero',
                                'trusty', 'fakesection', priority=None),
                objects.DEBRepo('mos', 'http://fakemos',
                                'mosX.Y', 'fakesection', priority=1000)],
            packages=['fakepackage1', 'fakepackage2'])
        self.mgr.driver.operating_system.proxies = objects.RepoProxies(
            proxies={'fake': 'fake'},
            direct_repo_addr_list='fake_addr')
        self.mgr.driver.operating_system.minor = 4
        self.mgr.driver.operating_system.major = 14
        mock_os.path.exists.return_value = False
        mock_os.path.join.return_value = '/tmp/imgdir/proc'
        mock_os.path.basename.side_effect = ['img.img.gz', 'img-boot.img.gz']
        mock_bu.create_sparse_tmp_file.side_effect = \
            ['/tmp/img', '/tmp/img-boot']
        mock_bu.attach_file_to_free_loop_device.side_effect = [
            '/dev/loop0', '/dev/loop1']
        mock_bu.mkdtemp_smart.return_value = '/tmp/imgdir'
        getsize_side = [20, 2, 10, 1]
        mock_os.path.getsize.side_effect = getsize_side
        md5_side = ['fakemd5_raw', 'fakemd5_gzip',
                    'fakemd5_raw_boot', 'fakemd5_gzip_boot']
        mock_utils.calculate_md5.side_effect = md5_side
        mock_bu.containerize.side_effect = ['/tmp/img.gz', '/tmp/img-boot.gz']
        mock_bu.stop_chrooted_processes.side_effect = [
            False, True, False, True]
        metadata = {'os': {'name': 'Ubuntu', 'major': 14, 'minor': 4},
                    'packages': self.mgr.driver.operating_system.packages}

        self.mgr.do_build_image()

        self.assertEqual(
            [mock.call('/fake/img.img.gz'),
             mock.call('/fake/img-boot.img.gz')],
            mock_os.path.exists.call_args_list)
        self.assertEqual([mock.call(dir=CONF.image_build_dir,
                                    suffix=CONF.image_build_suffix,
                                    size=CONF.sparse_file_size)] * 2,
                         mock_bu.create_sparse_tmp_file.call_args_list)
        self.assertEqual(
            [mock.call(
                '/tmp/img',
                loop_device_major_number=CONF.loop_device_major_number,
                max_loop_devices_count=CONF.max_loop_devices_count,
                max_attempts=CONF.max_allowed_attempts_attach_image),
             mock.call(
                '/tmp/img-boot',
                loop_device_major_number=CONF.loop_device_major_number,
                max_loop_devices_count=CONF.max_loop_devices_count,
                max_attempts=CONF.max_allowed_attempts_attach_image)
             ],
            mock_bu.attach_file_to_free_loop_device.call_args_list)
        self.assertEqual([mock.call(fs_type='ext4', fs_options='',
                                    fs_label='', dev='/dev/loop0'),
                          mock.call(fs_type='ext2', fs_options='',
                                    fs_label='', dev='/dev/loop1')],
                         mock_fu.make_fs.call_args_list)
        mock_bu.mkdtemp_smart.assert_called_once_with(
            CONF.image_build_dir, CONF.image_build_suffix)
        mock_mount_target.assert_called_once_with(
            '/tmp/imgdir', treat_mtab=False, pseudo=False)
        self.assertEqual([mock.call('/tmp/imgdir')] * 2,
                         mock_bu.suppress_services_start.call_args_list)
        mock_bu.run_debootstrap.assert_called_once_with(
            uri='http://fakeubuntu', suite='trusty', chroot='/tmp/imgdir',
            attempts=CONF.fetch_packages_attempts,
            proxies={'fake': 'fake'}, direct_repo_addr='fake_addr')
        mock_bu.set_apt_get_env.assert_called_once_with()
        mock_bu.pre_apt_get.assert_called_once_with(
            '/tmp/imgdir', allow_unsigned_file=CONF.allow_unsigned_file,
            force_ipv4_file=CONF.force_ipv4_file, proxies={'fake': 'fake'},
            direct_repo_addr='fake_addr')
        driver_os = self.mgr.driver.operating_system
        mock_set_apt_repos.assert_called_with(
            '/tmp/imgdir',
            driver_os.repos,
            proxies=driver_os.proxies.proxies,
            direct_repo_addrs=driver_os.proxies.direct_repo_addr_list
        )

        mock_utils.makedirs_if_not_exists.assert_called_once_with(
            '/tmp/imgdir/proc')
        self.assertEqual([
            mock.call('tune2fs', '-O', '^has_journal', '/dev/loop0'),
            mock.call('tune2fs', '-O', 'has_journal', '/dev/loop0')],
            mock_utils.execute.call_args_list)
        mock_fu.mount_bind.assert_called_once_with('/tmp/imgdir', '/proc')
        mock_bu.populate_basic_dev.assert_called_once_with('/tmp/imgdir')
        mock_bu.run_apt_get.assert_called_once_with(
            '/tmp/imgdir', packages=['fakepackage1', 'fakepackage2'],
            attempts=CONF.fetch_packages_attempts)
        mock_bu.do_post_inst.assert_called_once_with(
            '/tmp/imgdir', allow_unsigned_file=CONF.allow_unsigned_file,
            force_ipv4_file=CONF.force_ipv4_file)

        signal_calls = mock_bu.stop_chrooted_processes.call_args_list
        self.assertEqual(2 * [mock.call('/tmp/imgdir', signal=signal.SIGTERM),
                              mock.call('/tmp/imgdir', signal=signal.SIGKILL)],
                         signal_calls)
        self.assertEqual(
            [mock.call('/tmp/imgdir/proc')] * 2,
            mock_fu.umount_fs.call_args_list)
        self.assertEqual(
            [mock.call(
                '/tmp/imgdir', pseudo=False)] * 2,
            mock_umount_target.call_args_list)
        self.assertEqual(
            [mock.call('/dev/loop0'), mock.call('/dev/loop1')] * 2,
            mock_bu.deattach_loop.call_args_list)
        self.assertEqual([mock.call('/tmp/img'), mock.call('/tmp/img-boot')],
                         mock_bu.shrink_sparse_file.call_args_list)
        self.assertEqual([mock.call('/tmp/img'),
                          mock.call('/fake/img.img.gz'),
                          mock.call('/tmp/img-boot'),
                          mock.call('/fake/img-boot.img.gz')],
                         mock_os.path.getsize.call_args_list)
        self.assertEqual([mock.call('/tmp/img', 20),
                          mock.call('/fake/img.img.gz', 2),
                          mock.call('/tmp/img-boot', 10),
                          mock.call('/fake/img-boot.img.gz', 1)],
                         mock_utils.calculate_md5.call_args_list)
        self.assertEqual([mock.call('/tmp/img', 'gzip',
                                    chunk_size=CONF.data_chunk_size),
                          mock.call('/tmp/img-boot', 'gzip',
                                    chunk_size=CONF.data_chunk_size)],
                         mock_bu.containerize.call_args_list)
        mock_open.assert_called_once_with('/fake/img.yaml', 'wt',
                                          encoding='utf-8')
        self.assertEqual(
            [mock.call('/tmp/img.gz', '/fake/img.img.gz'),
             mock.call('/tmp/img-boot.gz', '/fake/img-boot.img.gz')],
            mock_shutil_move.call_args_list)

        for repo in self.mgr.driver.operating_system.repos:
            metadata.setdefault('repos', []).append({
                'type': 'deb',
                'name': repo.name,
                'uri': repo.uri,
                'suite': repo.suite,
                'section': repo.section,
                'priority': repo.priority,
                'meta': repo.meta})
        metadata['images'] = [
            {
                'raw_md5': md5_side[0],
                'raw_size': getsize_side[0],
                'raw_name': None,
                'container_name':
                os.path.basename(
                    self.mgr.driver.image_scheme.images[0].uri.split(
                        'file://', 1)[1]),
                'container_md5': md5_side[1],
                'container_size': getsize_side[1],
                'container': self.mgr.driver.image_scheme.images[0].container,
                'format': self.mgr.driver.image_scheme.images[0].format
            },
            {
                'raw_md5': md5_side[2],
                'raw_size': getsize_side[2],
                'raw_name': None,
                'container_name':
                os.path.basename(
                    self.mgr.driver.image_scheme.images[1].uri.split(
                        'file://', 1)[1]),
                'container_md5': md5_side[3],
                'container_size': getsize_side[3],
                'container': self.mgr.driver.image_scheme.images[1].container,
                'format': self.mgr.driver.image_scheme.images[1].format
            }
        ]
        mock_yaml_dump.assert_called_once_with(metadata, stream=mock_open())
