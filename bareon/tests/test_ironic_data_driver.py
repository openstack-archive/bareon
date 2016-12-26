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

from bareon.drivers.data import ironic

MiB = 2 ** 20


class IronicTestAbstract(unittest2.TestCase):
    def setUp(self):
        super(IronicTestAbstract, self).setUp()
        self.driver = self.new_data_driver()

    @staticmethod
    def new_data_driver(payload=None):
        if payload is None:
            payload = {}
        payload.setdefault('partitions', [])

        with mock.patch.object(ironic.Ironic, 'validate_data'):
            driver = ironic.Ironic(payload)

        return driver


class Abstract(unittest2.TestCase):
    dummy_data = {'partitions': []}

    def setUp(self):
        super(Abstract, self).setUp()
        self.data_driver = self._new_data_driver(self.dummy_data)

    @staticmethod
    @mock.patch(
        'bareon.drivers.data.ironic.Ironic._get_image_scheme', mock.Mock())
    @mock.patch(
        'bareon.drivers.data.ironic.Ironic.validate_data', mock.Mock())
    @mock.patch(
        'bareon.drivers.data.ironic.Ironic._collect_fs_bindings',
        mock.Mock(return_value={}))
    @mock.patch(
        'bareon.drivers.data.ironic.Ironic._collect_fs_claims',
        mock.Mock(return_value={}))
    @mock.patch(
        'bareon.drivers.data.ironic.Ironic._handle_loader', mock.Mock())
    @mock.patch(
        'bareon.drivers.data.ironic.StorageParser', mock.Mock())
    @mock.patch(
        'bareon.drivers.data.ironic.DeprecatedPartitionSchemeBuilder',
        mock.Mock())
    def _new_data_driver(data):
        return ironic.Ironic(data)


class TestGetImageSchema(Abstract):
    def test_get_image_scheme(self):
        image_uri = 'test_uri'
        rsync_flags = '-a -X'
        deploy_flags = {'rsync_flags': rsync_flags}
        data = {
            'images': [
                {
                    'image_pull_url': image_uri,
                    'target': '/',
                    'name': 'test'
                }
            ],
            'partitions': [],
            'image_deploy_flags': deploy_flags}

        data_driver = self._new_data_driver(data)
        result = data_driver._get_image_scheme()

        self.assertEqual(len(result.images), 1)

        result_image = result.images[0]
        self.assertEqual(result_image.deployment_flags, deploy_flags)
        self.assertEqual(result_image.uri, image_uri)
