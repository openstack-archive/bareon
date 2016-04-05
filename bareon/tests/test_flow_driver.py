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

from bareon.actions import base as base_action
from bareon.drivers.deploy import flow

import stevedore

if six.PY2:
    import mock
elif six.PY3:
    import unittest.mock as mock


class TestFlowDriver(unittest2.TestCase):

    @mock.patch.object(flow.Flow, '__init__',
                       return_value=None)
    def test_execute_flow(self, mock_init):
        fake_ext = mock.Mock(spec=base_action.BaseAction)
        fake_ext.name = 'foo'
        self.drv = flow.Flow('fake_data_driver')
        self.drv.ext_mgr = stevedore.NamedExtensionManager.make_test_instance(
            [fake_ext], namespace='TESTING')
        self.drv.execute_flow()
        self.assertEqual(['foo'], self.drv.ext_mgr.names())
        fake_ext.validate.assert_called_once_with()
        fake_ext.execute.assert_called_once_with()

    @mock.patch('stevedore.named.NamedExtensionManager')
    def test_init(self, mock_stevedore):
        fake_data_driver = mock.Mock()
        expected_flow = ['action1', 'action3']
        fake_data_driver.flow = expected_flow
        self.drv = flow.Flow(fake_data_driver)
        mock_stevedore.assert_called_once_with(
            'bareon.do_actions', names=expected_flow,
            invoke_on_load=True, invoke_args=(fake_data_driver,),
            name_order=True)
