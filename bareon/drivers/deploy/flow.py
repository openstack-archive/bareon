#
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

from bareon.drivers.deploy import base
from bareon.drivers.deploy import mixins
from bareon.openstack.common import log as logging

import stevedore.named

LOG = logging.getLogger(__name__)


class Flow(base.SimpleDeployDriver, mixins.MountableMixin):

    def __init__(self, data_driver):
        super(Flow, self).__init__(data_driver)
        self.ext_mgr = stevedore.named.NamedExtensionManager(
            'bareon.do_actions', names=self.driver.flow,
            invoke_on_load=True, invoke_args=(self.driver, ),
            name_order=True)

    def execute_flow(self):
        for action in self.ext_mgr:
            action.validate()
            action.execute()
