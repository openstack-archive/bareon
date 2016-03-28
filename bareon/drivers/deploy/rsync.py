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

import os

from bareon.drivers.deploy.generic import GenericDeployDriver
from bareon.openstack.common import log as logging
from bareon.utils import utils

LOG = logging.getLogger(__name__)


class Rsync(GenericDeployDriver):
    def do_copyimage(self, os_id):
        os_path = '/tmp/target/'
        with self.mount_target(os_path, os_id, pseudo=False,
                               treat_mtab=False):
            for image in self.driver.image_scheme.get_os_images(os_id):
                target_image_path = os.path.join(os_path,
                                                 image.target_device.strip(
                                                     os.sep))
                LOG.debug('Starting rsync from %s to %s', image.uri,
                          target_image_path)

                rsync_flags = image.deployment_flags.get('rsync_flags',
                                                         '-a -A -X')
                utils.execute('rsync', rsync_flags, image.uri,
                              target_image_path, check_exit_code=[0])
