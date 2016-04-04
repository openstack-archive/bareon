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

import abc

import six


@six.add_metaclass(abc.ABCMeta)
class SimpleDeployDriver(object):
    """Deploy driver API"""

    def __init__(self, data_driver):
        self.driver = data_driver


class BaseDeployDriver(SimpleDeployDriver):

    @abc.abstractmethod
    def do_partitioning(self):
        """Partitions storage devices"""

    @abc.abstractmethod
    def do_configdrive(self):
        """Adds configdrive"""

    @abc.abstractmethod
    def do_copyimage(self):
        """Provisions tenant image"""

    @abc.abstractmethod
    def do_reboot(self):
        """Reboots node"""

    @abc.abstractmethod
    def do_provisioning(self):
        """Provisions node"""

    @abc.abstractmethod
    def do_multiboot_bootloader(self):
        """Install MultiBoot Bootloader"""

    @abc.abstractmethod
    def do_install_os(self, os_id):
        """Generate fstab files"""
