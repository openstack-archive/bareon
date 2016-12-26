# Copyright 2015 Mirantis, Inc.
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
import copy
import os

import pkg_resources
import six

import bareon.drivers.data
import bareon.errors


@six.add_metaclass(abc.ABCMeta)
class BaseDataDriver(object):
    """Data driver API

    For example, data validation methods,
    methods for getting object schemes, etc.
    """

    # Can be used by clients outside of bareon package. Client who loads
    # data-driver via stevedore
    exc = bareon.errors

    data_validation_schema = None

    def __init__(self, data):
        self.validate_data(data)
        self.data = copy.deepcopy(data)
        if 'flow' in self.data:
            self.flow = self.data['flow']

    @classmethod
    def validate_data(cls, data):
        root = pkg_resources.resource_filename(
            'bareon.drivers.data', 'json_schemes')
        schema_path = os.path.join(root, cls.data_validation_schema)
        bareon.drivers.data.validate(schema_path, data)


@six.add_metaclass(abc.ABCMeta)
class PartitioningDataDriverMixin(object):

    @abc.abstractproperty
    def partition_scheme(self):
        """Retruns instance of PartionScheme object"""


@six.add_metaclass(abc.ABCMeta)
class ProvisioningDataDriverMixin(object):

    @abc.abstractproperty
    def image_scheme(self):
        """Returns instance of ImageScheme object"""

    @abc.abstractproperty
    def image_meta(self):
        """Returns image_meta dictionary"""

    @abc.abstractproperty
    def operating_system(self):
        """Returns instance of OperatingSystem object"""


@six.add_metaclass(abc.ABCMeta)
class ConfigDriveDataDriverMixin(object):

    @abc.abstractproperty
    def configdrive_scheme(self):
        """Returns instance of ConfigDriveScheme object"""


@six.add_metaclass(abc.ABCMeta)
class GrubBootloaderDataDriverMixin(object):

    @abc.abstractproperty
    def grub(self):
        """Returns instance of Grub object"""


@six.add_metaclass(abc.ABCMeta)
class MultibootDeploymentMixin(object):

    @abc.abstractmethod
    def get_os_ids(self):
        pass

    @abc.abstractmethod
    def _partition_data(self):
        pass
