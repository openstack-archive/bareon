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

import abc

import six


@six.add_metaclass(abc.ABCMeta)
class BaseAction(object):
    """BaseAction


    Validates data and executes the action
    """

    def __init__(self, driver):
        self.driver = driver

    @abc.abstractmethod
    def validate(self):
        """Validate

        Validates that data/objects of data driver satisfied all necessary
        requirements
        """

    @abc.abstractmethod
    def execute(self):
        """Executes the action"""
