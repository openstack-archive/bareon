# -*- coding: utf-8 -*-

#    Copyright 2015 Mirantis, Inc.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.


class FuelBootstrapException(Exception):
    """Base Exception for Fuel-Bootstrap

    All child classes must be instantiated before raising.
    """
    def __init__(self, *args, **kwargs):
        super(FuelBootstrapException, self).__init__(*args, **kwargs)
        self.message = args[0]


class ActiveImageException(FuelBootstrapException):
    """Should be raised when action can't be permited to active image"""


class ImageAlreadyExists(FuelBootstrapException):
    """Should be raised when image with same uuid already exists"""


class NotImplemented(FuelBootstrapException):
    """Should be raised when some method lacks implementation"""


class IncorrectRepository(FuelBootstrapException):
    """Should be raised when repository can't be parsed"""


class IncorrectImage(FuelBootstrapException):
    """Should be raised when image has incorrect format"""


class ConfigFileNotExists(FuelBootstrapException):
    """Should be raised when default config file is not found"""


class WrongCobblerProfile(FuelBootstrapException):
    """Should be raised when wrong cobbler profile has been chosen"""
