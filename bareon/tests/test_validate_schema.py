#
# Copyright 2017 Cray Inc.  All Rights Reserved.
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

import pkg_resources
import unittest

import bareon.drivers.data

from bareon import errors


class TestValidateSchema(unittest.TestCase):
    def setUp(self):
        self.validation_path = pkg_resources.resource_filename(
            'bareon.drivers.data.json_schemes', 'ironic.json')
        self.default_message = "Invalid input data:\n"
        self.deploy_schema = {
            "images": [
                {
                    "name": "centos",
                    "image_pull_url": "centos-7.1.1503.raw",
                    "target": "/"
                }
            ],
            "image_deploy_flags": {
                "rsync_flags": "-a -A -X --timeout 300"
            },
            "partitions": [
                {
                    "id": {
                        "type": "name",
                        "value": "vda"
                    },
                    "size": "15000 MB",
                    "type": "disk",
                    "volumes": [
                        {
                            "file_system": "ext4",
                            "mount": "/",
                            "size": "10000 MB",
                            "type": "partition"
                        },
                        {
                            "size": "remaining",
                            "type": "pv",
                            "vg": "volume_group"
                        }
                    ]
                },
                {
                    "id": "volume_group",
                    "type": "vg",
                    "volumes": [
                        {
                            "file_system": "ext3",
                            "mount": "/home",
                            "name": "home",
                            "size": "3000 MB",
                            "type": "lv"
                        },
                        {
                            "file_system": "ext3",
                            "mount": "/var",
                            "name": "var",
                            "size": "remaining",
                            "type": "lv"
                        }

                    ]
                }
            ],
            "partitions_policy": "clean"
        }

    def test_working(self):
        bareon.drivers.data.validate(self.validation_path,
                                     self.deploy_schema)

    def test_partitions_size_missing(self):
        del self.deploy_schema["partitions"][0]["size"]
        err_message = (
            self.default_message +
            "    [ERROR 1] partitions:0:u'size' is a required property")
        with self.assertRaises(errors.InputDataSchemaValidationError) as err:
            bareon.drivers.data.validate(self.validation_path,
                                         self.deploy_schema)
        self.assertEqual(str(err.exception), err_message)

    def test_partitions_size_not_string(self):
        self.deploy_schema["partitions"][0]["size"] = []
        err_message = (
            self.default_message +
            "    [ERROR 1] partitions:0:size:[] is not of type u'string'")
        with self.assertRaises(errors.InputDataSchemaValidationError) as err:
            bareon.drivers.data.validate(self.validation_path,
                                         self.deploy_schema)
        self.assertEqual(str(err.exception), err_message)

    def test_partitions_type_missing(self):
        del self.deploy_schema["partitions"][0]["type"]
        err_message = (
            self.default_message +
            "    [ERROR 1] partitions:0: could not be validated, "
            "u'type' is a required property")
        with self.assertRaises(errors.InputDataSchemaValidationError) as err:
            bareon.drivers.data.validate(self.validation_path,
                                         self.deploy_schema)
        self.assertEqual(str(err.exception), err_message)

    def test_partitions_type_not_valid(self):
        self.deploy_schema["partitions"][0]["type"] = "invalid"
        err_message = (
            self.default_message +
            "    [ERROR 1] partitions:0: could not be validated, "
            "'invalid' is not one of [u'disk', u'vg']")
        with self.assertRaises(errors.InputDataSchemaValidationError) as err:
            bareon.drivers.data.validate(self.validation_path,
                                         self.deploy_schema)
        self.assertEqual(str(err.exception), err_message)

    def test_volumes_type_missing(self):
        del self.deploy_schema["partitions"][0]["volumes"][0]["type"]
        err_message = (
            self.default_message +
            "    [ERROR 1] partitions:0:volumes:0: could not be validated, "
            "u'type' is a required property")
        with self.assertRaises(errors.InputDataSchemaValidationError) as err:
            bareon.drivers.data.validate(self.validation_path,
                                         self.deploy_schema)
        self.assertEqual(str(err.exception), err_message)

    def test_volumes_type_not_valid(self):
        self.deploy_schema["partitions"][0]["volumes"][0]["type"] = "invalid"
        err_message = (
            self.default_message +
            "    [ERROR 1] partitions:0:volumes:0: could not be validated, "
            "'invalid' is not one of [u'pv', u'raid', u'partition', u'boot', "
            "u'lvm_meta_pool']")
        with self.assertRaises(errors.InputDataSchemaValidationError) as err:
            bareon.drivers.data.validate(self.validation_path,
                                         self.deploy_schema)
        self.assertEqual(str(err.exception), err_message)


if __name__ == '__main__':
    unittest.main()
