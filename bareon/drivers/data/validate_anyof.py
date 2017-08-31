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

import jsonschema
import jsonschema.exceptions

import validate_schema


class ValidateAnyOf(object):

    def __init__(self, anyof_defect, schema):
        self.anyof = anyof_defect
        self.schema = schema

        self.sub_schemas = self._get_sub_schemas()
        self.defects = []

        if "type" in self.anyof.instance:
            permitted_types = []
            validated = False

            for sub_schema in self.sub_schemas:
                permitted_types.extend(sub_schema
                                       ['properties']['type']['enum'])
                if self._verify_type_valid(sub_schema):
                    self._validate_sub_schema(sub_schema)
                    validated = True

            if not validated:
                invalid_type = self.anyof.instance['type']
                message = " could not be validated, {!r} is not one of {!r}"\
                    .format(invalid_type, permitted_types)
                self._raise_validation_error(message)
        else:
            message = " could not be validated, u'type' is a required property"
            self._raise_validation_error(message)

    def _get_sub_schemas(self):
        """Returns list of sub schemas in anyof defect"""
        sub_schemas = self.schema
        path_to_anyof = list(self.anyof.schema_path)
        for path in path_to_anyof:
            sub_schemas = sub_schemas[path]
        return sub_schemas

    def _verify_type_valid(self, sub_schema):
        """Returns true if type is valid for given schema, false otherwise"""
        defects = validate_schema.validate_schema(sub_schema
                                                  ['properties']['type'],
                                                  self.anyof.instance['type'])
        return False if defects else True

    def _validate_sub_schema(self, sub_schema):
        """Performs validation on sub schemas"""
        for defect in validate_schema.validate_schema(sub_schema,
                                                      self.anyof.instance):
            validate_schema.add_path_to_defect_message(self.anyof.path, defect)
            self.defects.append(defect)

    def _raise_validation_error(self, message):
        """Adds ValidationError to defects with given message"""
        defect = jsonschema.exceptions.ValidationError(message)
        validate_schema.add_path_to_defect_message(self.anyof.path, defect)
        self.defects.append(defect)
