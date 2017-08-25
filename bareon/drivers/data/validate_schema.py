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

import jsonschema.validators

import validate_anyof


def validate_schema(schema, payload):
    cls = jsonschema.validators.validator_for(schema)
    cls.check_schema(schema)
    schema_validator = cls(schema, format_checker=jsonschema.FormatChecker())

    defects = []
    for defect in schema_validator.iter_errors(payload):
        if defect.validator == "anyOf":
            anyof_defects = validate_anyof.ValidateAnyOf(defect,
                                                         schema).defects
            defects.extend(anyof_defects)
        else:
            add_path_to_defect_message(defect.path, defect)
            defects.append(defect)
    return defects


def add_path_to_defect_message(path, defect):
    if path:
        path_string = ':'.join((str(x) for x in list(path)))
        defect.message = '{}:{}'.format(path_string, defect.message)
