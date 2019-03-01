#
# Copyright 2015 Cray Inc.  All Rights Reserved.
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

from oslo_serialization import jsonutils

from bareon import errors

from validate_schema import validate_schema


def validate(schema_path, payload):
    schema = _load_validator_schema(schema_path)

    defects = validate_schema(schema, payload)
    if defects:
        raise errors.InputDataSchemaValidationError(defects)


def _load_validator_schema(schema_path):
    try:
        with open(schema_path, 'rt') as storage:
            schema = jsonutils.load(storage)
    except IOError as e:
        raise errors.ApplicationDataCorruptError(
            'Can\'t read validation schema "{}": {} {}'.format(
                e.filename, e.errno, e.strerror))
    except (ValueError, TypeError) as e:
        raise errors.ApplicationDataCorruptError(
            'Corrupted validation schema "{}": {}'.format(schema_path, e))

    return schema
