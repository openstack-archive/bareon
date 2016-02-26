# Copyright 2014 Mirantis, Inc.
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

import json
import jsonschema
import os

from bareon import errors
from bareon.openstack.common import log as logging

LOG = logging.getLogger(__name__)


def validate(data, schema_file='nailgun'):
    """Validates a given partition scheme using jsonschema.

    :param scheme: partition scheme to validate
    """
    base_path = os.path.dirname(__file__)
    schemas_path = os.path.join(base_path, 'json_schemes')
    with open(os.path.join(schemas_path, '%s.json' % schema_file)) as file:
        schema = json.load(file)

    try:
        checker = jsonschema.FormatChecker()
        jsonschema.validate(data, schema,
                            format_checker=checker)
    except Exception as exc:
        LOG.exception(exc)
        raise errors.WrongPartitionSchemeError(str(exc))

    # scheme is not valid if the number of disks is 0
    if not [d for d in data if d['type'] == 'disk']:
        raise errors.WrongPartitionSchemeError(
            'Partition scheme seems empty')

    # TODO(lobur): Must be done after unit conversion
    # for space in data:
    #     for volume in space.get('volumes', []):
    #         if volume['size'] > 16777216 and volume.get('mount') == '/':
    #             raise errors.WrongPartitionSchemeError(
    #                 'Root file system must be less than 16T')

    # TODO(kozhukalov): need to have additional logical verifications
    # maybe sizes and format of string values
