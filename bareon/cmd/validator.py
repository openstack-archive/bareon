#    Copyright 2016 Mirantis, Inc.
#    Copyright 2016 Cray Inc.  All Rights Reserved.
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

import sys

from oslo.config import cfg
from oslo_serialization import jsonutils

from bareon import errors
from bareon.utils import utils
from bareon import version


cli_opts = [
    cfg.StrOpt(
        'data_driver', default='ironic',
        help='Data driver'
    ),
    cfg.StrOpt(
        'input_data_file', positional=True, default='-',
        help='Path to deployment config'
    )
]

CONF = cfg.ConfigOpts()


def worker():
    CONF.register_cli_opts(cli_opts)
    CONF(sys.argv[1:], project='fuel-agent',
         version=version.version_info.release_string())

    stream = None
    try:
        if CONF.input_data_file == '-':
            stream = sys.stdin
        else:
            stream = open(CONF.input_data_file, 'rt')
        data = jsonutils.load(stream)
        stream.close()
    except IOError as e:
        raise OperationFailed('Unable to read input data {!r}: {} - {}'.format(
            stream, e.filename, e))
    except (TypeError, ValueError) as e:
        raise OperationFailed('Unable to decode input data: {}'.format(e))

    cls = utils.get_data_driver(CONF.data_driver)
    try:
        cls.validate_data(data)
    except errors.WrongInputDataError as e:
        message = [
            'Validation failure\n\n',
            e.message]
        raise OperationFailed(''.join(message))


def main():
    try:
        worker()
    except OperationFailed as e:
        sys.stderr.write('{}'.format(e))
        sys.stderr.write('\n')
        return 1


class OperationFailed(Exception):
    pass


if __name__ == '__main__':
    sys.exit(main())
