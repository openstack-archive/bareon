#    Copyright 2014 Mirantis, Inc.
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

from oslo_config import cfg
import six
import yaml

from bareon.openstack.common import log as logging
from bareon.utils import utils
from bareon import version

cli_opts = [
    cfg.StrOpt(
        'input_data_file',
        default='/tmp/provision.json',
        help='Input data file'
    ),
    cfg.StrOpt(
        'input_data',
        default='',
        help='Input data (json string)'
    ),
    cfg.StrOpt(
        'data_driver',
        default='nailgun',
        help='Data driver'
    ),
    cfg.StrOpt(
        'deploy_driver',
        default='nailgun',
        help='Deploy driver'
    ),
    cfg.StrOpt(
        'image_build_dir',
        default='/tmp',
        help='Directory where the image is supposed to be built',
    ),
    cfg.StrOpt(
        'config_drive_path',
        default='/tmp/config-drive.img',
        help='Path where to store generated config drive image',
    ),
]

CONF = cfg.CONF
CONF.register_cli_opts(cli_opts)


def list_opts():
    """Returns a list of oslo.config options available in the library.

    The returned list includes all oslo.config options which may be registered
    at runtime by the library.

    Each element of the list is a tuple. The first element is the name of the
    group under which the list of elements in the second element will be
    registered. A group name of None corresponds to the [DEFAULT] group in
    config files.

    The purpose of this is to allow tools like the Oslo sample config file
    generator (oslo-config-generator) to discover the options exposed to users
    by this library.

    :returns: a list of (group_name, opts) tuples
    """
    return [(None, (cli_opts))]


def provision():
    main(['do_provisioning'])


def partition():
    main(['do_partitioning'])


def copyimage():
    main(['do_copyimage'])


def configdrive():
    main(['do_configdrive'])


def bootloader():
    main(['do_bootloader'])


def build_image():
    main(['do_build_image'])


def mkbootstrap():
    main(['do_mkbootstrap'])


def print_err(line):
    sys.stderr.write(six.text_type(line))
    sys.stderr.write('\n')


def handle_exception(exc):
    LOG = logging.getLogger(__name__)
    LOG.exception(exc)
    print_err('Unexpected error')
    print_err(exc)
    sys.exit(-1)


def main(actions=None):
    CONF(sys.argv[1:], project='bareon',
         version=version.version_info.release_string())

    logging.setup('bareon')
    LOG = logging.getLogger(__name__)

    try:
        if CONF.input_data:
            data = yaml.safe_load(CONF.input_data)
        else:
            with open(CONF.input_data_file) as f:
                data = yaml.safe_load(f)
        LOG.debug('Input data: %s', data)

        data_driver_class = utils.get_data_driver(CONF.data_driver)
        data_driver = data_driver_class(data)

        deploy_driver_class = utils.get_deploy_driver(CONF.deploy_driver)
        deploy_driver = deploy_driver_class(data_driver)

        if actions:
            for action in actions:
                getattr(deploy_driver, action)()
    except Exception as exc:
        handle_exception(exc)


if __name__ == '__main__':
    main()
