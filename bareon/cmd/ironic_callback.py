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

import abc
import collections
import inspect
import sys
import time
import traceback

import requests
import six

from bareon.utils import utils


def main():
    """Script informs Ironic about bootstrap completion.

    There are three mandatory parameters in kernel command line.
    Ironic prepares these two:
    ironic_api_url - URL of Ironic API service,
    deployment_id - UUID of the node in Ironic.

    And the last one passed by PXE boot loader:
    BOOTIF - MAC address of the boot interface
    To gete more details about interaction with PXEE boot loader visit
    http://www.syslinux.org/wiki/index.php/SYSLINUX#APPEND_-
    """

    app = App()
    app()


class App(object):
    def __init__(self):
        self.kernel_cli_data = _KernelCLIAdapter()

        self.base_url = six.moves.urllib.parse.urljoin(
            self.kernel_cli_data.api_url,
            'v1/nodes/{}/vendor_passthru/'.format(
                self.kernel_cli_data.node_uuid))
        self.root_url = six.moves.urllib.parse.urljoin(
            self.base_url, 'deploy_steps')

        self.http_session = requests.Session()
        self.http_session.headers['Accept'] = 'application/json'

        self.steps_mapping = _StepMapping()

        self.work_queue = [self.root_url]

    def __call__(self):
        rcode = 1
        try:
            self._do_deploy()
            self._do_complete_notify()
        except ControlledFail as e:
            six.print_('Deployment is incomplete!')
            if e.message:
                six.print_(e.message)
        except Exception:
            error = 'Deployment handler internal error!'
            error = '\n\n'.join((error, traceback.format_exc()))
            six.print_(error)

            self.http_session.post(
                self.root_url, json=self._make_report(error=error))
        else:
            rcode = 0

        return rcode

    def _do_deploy(self):
        while self.work_queue:
            url = self.work_queue[0]
            self._do_step(url)
            self.work_queue.pop(0)

    def _do_step(self, url):
        request_data = self._step_request(url)
        step = self._make_step(request_data)

        report = self._make_report(error=RuntimeError('unhandled exception'))
        try:
            results = step()
        except Exception as e:
            report = self._make_report(step=step, error=e)
            raise ControlledFail()
        else:
            report = self._make_report(step=step, payload=results)
        finally:
            response_data = self.http_session.post(url, json=report)
            response_data = response_data.json()
            response_data = _ResponseDataAdapter(response_data)

        if response_data.url:
            self.work_queue.append(response_data.url)

    def _do_complete_notify(self):
        url = six.moves.urllib.parse.urljoin(self.base_url, 'pass_deploy_info')
        data = {
            'address': self.kernel_cli_data.boot_ip,
            'error_message': 'no errors'}
        self.http_session.post(url, json=data).raise_for_status()

    def _step_request(self, url):
        reply = self.http_session.get(url)
        reply.raise_for_status()

        return _RequestDataAdapter(reply.json())

    def _make_step(self, data):
        try:
            step_cls = self.steps_mapping.name_to_step[data.action]
        except KeyError:
            raise InadequateRequirementError(
                'There is no deployment step "{}"'.format(data.action))
        return step_cls(data.payload)

    def _report_results(self, step, results):
        data = self._make_report(step=step, payload=results)
        self.http_session.post(json=data)

    def _report_failure(self, step, e):
        raise NotImplementedError

    @staticmethod
    def _make_report(step=None, payload=None, error=None):
        name = None
        if step is not None:
            name = step.name

        report = {
            'name': name,
            'status': bool(error is None)}

        if payload is not None:
            report['payload'] = payload
        if error is not None:
            report['status-details'] = str(error)

        return report


class _AbstractAdapter(object):
    def __init__(self, data):
        self._raw = data

    def _extract_fields(self, mapping, is_mandatory=False):
        missing = set()
        for attr, name in mapping:
            try:
                value = self._raw[name]
            except KeyError:
                missing.add(name)
                continue
            setattr(self, attr, value)

        if is_mandatory and missing:
            raise self._make_missing_exception(missing)

    @staticmethod
    def _make_missing_exception(missing):
        if isinstance(missing, KeyError):
            missing = str(missing)
        elif isinstance(missing, six.text_type):
            missing = [missing]
        elif not isinstance(missing, collections.Sequence):
            missing = [missing]

        return ValueError(
            'Mandatory fields is missing: {}'.format(
                ', '.join(sorted(missing))))


class _KernelCLIAdapter(_AbstractAdapter):
    api_url = None
    node_uuid = None
    boot_hw_address = None

    def __init__(self):
        super(_KernelCLIAdapter, self).__init__(utils.parse_kernel_cmdline())

        self._extract_fields({
            'api_url': 'ironic_api_url',
            'node_uuid': 'deployment_id',
            'boot_hw_address': 'BOOTIF'}.items(), is_mandatory=True)

        self.api_url = self.api_url.rstrip('/') + '/'

        # cut '01-' and replace '-' on ':' to make usual mac address format
        self.boot_hw_address = self.boot_hw_address[3:].replace('-', ':')
        self._extract_boot_ip()

    def _extract_boot_ip(self):
        for n in range(10):
            ip = utils.get_interface_ip(self.boot_hw_address)
            if ip is not None:
                break
            time.sleep(10)
        else:
            raise ControlledFail('Cannot find IP address of boot interface.')

        self.boot_ip = ip


class _RequestDataAdapter(_AbstractAdapter):
    action = None
    payload = None

    def __init__(self, data):
        super(_RequestDataAdapter, self).__init__(data)

        self._extract_fields({
            'action': 'name',
            'payload': 'payload'}.items(), is_mandatory=True)


class _ResponseDataAdapter(_AbstractAdapter):
    url = None

    def __init__(self, data):
        super(_ResponseDataAdapter, self).__init__(data)
        self._extract_fields({'url': 'url'}.items(), is_mandatory=True)


class _StepMapping(object):
    def __init__(self):
        self.steps = []

        base_cls = _AbstractTask
        target = sys.modules[__name__]
        for name in dir(target):
            value = getattr(target, name)
            if not inspect.isclass(value):
                continue
            if value is base_cls:
                continue
            if not issubclass(value, base_cls):
                continue

            self.steps.append(value)

        self.name_to_step = {}
        self.step_to_name = {}
        for step in self.steps:
            self.name_to_step[step.name] = step
            self.step_to_name[step] = step.name


@six.add_metaclass(abc.ABCMeta)
class _AbstractTask(_AbstractAdapter):
    @abc.abstractproperty
    def name(self):
        pass

    def __init__(self, payload):
        super(_AbstractTask, self).__init__(payload)

    def __call__(self):
        return self._handle()

    @abc.abstractmethod
    def _handle(self):
        pass


class _InjectSSHKeysStep(_AbstractTask):
    name = 'inject-ssh-keys'

    def __init__(self, payload):
        super(_InjectSSHKeysStep, self).__init__(payload)

        self.user_ssh_keys = {}
        try:
            self._extract_keys_map(self._raw['ssh-keys'])
        except KeyError as e:
            raise self._make_missing_exception(e)

    def _extract_keys_map(self, raw_map):
        for user, keys in raw_map.items():
            if isinstance(keys, collections.Sequence):
                pass
            elif all(isinstance(x, six.text_type) for x in keys):
                pass
            else:
                raise ValueError(
                    'Invalid user\'s SSH key definition: user={!r}, '
                    'keys={!r}'.format(user, keys))
            self.user_ssh_keys[user] = keys

    def _handle(self):
        for login in self.user_ssh_keys:
            user_keys = utils.UsersSSHAuthorizedKeys(login)
            for key in self.user_ssh_keys[login]:
                user_keys.add(key)
            user_keys.sync()


class AbstractError(Exception):
    pass


class InadequateRequirementError(AbstractError):
    pass


class ControlledFail(AbstractError):
    pass
