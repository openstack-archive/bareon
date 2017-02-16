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

import mock
import unittest2

from bareon.cmd import ironic_callback


class TestIronicCallbackApp(unittest2.TestCase):
    @mock.patch('bareon.cmd.ironic_callback.App._do_step')
    def test_workflow(self, do_step):
        rcode = self.app()

        self.assertEqual(0, rcode)
        do_step.assert_called_once_with(
            '{}/deploy_steps'.format(self.root_url))
        self.mock_http_session.post.assert_called_once_with(
            '{}/pass_deploy_info'.format(self.root_url),
            json={
                'address': self.mock_get_interface_ip.return_value,
                'error_message': 'no errors'})

    @mock.patch('bareon.cmd.ironic_callback.App._do_step')
    @mock.patch('bareon.cmd.ironic_callback.App._make_report')
    def test_workflow_fail(self, make_report, do_step):
        do_step.side_effect = RuntimeError()
        rcode = self.app()

        self.assertEqual(1, rcode)
        make_report.assert_called_once_with(error=mock.ANY)
        self.mock_http_session.post.assert_called_once_with(
            '{}/deploy_steps'.format(self.root_url),
            json=make_report.return_value)

    @mock.patch('bareon.cmd.ironic_callback.App._do_step')
    def test_workflow_fail_controlled(self, do_step):
        do_step.side_effect = ironic_callback.ControlledFail()
        rcode = self.app()

        self.assertEqual(1, rcode)
        self.assertEqual(0, self.mock_http_session.post.call_count)

    @mock.patch('bareon.cmd.ironic_callback.App._do_step')
    def test_do_deploy(self, do_step):
        do_step.side_effect = _AppUrlAddSide(
            self.app,
            'http://test.local/A',
            'http://test.local/B')
        self.app()

        self.assertEqual([
            mock.call('{}/deploy_steps'.format(self.root_url)),
            mock.call('http://test.local/A'),
            mock.call('http://test.local/B')
        ], do_step.call_args_list)

    @mock.patch('bareon.cmd.ironic_callback._InjectSSHKeysStep._handle')
    def test_step_inject_ssh_key(self, step_handler):
        self.mock_http_session.get.return_value.json.return_value = {
            'name': 'inject-ssh-keys',
            'payload': {
                'ssh-keys': {
                    'root': ['SSH KEY (public)']}}}
        self.mock_http_session.post.return_value.json.return_value = {
            'url': None}

        step_handler.return_value = {'step-results': 'dummy'}
        self.app()

        step_handler.assert_called_once_with()

        self.mock_http_session.get.assert_called_once_with(
            '{}/deploy_steps'.format(self.root_url))
        self.mock_http_session.post.assert_has_calls([
            mock.call(
                '{}/deploy_steps'.format(self.root_url), json={
                    'name': 'inject-ssh-keys',
                    'status': True,
                    'payload': step_handler.return_value})], any_order=True)

    @mock.patch('bareon.cmd.ironic_callback.App._make_step')
    @mock.patch('bareon.cmd.ironic_callback.App._make_report')
    def test_step_fail(self, make_report, make_step):
        self.mock_http_session.get.return_value.json.return_value = {
            'name': 'inject-ssh-keys',
            'payload': {
                'ssh-keys': {
                    'root': ['SSH KEY (public)']}}}
        self.mock_http_session.post.return_value.json.return_value = {
            'url': None}

        error = RuntimeError()

        make_step.return_value.side_effect = error
        self.app()

        make_step.return_value.assert_called_once_with()
        make_report.assert_has_calls([
            mock.call(error=mock.ANY),
            mock.call(step=mock.ANY, error=error)])

    def setUp(self):
        self.mock_parse_kernel_cmdline = mock.Mock()
        self.mock_parse_kernel_cmdline.return_value = {
            'ironic_api_url': 'http://api.ironic.local:6385/',
            'deployment_id': 'ironic-node-uuid',
            'BOOTIF': '01-01-02-03-04-05-06'}

        self.mock_get_interface_ip = mock.Mock()
        self.mock_get_interface_ip.return_value = '127.0.0.2'

        self.mock_http_session = mock.Mock()

        for path, m in (
                ('bareon.utils.utils.'
                 'parse_kernel_cmdline', self.mock_parse_kernel_cmdline),
                ('bareon.utils.utils.'
                 'get_interface_ip', self.mock_get_interface_ip)):
            patch = mock.patch(path, m)
            patch.start()
            self.addCleanup(patch.stop)

        self.app = ironic_callback.App()
        patch = mock.patch.object(
            self.app, 'http_session', self.mock_http_session)
        patch.start()
        self.addCleanup(patch.stop)

        self.root_url = '{}v1/nodes/{}/vendor_passthru'.format(
            self.mock_parse_kernel_cmdline.return_value['ironic_api_url'],
            self.mock_parse_kernel_cmdline.return_value['deployment_id'])


class _AppUrlAddSide(object):
    def __init__(self, app, *urls):
        self.app = app
        self.urls = iter(urls)

    def __call__(self, *args, **kwargs):
        try:
            url = next(self.urls)
        except StopIteration:
            return
        self.app.work_queue.append(url)
