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

import abc
import argparse
import errno
import os

import mock
from oslo_serialization import jsonutils
import pkg_resources
import six

from bareon import errors


class BlockDeviceMock(object):
    def __init__(self, name):
        self._patch = []
        path = pkg_resources.resource_filename('bareon.tests', 'data')

        self.root = os.path.join(path, 'block-device', name)
        with open(os.path.join(self.root, 'meta.json')) as meta:
            self.meta = jsonutils.load(meta)

        self.mock_exec = mock.Mock(side_effect=self)
        self.mock_os_stat = mock.Mock(side_effect=self._os_stat)

        self._exec_map = {
            'sgdisk': SGDiskExecMock(self, self.meta['sgdisk']),
            'file': FileExecMock(self, self.meta['file']),
            'lsblk': LSBlkExecMock(self, self.meta['lsblk'])
        }

    def __enter__(self):
        self._patch = [
            mock.patch('bareon.utils.utils.execute', self.mock_exec),
            mock.patch('os.stat', self.mock_os_stat)
        ]
        for p in self._patch:
            p.start()
        return self

    def __exit__(self, *exc_info):
        for p in self._patch:
            p.stop()
        self._patch[:] = []

    def __call__(self, *cmd, **kwargs):
        prog, args = cmd[0], cmd[1:]
        try:
            exec_mock = self._exec_map[prog]
        except KeyError:
            raise RuntimeError(
                '{!r} can\'t handle call: "{}"'.format(self, '", "'.join(cmd)))
        return exec_mock(args)

    def open(self, path):
        path = os.path.join(self.root, path)
        try:
            fd = open(path, 'r')
        except IOError:
            raise RuntimeError('Missing test-data file: {}'.format(path))
        return fd

    def _os_stat(self, path):
        try:
            meta = self.meta['py-call']['os.stat']
        except KeyError as e:
            raise RuntimeError(
                '{!r}: Invalid meta - missing key {!r}'.format(self, e))
        try:
            entry = meta[path]
        except KeyError:
            raise OSError(
                errno.ENOENT, 'FAKE: No such file or directory',  path)
        return entry


@six.add_metaclass(abc.ABCMeta)
class ExecMockBase(object):
    def __init__(self, manager, meta):
        self.manager = manager
        self.meta = meta
        self.argp = self._make_argp()

    def __call__(self, args):
        try:
            args = self.argp.parse_args(args)
            result = self._handle(args)
        except errors.ProcessExecutionError as e:
            e.stderr = 'mock-exec: {}'.format(e.stderr)
            raise
        return result, ''

    @abc.abstractmethod
    def _handle(self, args):
        pass

    @staticmethod
    @abc.abstractmethod
    def _make_argp():
        pass


class SGDiskExecMock(ExecMockBase):
    def _handle(self, args):
        info = None
        try:
            meta = self.meta[args.dev]
            if args.info:
                info = meta['info'][args.info]
        except KeyError:
            raise errors.ProcessExecutionError(
                exit_code=1,
                stderr='There is no metadata for device '
                       '"{}"'.format(args.dev))

        if args.action == 'print':
            output = self.manager.open(meta['root'])
        elif args.info:
            output = self.manager.open(info)
        else:
            raise errors.ProcessExecutionError(
                exit_code=1,
                stderr='Undetermined command action - {}'.format(args))
        return output.read()

    @staticmethod
    def _make_argp():
        p = _ArgumentParser(prog='mock-sgdisk')
        p.add_argument(
            '--print', dest='action', action='store_const', const='print')
        p.add_argument('--info')
        p.add_argument('dev')
        return p


class FileExecMock(ExecMockBase):
    def _handle(self, args):
        try:
            meta = self.meta[args.target]
        except KeyError:
            raise errors.ProcessExecutionError(
                exit_code=1,
                stderr='There is no mock for file "{}"'.format(args.target))
        return self.manager.open(meta).read()

    @staticmethod
    def _make_argp():
        p = _ArgumentParser(prog='mock-file')
        p.add_argument('--brief', action='store_true')
        p.add_argument('--keep-going', action='store_true')
        p.add_argument('--special-files', action='store_true')
        p.add_argument('target')
        return p


class LSBlkExecMock(FileExecMock):
    @staticmethod
    def _make_argp():
        p = _ArgumentParser(prog='mock-lsblk')
        p.add_argument('--bytes', action='store_true')
        p.add_argument('--list', action='store_true')
        p.add_argument('--noheadings', action='store_true')
        p.add_argument('--output')
        p.add_argument('target')
        return p


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        raise errors.ProcessExecutionError(
            exit_code=1,
            stderr='Invalid call args - {}'.format(message))
