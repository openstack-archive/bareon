#
# Copyright 2016 Cray Inc.  All Rights Reserved.
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

from bareon.drivers.data import generic
from bareon.utils.partition import MiB
from bareon.utils.partition import TiB


class TestKsDisks(unittest2.TestCase):
    def setUp(self):
        super(TestKsDisks, self).setUp()
        self.driver = _DummyDataDriver({})
        self.driver._partition_data = self.mock_part_data = mock.MagicMock()

    def test_no_partition_data(self):
        self.mock_part_data.return_value = []
        desired = []

        result = self.driver._ks_disks

        self.assertEqual(result, desired)
        self.mock_part_data.assert_called_once_with()

    def test_no_partitions_valid_size(self):
        self.mock_part_data.return_value = [
            {'size': -100, 'type': 'disk'},
            {'size': 0, 'type': 'disk'}
        ]
        desired = []

        result = self.driver._ks_disks

        self.assertEqual(result, desired)
        self.mock_part_data.assert_called_once_with()

    def test_no_partitions_valid_type(self):
        self.mock_part_data.return_value = [
            {'size': 100, 'type': 'vg'},
            {'size': 200, 'type': 'pv'}
        ]
        desired = []

        result = self.driver._ks_disks

        self.assertEqual(result, desired)
        self.mock_part_data.assert_called_once_with()

    def test_valid_data(self):
        self.mock_part_data.return_value = [
            {'size': 100, 'type': 'vg'},
            {'size': 200, 'type': 'disk'}
        ]
        desired = [{'size': 200, 'type': 'disk'}]

        result = self.driver._ks_disks

        self.assertEqual(result, desired)
        self.mock_part_data.assert_called_once_with()


class TestKsVgs(unittest2.TestCase):
    def setUp(self):
        super(TestKsVgs, self).setUp()
        self.driver = _DummyDataDriver({})
        self.driver._partition_data = self.mock_part_data = mock.MagicMock()

    def test_no_partition_data(self):
        self.mock_part_data.return_value = []
        desired = []

        result = self.driver._ks_vgs

        self.assertEqual(result, desired)
        self.mock_part_data.assert_called_once_with()

    def test_no_partitions_valid_type(self):
        self.mock_part_data.return_value = [
            {'type': 'disk'},
            {'type': 'pv'}
        ]
        desired = []

        result = self.driver._ks_vgs

        self.assertEqual(result, desired)
        self.mock_part_data.assert_called_once_with()

    def test_valid_data(self):
        self.mock_part_data.return_value = [
            {'type': 'vg'},
            {'type': 'disk'}
        ]
        desired = [{'type': 'vg'}]

        result = self.driver._ks_vgs

        self.assertEqual(result, desired)
        self.mock_part_data.assert_called_once_with()


class TestSmallKsDisks(unittest2.TestCase):
    def setUp(self):
        super(TestSmallKsDisks, self).setUp()
        self.driver = _DummyDataDriver({})
        self.driver._partition_data = self.mock_part_data = mock.MagicMock()

    def test_no_partition_data(self):
        self.mock_part_data.return_value = []
        desired = []

        result = self.driver._small_ks_disks

        self.assertEqual(result, desired)
        self.mock_part_data.assert_called_once_with()

    def test_no_partitions_valid_size(self):
        self.mock_part_data.return_value = [
            {'size': 3 * TiB, 'type': 'disk'},
            {'size': 5 * TiB, 'type': 'disk'}
        ]
        desired = []

        result = self.driver._small_ks_disks

        self.assertEqual(result, desired)
        self.mock_part_data.assert_called_once_with()

    def test_valid_data(self):
        self.mock_part_data.return_value = [
            {'size': 3 * MiB, 'type': 'vg'},
            {'size': 1 * MiB, 'type': 'disk'}
        ]
        desired = [{'size': 1 * MiB, 'type': 'disk'}]

        result = self.driver._small_ks_disks

        self.assertEqual(result, desired)
        self.mock_part_data.assert_called_once_with()


class TestGetLabel(unittest2.TestCase):
    def setUp(self):
        super(TestGetLabel, self).setUp()
        self.driver = _DummyDataDriver({})

    def test_no_label(self):
        label = None
        desired = ''

        result = self.driver._getlabel(label)

        self.assertEqual(result, desired)

    def test_long_label(self):
        label = 'l' * 100
        desired = ' -L {0} '.format('l' * 12)

        result = self.driver._getlabel(label)

        self.assertEqual(result, desired)

    def test_valid_label(self):
        label = 'label'
        desired = ' -L {0} '.format(label)

        result = self.driver._getlabel(label)

        self.assertEqual(result, desired)


class _DummyDataDriver(generic.GenericDataDriver):
    def _partition_data(self):
        return []

    @classmethod
    def validate_data(cls, payload):
        pass
