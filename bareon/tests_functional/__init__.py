#
# Copyright 2015 Cray Inc.  All Rights Reserved.
#

import ramdisk_func_test
import pkg_resources
import unittest2


class TestCase(ramdisk_func_test.TestCaseMixin, unittest2.TestCase):
    _rft_template_path = pkg_resources.resource_filename(
        __name__, 'node_templates')
