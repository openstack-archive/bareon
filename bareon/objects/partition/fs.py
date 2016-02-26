# -*- coding: utf-8 -*-

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
from bareon.objects import base


class FileSystem(base.Serializable):

    def __init__(self, device, mount=None, fs_type=None, fs_options=None,
                 fs_label=None, keep_data=False, fstab_enabled=True,
                 fstab_options='defaults', os_id=[]):
        self.keep_data = keep_data
        self.device = device
        self.mount = mount
        self.type = fs_type if (fs_type is not None) else 'xfs'
        self.options = fs_options or ''
        self.fstab_options = fstab_options
        self.label = fs_label or ''
        self.fstab_enabled = fstab_enabled
        self.os_id = os_id

    def to_dict(self):
        return {
            'device': self.device,
            'mount': self.mount,
            'fs_type': self.type,
            'fs_options': self.options,
            'fs_label': self.label,
            'keep_data': self.keep_data,
            'fstab_enabled': self.fstab_enabled,
            'fstab_options': self.fstab_options,
            'os_id': self.os_id,
        }
