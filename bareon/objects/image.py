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
import os

from bareon import errors


class Image(object):
    SUPPORTED_CONTAINERS = ['raw', 'gzip']

    def __init__(self, uri, target_device,
                 format, container, size=None, md5=None, os_id=None,
                 os_boot=False, image_name='', image_uuid='',
                 deployment_flags={}):
        # uri is something like
        # http://host:port/path/to/image.img or
        # file:///tmp/image.img
        self.uri = uri
        self.target_device = target_device
        # this must be one of 'iso9660', 'ext[234]', 'xfs'
        self.format = format
        if container not in self.SUPPORTED_CONTAINERS:
            raise errors.WrongImageDataError(
                'Error while image initialization: '
                'unsupported image container')
        self.container = container
        self.size = size
        self.md5 = md5
        self.img_tmp_file = None
        self.os_id = os_id
        self.os_boot = os_boot
        self.image_name = image_name
        self.image_uuid = image_uuid
        self.deployment_flags = deployment_flags


class ImageScheme(object):
    def __init__(self, images=None):
        self.images = images or []

    def add_image(self, **kwargs):
        self.images.append(Image(**kwargs))

    def get_images_sorted_by_depth(self, os_id=None, reverse=False):
        key = lambda x: x.target_device.rstrip(os.path.sep).count(os.path.sep)
        return sorted(self.get_os_images(), key=key, reverse=reverse)

    def get_os_images(self, os_id=None):
        if os_id:
            return filter(lambda img: os_id in img.os_id, self.images)
        return self.images

    def get_os_root(self, os_id=None):
        images = self.get_os_images(os_id)
        return next((image for image in images if image.target_device == '/'))
