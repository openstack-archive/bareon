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
from bareon.objects.bootloader import Grub
from bareon.objects.configdrive import ConfigDriveCommon
from bareon.objects.configdrive import ConfigDriveMcollective
from bareon.objects.configdrive import ConfigDrivePuppet
from bareon.objects.configdrive import ConfigDriveScheme
from bareon.objects.device import Loop
from bareon.objects.image import Image
from bareon.objects.image import ImageScheme
from bareon.objects.operating_system import Centos
from bareon.objects.operating_system import OperatingSystem
from bareon.objects.operating_system import Ubuntu
from bareon.objects.partition.fs import FileSystem
from bareon.objects.partition.lv import LogicalVolume
from bareon.objects.partition.md import MultipleDevice
from bareon.objects.partition.parted import Parted
from bareon.objects.partition.parted import Partition
from bareon.objects.partition.pv import PhysicalVolume
from bareon.objects.partition.scheme import PartitionScheme
from bareon.objects.partition.vg import VolumeGroup
from bareon.objects.repo import DEBRepo
from bareon.objects.repo import Repo
from bareon.objects.repo import RepoProxies


PV = PhysicalVolume
VG = VolumeGroup
LV = LogicalVolume
MD = MultipleDevice
FS = FileSystem


__all__ = [
    'Partition',
    'Parted',
    'PhysicalVolume',
    'PV',
    'VolumeGroup',
    'VG',
    'LogicalVolume',
    'LV',
    'MultipleDevice',
    'MD',
    'FileSystem',
    'FS',
    'PartitionScheme',
    'ConfigDriveCommon',
    'ConfigDrivePuppet',
    'ConfigDriveMcollective',
    'ConfigDriveScheme',
    'Image',
    'ImageScheme',
    'Grub',
    'OperatingSystem',
    'Ubuntu',
    'Centos',
    'Repo',
    'DEBRepo',
    'Loop',
    'RepoProxies'
]
