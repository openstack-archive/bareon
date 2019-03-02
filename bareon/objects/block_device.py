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

import itertools
import os

from bareon import errors
from bareon.utils import utils


class Default(object):
    def __init__(self, value, produce=False):
        self.value = value
        self.produce = produce
        self._bindings = {}

    def __get__(self, instance, owner):
        if instance is None:
            return self

        name = self._resolve_bind(owner)
        value = self.value
        if self.produce:
            value = value()
        setattr(instance, name, value)

        return value

    def _resolve_bind(self, owner):
        try:
            name = self._bindings[owner]
        except KeyError:
            for attr in dir(owner):
                value = getattr(owner, attr)
                if value is not self:
                    continue
                break
            else:
                raise RuntimeError(
                    '{!r} can\'t resolve bound name, owner {!r}'.format(
                        self, owner))
            self._bindings[owner] = name = attr
        return name


class Abstract(utils.EqualComparisonMixin, object):
    owner = None

    # TODO(dbogun): caller should provide correct field names
    field_to_attr = {}
    allowed_items_type = ()

    def __init__(self, **fields):
        self.field_to_attr = self._merge_inherited_dicts('field_to_attr')
        self.items = []

        cls = type(self)
        unknown = set()
        for name in fields:
            attr = self.field_to_attr.get(name, name)
            try:
                value = getattr(cls, attr)
                if not isinstance(value, Default):
                    raise AttributeError
            except AttributeError:
                unknown.add(name)
                continue
            setattr(self, attr, fields[name])

        if unknown:
            raise ValueError(
                '{!r} unknown fields: "{}"'.format(
                    self, '", "'.join(sorted(unknown))))

        self._validate()

    def __repr__(self):
        fields = vars(self).copy()
        fields.pop('owner', None)
        items = fields.pop('items', [])

        result = ['{cls.__name__}:'.format(cls=type(self))]
        for f in sorted(fields):
            sub = repr(fields[f])
            sub = sub.splitlines()
            if not sub:
                continue
            for indent, line in itertools.izip(
                    itertools.chain(
                        ['{}: '.format(f)],
                        itertools.repeat(' ' * (len(f) + 2))),
                    sub):
                result.append('{}{}'.format(indent, line))

        if items:
            result.append('items:')
            for i in items:
                sub = repr(i)
                sub = sub.splitlines()
                for indent, line in itertools.izip(
                        itertools.chain(['* '], itertools.repeat('  ')),
                        sub):
                    result.append('{}{}'.format(indent, line))

        return '<{}>'.format('\n '.join(result))

    def add(self, item, head=False):
        if not isinstance(item, self.allowed_items_type):
            raise ValueError('Only instances of {} are allowed'.format(
                ', '.join(repr(x) for x in self.allowed_items_type)))

        item.set_ownership(self)
        if head:
            self.items.insert(0, item)
        else:
            self.items.append(item)

    def items_by_kind(self, kind, recursion=False):
        backlog = []
        sub = []
        for i in self.items:
            if isinstance(i, kind):
                sub.append(i)
            if recursion and isinstance(i, Abstract):
                backlog.append(i)
        return itertools.chain(
            sub, *(s.items_by_kind(kind, recursion) for s in backlog))

    def set_ownership(self, owner):
        self.owner = owner

    def _validate(self):
        pass

    def _merge_inherited_dicts(self, attr):
        result = {}
        for cls in reversed(type(self).__mro__):
            try:
                value = getattr(cls, attr)
            except AttributeError:
                continue
            result.update(value)
        return result

    @classmethod
    def _comparable_shape(cls, target):
        # resolve "default" values to eliminate resolved/unresolved difference
        for attr in dir(cls):
            value = getattr(cls, attr)
            if not isinstance(value, Default):
                continue
            getattr(target, attr)

        value = super(Abstract, cls)._comparable_shape(target)

        # To avoid circular loop in recursive comparison
        value['payload'].pop('owner', None)
        return value


class FileSystemMixin(Abstract):
    label = Default(None)
    mount = Default(None)
    mount_options = Default('defaults')
    fstab_member = Default(True)
    file_system = Default(None)
    keep_data_flag = Default(True)
    os_binding = Default(set, produce=True)

    field_to_attr = {
        'keep_data': 'keep_data_flag'}

    def _validate(self):
        super(FileSystemMixin, self)._validate()

        if self.file_system:
            self.file_system = self.file_system.lower()


class BlockDevice(Abstract):
    idnr = Default(None)
    is_service = Default(False)
    guid = Default(None)
    guid_code = Default(None)

    def __init__(self, size, **fields):
        super(BlockDevice, self).__init__(**fields)
        self.size = size

    @property
    def expected_dev(self):
        if self.owner is None:
            raise ValueError(
                'Unable to evaluate "expected_dev" property on unbound object'
                ' {!r}.'.format(self))

        prefix = self.owner.dev
        if prefix[-1:].isdigit():
            prefix += 'p'
        return '{}{}'.format(prefix, self.owner.item.index(self))

    def _validate(self):
        if self.guid_code is None:
            self.guid_code = self._auto_guid_code()

    def _auto_guid_code(self):
        if isinstance(self, MDDev):
            code = 0xfd00
        elif isinstance(self, LVMpv):
            code = 0x8e00
        elif isinstance(self, Partition):
            code = {
                'xfs': 0x8300,
                'ext2': 0x8300,
                'ext3': 0x8300,
                'ext4': 0x8300,
                'fat12': 0x0700,
                'fat16': 0x0700,
                'fat32': 0x0700,
                'vfat': 0x0700,
                'dosfs': 0x0700
            }.get(self.file_system, '0FC63DAF-8483-4772-8E79-3D69D8477DE4')
        else:
            # the default is linux BDP type guid
            code = '0FC63DAF-8483-4772-8E79-3D69D8477DE4'

        return code


class Partition(FileSystemMixin, BlockDevice):
    is_boot = Default(False)


class MDDev(BlockDevice):
    @property
    def expected_dev(self):
        return self.owner.name


class LVMpv(BlockDevice):
    meta_size = Default(None)

    field_to_attr = {
        'lvm_meta_size': 'meta_size'}

    def __init__(self, vg_idnr, size, **fields):
        super(LVMpv, self).__init__(size, **fields)
        self.vg_idnr = vg_idnr


class LVMlv(FileSystemMixin, BlockDevice):
    def __init__(self, name, size, **fields):
        BlockDevice.__init__(self, size, **fields)
        self.name = name

    @property
    def dev(self):
        if self.owner is None:
            raise ValueError(
                'Unable to evaluate "dev" on unbound object '
                '{!r}.'.format(self))
        return os.path.join('/dev', self.owner.idnr, self.name)

    @property
    def expected_dev(self):
        return self.name


class LVMvg(Abstract):
    label = Default(None)
    min_size = Default(None)  # FIXME(dbogun): unused
    keep_data_flag = Default(None)  # FIXME(dbogun): how it should be used?
    _allocate_size = Default(None)  # FIXME(dbogun): what is this?

    allowed_items_type = (LVMlv, LVMpv)
    field_to_attr = {
        'keep_data': 'keep_data_flag'}

    def __init__(self, idnr, **fields):
        super(LVMvg, self).__init__(**fields)
        self.idnr = idnr


class MDRaid(FileSystemMixin, Abstract):
    level = Default('raid1')

    allowed_items_type = (MDDev, )

    def __init__(self, name, **fields):
        super(MDRaid, self).__init__(**fields)
        self.name = name


class Disk(BlockDevice):
    bootable = None

    name = Default(None)

    allowed_items_type = (Partition, MDDev, LVMpv)

    def __init__(self, idnr, size, **fields):
        super(Disk, self).__init__(size, **fields)
        self.idnr = idnr

    def add(self, item, head=False):
        super(Disk, self).add(item, head=head)

        if isinstance(item, Partition):
            if item.is_boot:
                if self.bootable is not None:
                    raise errors.WrongInputDataError(
                        'Multiple bootable partitions on {}'.format(self.idnr))
                self.bootable = item

    @property
    def expected_dev(self):
        return self.dev

    @property
    def dev(self):
        return self.idnr.dev

    @property
    def dev_info(self):
        return self.idnr.info


class DevIdnr(Abstract):
    info = None
    dev = None

    def __init__(self, kind, needle):
        super(DevIdnr, self).__init__()
        self.kind = kind
        self.needle = needle

    def __call__(self, dev_lookup):
        self.info = dev_lookup(self.kind, self.needle)
        self.dev = self.info['uspec']['DEVNAME']
        return self.dev, self.info

    def __str__(self):
        value = 'block-device: type={0.kind}, value={0.value}'.format(self)
        if self.dev:
            value = '{} => {}'.format(value, self.dev)
        return '<{}>'.format(value)


class StorageSubsystem(Abstract):
    allowed_items_type = (Disk, LVMvg, MDRaid)


class Exports(object):
    StorageSubsystem = StorageSubsystem
    FileSystemMixin = FileSystemMixin
    BlockDevice = BlockDevice
    DevIdnr = DevIdnr
    Disk = Disk
    LVMlv = LVMlv
    LVMpv = LVMpv
    LVMvg = LVMvg
    MDDev = MDDev
    MDRaid = MDRaid
    Partition = Partition
