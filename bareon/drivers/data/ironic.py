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

import collections
import itertools
import json

from oslo_config import cfg
from oslo_log import log as logging

from bareon.drivers.data.generic import GenericDataDriver
from bareon import errors
from bareon import objects
from bareon.utils import block_device
from bareon.utils import utils


LOG = logging.getLogger(__name__)

CONF = cfg.CONF


class Ironic(GenericDataDriver):
    loader_partition_size = block_device.SizeUnit(24, 'MiB')
    multiboot_partition_size = block_device.SizeUnit(100, 'MiB')
    _multiboot_claim = None

    data_validation_schema = 'ironic.json'

    # satisfy abstractproperty
    _partition_data = tuple()

    def __init__(self, data):
        super(Ironic, self).__init__(data)
        self.partitions_policy = self.data.get('partitions_policy', 'verify')
        self.storage_claim = StorageParser(
            self.data, self.image_scheme).storage

        self.fs_by_os = self._collect_fs_bindings()
        self.is_multiboot = 1 < len(self.fs_by_os)
        if self.is_multiboot:
            self._multiboot_claim = self._handle_multiboot()
        self.fs_by_mount = self._collect_fs_claims()
        self.boot_on_lvm = self._check_is_boot_on_lvm()
        self._handle_loader()

        self._partition_schema = DeprecatedPartitionSchemeBuilder(
            self.storage_claim, self._multiboot_claim).schema

    def _collect_fs_claims(self):
        result = collections.defaultdict(list)
        for claim in self.storage_claim.items_by_kind(
                objects.block_device.FileSystemMixin, recursion=True):
            result[claim.mount].append(claim)

        return dict(result)

    def _collect_fs_bindings(self):
        result = collections.defaultdict(list)
        for claim in self.storage_claim.items_by_kind(
                objects.block_device.FileSystemMixin, recursion=True):
            for bind in claim.os_binding:
                result[bind].append(claim)

        return dict(result)

    def _check_is_boot_on_lvm(self):
        is_lvm_claim = []
        for mount in ('/', '/boot'):
            for claim in self.fs_by_mount.get(mount, ()):
                if not isinstance(claim, objects.block_device.LVMlv):
                    continue
                is_lvm = True
                break
            else:
                is_lvm = False
            is_lvm_claim.append(is_lvm)

        root_on_lvm, boot_on_lvm = is_lvm_claim
        if not boot_on_lvm:
            if '/boot' not in self.fs_by_mount:
                boot_on_lvm = root_on_lvm

        return boot_on_lvm

    def _handle_multiboot(self):
        disk_claim = self.storage_claim.items_by_kind(
            objects.block_device.Disk)
        try:
            disk_claim = next(disk_claim)
        except StopIteration:
            raise errors.WrongInputDataError(
                'There is no any disk defined. Multiboot feature require '
                'disk to make service boot-partition.')

        size = block_device.SpaceClaim.new_by_sizeunit(
            self.multiboot_partition_size)
        boot_claim = objects.block_device.Partition(
            size, is_boot=True, is_service=True, file_system='ext4')
        disk_claim.add(boot_claim, head=True)

        return boot_claim

    def _handle_loader(self):
        for disk_claim in self.storage_claim.items_by_kind(
                objects.block_device.Disk):
            size = block_device.SpaceClaim.new_by_sizeunit(
                self.loader_partition_size)
            claim = objects.block_device.Partition(
                size, guid_code=0xEF02, is_service=True)
            disk_claim.add(claim, head=True)

    def _get_image_meta(self):
        pass

    def _get_image_scheme(self):
        LOG.debug('--- Preparing image schema ---')
        data = self.data
        image_scheme = objects.ImageScheme()
        image_list = data['images']
        deployment_flags = data.get('image_deploy_flags', {})

        image_scheme.images = [objects.Image(uri=image['image_pull_url'],
                                             target_device=image['target'],
                                             format='bare',
                                             container='raw',
                                             os_id=image['name'],
                                             os_boot=image.get('boot', False),
                                             image_name=image.get('image_name',
                                                                  ''),
                                             image_uuid=image.get('image_uuid',
                                                                  ''),
                                             deployment_flags=deployment_flags)
                               for image in image_list]
        return image_scheme

    def get_os_ids(self):
        return tuple(self.fs_by_os)

    def _get_grub(self):
        LOG.debug('--- Parse grub settings ---')
        grub = objects.Grub()
        kernel_params = self.data.get('deploy_data', {}).get(
            'kernel_params', '')
        # NOTE(lobur): Emulating ipappend 2 to allow early network-based
        # initialization during tenant image boot.
        bootif = utils.parse_kernel_cmdline().get("BOOTIF")
        if bootif:
            kernel_params += " BOOTIF=%s" % bootif

        if kernel_params:
            LOG.debug('Setting initial kernel parameters: %s',
                      kernel_params)
            grub.kernel_params = kernel_params
        return grub

    @classmethod
    def validate_data(cls, data):
        super(Ironic, cls).validate_data(data)

        disks = data['partitions']

        # scheme is not valid if the number of disks is 0
        if not [d for d in disks if d['type'] == 'disk']:
            raise errors.WrongInputDataError(
                'Invalid partition schema: You must specify at least one '
                'disk.')


class StorageParser(object):
    def __init__(self, data, image_scheme):
        self.storage = objects.block_device.StorageSubsystem()
        self.disk_finder = block_device.DeviceFinder()

        operation_systems = self._collect_operation_systems(image_scheme)
        self._existing_os_binding = set(operation_systems)
        self._default_os_binding = operation_systems[:1]

        self.mdfs_by_mount = {}
        self.mddev_by_mount = collections.defaultdict(list)
        self.lvm_pv_reference = collections.defaultdict(list)

        LOG.debug('--- Preparing partition scheme ---')
        LOG.debug('Looping over all disks in provision data')
        try:
            self.claim = self._parse(data)
        except KeyError:
            raise errors.DataSchemaCorruptError()

        self._assemble_lvm_vg()
        self._assemble_mdraid()
        self._validate()

    def _collect_operation_systems(self, image_scheme):
        return [image.os_id for image in image_scheme.images]

    def _parse(self, data):
        for raw in data['partitions']:
            kind = raw['type']
            if kind == 'disk':
                item = self._parse_disk(raw)
            elif kind == 'vg':
                item = self._parse_lvm_vg(raw)
            else:
                raise errors.DataSchemaCorruptError(exc_info=False)

            self.storage.add(item)

    def _assemble_lvm_vg(self):
        vg_by_id = {
            i.idnr: i for i in self.storage.items_by_kind(
                objects.block_device.LVMvg)}

        defined_vg = set(vg_by_id)
        referred_vg = set(self.lvm_pv_reference)
        empty_vg = defined_vg - referred_vg
        orphan_pv = referred_vg - defined_vg

        if empty_vg:
            raise errors.WrongInputDataError(
                'Following LVMvg have no any PV: "{}"'.format(
                    '", "'.join(sorted(empty_vg))))
        if orphan_pv:
            raise errors.WrongInputDataError(
                'Following LVMpv refer to missing VG: "{}"'.format(
                    '", "'.join(sorted(orphan_pv))))

        for idnr in defined_vg:
            vg = vg_by_id[idnr]
            for pv in self.lvm_pv_reference[idnr]:
                vg.add(pv)

    def _assemble_mdraid(self):
        name_template = '/dev/md{:d}'

        idx = itertools.count()
        for mount in sorted(self.mddev_by_mount):
            components = self.mddev_by_mount[mount]

            fields = self.mdfs_by_mount[mount]
            try:
                name = fields.pop('name')
                if not name.startswith('/dev/'):
                    name = '/dev/{}'.format(name)
            except KeyError:
                name = name_template.format(next(idx))

            md = objects.block_device.MDRaid(name, **fields)
            for item in components:
                md.add(item)

            self.storage.add(md)

    def _validate(self):
        for item in self.storage.items:
            if isinstance(item, objects.block_device.Disk):
                self._validate_disk(item)
            elif isinstance(item, objects.block_device.LVMvg):
                self._validate_lvm_vg(item)

    def _validate_disk(self, disk):
        remaining = []
        for item in disk.items:
            if item.size.kind != item.size.KIND_BIGGEST:
                continue
            remaining.append(item)

        if len(remaining) < 2:
            return

        raise errors.WrongInputDataError(
            'Multiple requests on "remaining" space.\n'
            'disk:\n{}\npartitions:\n{}'.format(disk.idnr, '\n'.join(
                repr(x) for x in remaining)))

    def _validate_lvm_vg(self, vg):
        remaining = []
        for item in vg.items_by_kind(objects.block_device.LVMlv):
            if item.size.kind != item.size.KIND_BIGGEST:
                continue
            remaining.append(item)

        if len(remaining) < 2:
            return

        raise errors.WrongInputDataError(
            'Multiple requests on "remaining" space.\n'
            'lvm-vg: {}\nlogical volumes:\n{}'.format(vg.idnr, '\n'.join(
                repr(x) for x in remaining)))

    def _parse_disk(self, data):
        size = self._size(data['size'])
        idnr = self._disk_id(data['id'])
        disk = objects.block_device.Disk(
            idnr, size, **self._get_fields(data, 'name'))

        for raw in data['volumes']:
            kind = raw['type']
            if kind == 'pv':
                item = self._parse_lvm_pv(raw)
            elif kind == 'raid':
                item = self._parse_mdraid_dev(raw)
            elif kind == 'partition':
                item = self._parse_disk_partition(raw)
            elif kind == 'boot':
                item = self._parse_disk_partition(raw)
                item.is_boot = True
            # FIXME(dbogun): unsupported but allowed by data-schema type
            elif kind == 'lvm_meta_pool':
                item = None
            else:
                raise errors.DataSchemaCorruptError(exc_info=False)

            if item is not None:
                disk.add(item)

        return disk

    def _parse_lvm_vg(self, data):
        vg = objects.block_device.LVMvg(
            data['id'], **self._get_fields(
                data, 'label', 'min_size', 'keep_data', '_allocate_size'))
        for raw in data['volumes']:
            item = self._parse_lvm_lv(raw)
            vg.add(item)

        return vg

    def _parse_lvm_pv(self, data):
        size = self._size(data['size'])
        fields = self._get_fields(data, 'keep_data', 'lvm_meta_size')
        if 'lvm_meta_size' in fields:
            fields['lvm_meta_size'] = self._size(fields['lvm_meta_size'])
        pv = objects.block_device.LVMpv(data['vg'], size, **fields)

        self.lvm_pv_reference[pv.vg_idnr].append(pv)

        return pv

    def _parse_mdraid_dev(self, data):
        fields = self._get_filesystem_fields(data, 'name')
        size = fields.pop('size')
        mddev = objects.block_device.MDDev(size)

        mount = fields['mount']
        self.mdfs_by_mount.setdefault(mount, fields)
        self.mddev_by_mount[mount].append(mddev)

        return mddev

    def _parse_disk_partition(self, data):
        fields = self._get_filesystem_fields(data, 'disk_label')
        self._rename_fields(fields, {'disk_label': 'label'})
        if fields.get('file_system') == 'swap':
            fields['guid_code'] = 0x8200
        size = fields.pop('size')
        return objects.block_device.Partition(size, **fields)

    def _parse_lvm_lv(self, data):
        fields = self._get_filesystem_fields(data)
        size = fields.pop('size')
        return objects.block_device.LVMlv(data['name'], size, **fields)

    def _get_filesystem_fields(self, data, *extra):
        fields = self._get_fields(
            data,
            'mount', 'keep_data', 'file_system', 'size', 'images',
            'fstab_options', 'fstab_enabled', *extra)
        self._rename_fields(fields, {
            'fstab_enabled': 'fstab_member',
            'fstab_options': 'mount_options',
            'images': 'os_binding'})

        fields.setdefault('os_binding', self._default_os_binding)
        fields['size'] = self._size(fields['size'])

        if 'mount' in fields:
            if fields['mount'].lower() == 'none':
                fields.pop('mount')
        if 'file_system' in fields:
            fields['file_system'] = fields['file_system'].lower()

        fields['os_binding'] = set(fields['os_binding'])
        missing = fields['os_binding'] - self._existing_os_binding
        if missing:
            # FIXME(dbogun): it must be treated as error
            LOG.warn(
                'Try to claim not existing operating systems: '
                '"{}"\n\n{}'.format(
                    '", "'.join(sorted(missing)),
                    json.dumps(data, indent=2)))
            fields['os_binding'] -= missing

        return fields

    @staticmethod
    def _get_fields(data, *fields):
        result = {}
        for f in fields:
            try:
                result[f] = data[f]
            except KeyError:
                pass
        return result

    @staticmethod
    def _rename_fields(data, mapping):
        for src in mapping:
            try:
                value = data.pop(src)
            except KeyError:
                continue
            data[mapping[src]] = value

    @staticmethod
    def _size(size):
        if size == 'remaining':
            result = block_device.SpaceClaim.new_biggest()
        else:
            result = block_device.SizeUnit.new_by_string(
                size, default_unit='MiB')
            result = block_device.SpaceClaim.new_by_sizeunit(result)
        return result

    def _disk_id(self, idnr):
        idnr = objects.block_device.DevIdnr(idnr['type'], idnr['value'])
        idnr(self.disk_finder)
        return idnr


class DeprecatedPartitionSchemeBuilder(object):
    def __init__(self, storage_claim, multiboot_partition):
        self.storage_claim = storage_claim
        self.multiboot_partition = multiboot_partition

        self.schema = objects.PartitionScheme()

        self._convert()

    def _convert(self):
        for claim in self.storage_claim.items:
            if isinstance(claim, objects.block_device.Disk):
                self._convert_disk(claim)
            elif isinstance(claim, objects.block_device.LVMvg):
                self._convert_lvm_vg(claim)
            elif isinstance(claim, objects.block_device.MDRaid):
                self._convert_mdraid(claim)

    def _convert_disk(self, disk):
        old_disk = self.schema.add_parted(
            name=disk.dev, label='gpt', install_bootloader=True,
            disk_size=self._unpack_size(disk.size).bytes)

        for claim in disk.items:
            args = {}
            if isinstance(claim, objects.block_device.Partition):
                args['keep_data'] = claim.keep_data_flag
            partition = old_disk.add_partition(
                size=self._unpack_size(claim.size).bytes, guid=claim.guid,
                **args)

            if isinstance(claim, objects.block_device.FileSystemMixin):
                self._add_fs(claim, partition.name)

    def _convert_lvm_vg(self, vg):
        self.schema.add_vg(name=vg.idnr)
        for claim in vg.items:
            if isinstance(claim, objects.block_device.LVMpv):
                args = {}
                if claim.meta_size:
                    args['metadatasize'] = self._unpack_size(
                        claim.meta_size).in_unit('MiB').value_int
                self.schema.add_pv(name=claim.vg_idnr, **args)
            elif isinstance(claim, objects.block_device.LVMlv):
                self.schema.add_lv(
                    name=claim.name, vgname=vg.idnr,
                    size=self._unpack_size(claim.size).bytes)
                self._add_fs(claim, claim.name)

    def _convert_mdraid(self, md):
        self.schema.add_md(
            name=md.name, level=md.level, devices=[
                x.expected_dev for x in md.items])
        self._add_fs(md, md.name)

    def _add_fs(self, claim, dev):
        mount = claim.mount
        if claim is self.multiboot_partition:
            mount = 'multiboot'

        if not mount:
            return

        args = {k: v for k, v in (
            ('fstab_options', claim.mount_options),
            ('fs_type', claim.file_system),
            ('os_id', list(claim.os_binding))) if v}

        self.schema.add_fs(
            device=dev, mount=mount, fstab_enabled=claim.fstab_member, **args)

    @staticmethod
    def _unpack_size(size):
        return size.size

    @staticmethod
    def guid_code_to_parted_flags(code):
        flags = set()

        if code == 0xEF02:
            flags.add('bios_grub')
        elif code == 0xEF00:
            flags.add('boot')
        elif code == 0xFD00:
            flags.add('raid')
        elif code == 0x8E00:
            flags.add('lvm')

        return sorted(flags)
