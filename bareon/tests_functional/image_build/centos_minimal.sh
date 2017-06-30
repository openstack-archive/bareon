#!/usr/bin/env bash
#
# Copyright 2016 Cray Inc.  All Rights Reserved.
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

set -e

if [ -n ${NO_DIB:-""} ] ; then
    echo "================== NO_DIB passed. Skipping image build. =================="
    exit 0
fi

echo "================== Rebuilding deploy images =================="

source ${BUILD_ENV:-"bareon/tests_functional/image_build/centos_minimal_env.sh"}

BAREON_PATH="$PWD"

rm -rf $BUILD_DIR
mkdir $BUILD_DIR
cd $BUILD_DIR

git clone -b $DIB_BRANCH $DIB_SRC
git clone -b $DIB_UTILS_BRANCH $DIB_UTILS_SRC
git clone -b $DIB_ELEMENTS_BRANCH $DIB_ELEMENTS_SRC

# Prepare SSH key
ssh-keygen -N '' -f bareon_key

# Apply changes from https://review.openstack.org/319909
# The problem is still actual for CentOS (https://bugs.launchpad.net/diskimage-builder/+bug/1650582)
sed -i -e 's%mv \(/usr/lib/locale/locale-archive\)%cp \1%' $BUILD_DIR/diskimage-builder/diskimage_builder/elements/yum-minimal/pre-install.d/03-yum-cleanup

export PATH=$BUILD_DIR/diskimage-builder/bin:$BUILD_DIR/dib-utils/bin:$PATH

export BAREON_SRC="file://$BAREON_PATH"
export BAREON_BRANCH="$(cd $BAREON_PATH && git rev-parse --abbrev-ref HEAD)" # Use current branch

export ELEMENTS_PATH="$BUILD_DIR/bareon-image-elements"

export DIB_OFFLINE=1
export DIB_DEBUG_TRACE=1
export DIB_DATA_ROOT="$BUILD_DIR"
#export DIB_BAREON_ROOT_PASSWORD=""
[ -f bareon_key ] && export DIB_BAREON_INJECT_SSH_KEY="$PWD/bareon_key.pub"

disk-image-create -n -t raw -o cent-min centos-minimal centos-bareon

rm -r cent-min.raw
mv cent-min.initramfs initramfs
mv cent-min.vmlinuz vmlinuz

echo "================== DONE Rebuilding deploy images =================="

set +e
