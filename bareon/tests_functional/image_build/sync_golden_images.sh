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

if [ -n ${NO_SYNC:-""} ] ; then
    echo " ================== NO_SYNC passed. Skipping the rsync of the golden images. =================="
    exit 0
fi

source ${BUILD_ENV:-"bareon/tests_functional/image_build/centos_minimal_env.sh"}

# NOTE(lobur): This is not actually syncing images (-nc). If image has changed
# it won't catch that. So to get the new image you need to manually clean the
# GOLDEN_IMAGE_DIR or remove that particular image before running the script.
echo "================== Getting golden images from server =================="
wget -r -P $GOLDEN_IMAGE_DIR -nc -nH --cut-dirs=2 --no-parent --reject "index.html*" $GOLDEN_IMAGE_SRC
echo "================== DONE Getting golden images from server =================="

set +e
