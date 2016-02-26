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

# A default environment used for image build/sync.

export DIB_SRC=git@github.com:openstack/diskimage-builder.git
export DIB_BRANCH=master

export DIB_UTILS_SRC=git@github.com:openstack/dib-utils.git
export DIB_UTILS_BRANCH=master

export DIB_ELEMENTS_SRC=git@github.com:openstack/bareon-image-elements.git
export DIB_ELEMENTS_BRANCH=master

export FUEL_KEY=https://raw.githubusercontent.com/stackforge/fuel-main/master/bootstrap/ssh/id_rsa
export BUILD_DIR=/tmp/rft_image_build

export GOLDEN_IMAGE_DIR=/tmp/rft_golden_images/
export GOLDEN_IMAGE_SRC=http://images.fuel-infra.org/rft_golden_images/