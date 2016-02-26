============
Contributing
============
.. include:: ../../CONTRIBUTING.rst


------------------
Functional testing
------------------

Overview
--------

Bareon Agent functional testing performed against a kernel/ramdisk
built on CentOS minimal. By default the image is built as part of tox command,
using DIB. Option to use pre-built images available.
Tests are written using unittest2 and Functional Test Framework
(ramdisk-func-test). The framework itself uses libvirt (python bindings) to
configure network, spawn a slave VM running ramdisk. In future we may add
support for baremetal slaves. Framework resides in a standalone repo.
Functional tests, as well as commonly changed parts of test data (node
templates, etc) are located in bareon tree, so that each
pull request introducing the new functionality to bareon can also carry
corresponding functional tests update.


How to run tests (Devstack / CentOS 7.1 environment)
----------------------------------------------------

- Build the devstack environment
- Install additional dependencies:

.. code-block:: console

    $ sudo yum install yum-utils.noarch

- Disable GPG check at the epel repo:

    ::

        [epel]
        name=Extra Packages for Enterprise Linux 7 - $basearch
        #baseurl=http://download.fedoraproject.org/pub/epel/7/$basearch
        mirrorlist=https://mirrors.fedoraproject.org/metalink?repo=epel-7&arch=$basearch
        failovermethod=priority
        enabled=1
        gpgcheck=0
        gpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-EPEL-7


- Install ramdisk-func-test from source:

.. code-block:: console

    $ cd /opt/stack/
    $ git clone git@github.com:openstack/ramdisk-func-test.git
    $ cd ramdisk-func-test
    $ sudo python setup.py develop

- Configure ramdisk-func-test framework

.. code-block:: console

    # Create config
    $ sudo mkdir /etc/ramdisk-func-test
    $ sudo chown $USER:$USER /etc/ramdisk-func-test
    $ touch /etc/ramdisk-func-test/ramdisk-func-test.conf

    # Open port for the Ironic API stub
    $ sudo iptables -I INPUT -p tcp --dport 8011 -j ACCEPT

    # Configure rsync daemon
    $ python ramdisk-func-test/tools/setup_rsync.py


- Get bareon source:

.. code-block:: console

    $ cd /opt/stack/
    $ git clone git@github.com:openstack/bareon.git

- Run tests

.. code-block:: console

    $ cd /opt/stack/bareon
    # Build image & run all tests
    $ tox
    # Build image & run only functional tests
    $ tox -efunc
    # Run only functional tests without image rebuild (assuming you already
    # have images at /tmp/rft_image_build)
    $ NO_DIB=1 tox -efunc
    # Run only functional tests without syncing golden images with server
    $ NO_SYNC=1 tox -efunc
    # Run a single functional test:
    $ tox -efunc bareon/tests_functional/test_data_retention.py::DataRetentionTestCase::test_clean_policy


How to run tests (Clean CentOS environment, e.g. CI slave)
----------------------------------------------------------

- Provision CI slave
- Install tox, and other dependencies:

.. code-block:: console

    $ sudo pip install tox
    $ sudo yum install yum-utils.noarch
    $ sudo yum install dib-utils # Use newt-provided noarch rpm

- If local KVM is going to be used to run slaves (nested virtualization)

.. note:: Currently this is a required step (no support for remote qemu)

.. code-block:: console

    $ sudo yum install libvirt
    $ sudo yum install libvirt-python
    $ echo 'auth_unix_ro = "none"' | sudo tee -a /etc/libvirt/libvirtd.conf
    $ echo 'auth_unix_rw = "none"' | sudo tee -a /etc/libvirt/libvirtd.conf
    $ /bin/systemctl start libvirtd.service

- Install ramdisk-func-test from source:

.. code-block:: console

    $ cd /opt/stack/
    $ git clone git@github.com:openstack/ramdisk-func-test.git
    $ cd ramdisk-func-test
    # If this job is triggered by pull request to ramdisk-func-test, checkout
    # PR source branch git checkout <A branch passed by trigger>
    # Otherwise use master
    $ sudo python setup.py develop

- Configure ramdisk-func-test framework

.. code-block:: console

    # Create config
    $ sudo mkdir /etc/ramdisk-func-test
    $ sudo chown $USER:$USER /etc/ramdisk-func-test
    $ cp ~/ramdisk-func-test/etc/ramdisk-func-test/ramdisk-func-test.conf.sample \
    /etc/ramdisk-func-test/ramdisk-func-test.conf

    # Open port for the Ironic API stub
    $ sudo iptables -I INPUT -p tcp --dport 8011 -j ACCEPT

    # Configure rsync daemon
    $ cd ramdisk-func-test && sudo python tools/setup_rsync.py

- Get bareon source:

.. code-block:: console

    $ cd /opt/stack/
    $ git clone git@github.com:openstack/bareon.git
    # If this job is triggered by pull request to bareon, checkout PR source branch
    $ git checkout <A branch passed by trigger>
    # Otherwise use master
    $ git checkout newt/kilo

- Configure image build environment If needed (example below). Otherwise default is used.
- Run all tests

.. code-block:: console

    $ cd ~/bareon
    $ tox


Customizing image build environment
-----------------------------------
A default environment file shown below.

.. code-block:: console

    #!/usr/bin/env bash
    export DIB_SRC=git@github.com:openstack/diskimage-builder.git
    export DIB_BRANCH=master

    export DIB_UTILS_SRC=git@github.com:openstack/dib-utils.git
    export DIB_UTILS_BRANCH=master

    export FUEL_KEY=https://raw.githubusercontent.com/stackforge/fuel-main/master/bootstrap/ssh/id_rsa
    export BUILD_DIR=/tmp/rft_image_build

    export GOLDEN_IMAGE_DIR=/tmp/rft_golden_images/
    export GOLDEN_IMAGE_SRC=http://images.fuel-infra.org/rft_golden_images/


You can override these with your own environment. To run tests using a custom
environment:

.. code-block:: console

    $ export BUILD_ENV=/path/to/my_bareon_env.sh


Using pre-built images
----------------------
- Make sure images are at /tmp/rft_image_build and named initramfs and vmlinuz
- Make sure the fuel_key is at /tmp/rft_image_build
- Use the following command to run tests:

.. code-block:: console

    $ cd ~/bareon
    $ NO_DIB=1 tox


Updating golden images
----------------------
According to the https://bugs.launchpad.net/fuel/+bug/1549368 golden images
are hosted at http://images.fuel-infra.org/rft_golden_images/

To update existing golden images you need to put them on two hosts. Make sure
you have the key and proper ssh config.


.. code-block:: console

    $ cat ~/.ssh/config

    Host fuel-infra-images
    HostName seed-*.fuel-infra.org
    User images
    identityfile ~/.ssh/golden_images_key_rsa

    $ cd /tmp/rft_golden_images
    $ rsync -av --progress . images@seed-cz1.fuel-infra.org:/var/www/images/rft_golden_images/ &
    $ rsync -av --progress . images@seed-us1.fuel-infra.org:/var/www/images/rft_golden_images/ &
