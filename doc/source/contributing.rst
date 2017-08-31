Contributing
++++++++++++
.. include:: ../../CONTRIBUTING.rst

Technical Overview
==================

An overview of a typical deployment using Bareon is given below:

.. image:: diagrams/technical_overview.svg

#. Initiate boot via Nova or Heat, passing in a reference to the
   ``deploy_config`` and any ``driver_actions`` which should be performed
#. Request IP from Neutron
#. Call Ironic to begin deployment
#. Configure TFTP server
#. Cache images (deploy kernel & ramdisk, filesystem,
   cloud_default_deploy_config, deploy_config and
   driver_actions) and write provision script for Bareon
#. Update MAC and PXE config
#. Set boot device to PXE
#. Reboot target node
#. Target node gets IP
#. PXE boot image containing Bareon
#. Bareon calls back to tell Ironic that it is ready
#. Ironic SFTPs across the provision script and forwards the rsync server
   port by SSH (in secure mode)
#. Provisioning is triggered by SSH, eg:
   ``provision --data_driver ironic --deploy_driver rsync``
#. Partition and clean local storage, mount partitions, rsync filesystem
   across, write fstab, configure bootloader and unmount partitions
#. Run driver actions over SSH, eg update BIOS, SFTP file across from Swift
#. Set boot device to local disk
#. Reboot the node


Functional testing
==================

Overview
--------

Tests are written using unittest2 and Functional Test Framework
(ramdisk-func-test). The framework uses libvirt (python bindings) to
configure a network, spawn a slave VM and run test commands.
The framework resides in a standalone repo, however the functional tests,
as well as commonly changed parts of test data (node templates, etc) are located
in the bareon repo, so that these are updated when new functionality is added.

How to run tests (Devstack / CentOS 7.1 environment)
----------------------------------------------------

Follow the instructions on Running Bareon on Devstack upto and including
"Install Bareon and build images".

.. code-block:: console

    cd ~/bareon
    sudo yum install -y ansible
    ansible-playbook bareon/tests_functional/ansible/bootstrap_func_tests.yaml
    sudo NO_DIB=1 tox -e func

Customizing image build environment
-----------------------------------
A default environment file could be found at
bareon/tests_functional/image_build/centos_minimal_env.sh

You can override these with your own environment. To run the tests using a custom
environment:

.. code-block:: console

    $ export BUILD_ENV=/path/to/my_bareon_env.sh
