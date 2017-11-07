Running Bareon on DevStack
==========================


The following notes give an overview of how to setup Devstack (Newton) for use with Bareon.

.. contents:: Contents
      :local:
      :depth: 1

Preparation
-----------

What you will need:

-  A familiarity with `DevStack <http://docs.openstack.org/developer/devstack/>`_
-  A machine with:

   -  Local block storage. In this guide 40GB was used, but this is not definitive.
   -  Hardware virtualization. Configured by default in libvirt.
   -  A large amount of RAM:

      -  Each virtual Ironic node requires >=3GB.
      -  Building bareon images requires >=16GB. Although these can be
         built elsewhere and copied to the host.

.. warning::

    DevStack will make substantial changes to your system during installation. Only run DevStack on servers or virtual machines that are dedicated to this purpose.

Setup Libvirt
-------------

1. Create VM using the Centos minimal image (`<https://www.centos.org/download/>`_).
2. Log into the machine and enable networking

   .. code-block:: console

    vi /etc/sysconfig/network-scripts/ifcfg-eth0

3. Change 'ONBOOT' to 'yes'

  .. code-block:: console

    systemctl restart network

4. Update and install git

  .. code-block:: console

    yum update
    yum install git
    reboot


Setup Openstack with DevStack
-----------------------------

Setting up DevStack and booting a node with Bareon requires the following
steps:

.. contents::
   :local:
   :depth: 1


Setup Stack User
^^^^^^^^^^^^^^^^

1. Login into target host as root user
2. Download devstack sources and create "stack" user

  .. code-block:: console

    useradd -m -d /opt/stack stack
    sudo -u stack -iH

    git config --global user.name "Name"
    git config --global user.email "user@email.com"

    git clone https://github.com/openstack-dev/devstack.git
    cd devstack
    git checkout -b newton origin/stable/newton
    exit

    cd ~stack/devstack
    tools/create-stack-user.sh

3. Switch user to “stack”

  .. code-block:: console

    sudo -u stack -iH


Patch Ironic & Nova Using Bareon-Ironic
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The bareon-ironic repo contains a series of patches which are required by
Bareon but have not yet been merged into Nova and Ironic.

1. Retrieve bareon-ironic code

  .. code-block:: console

    cd ~
    git clone https://git.openstack.org/openstack/bareon-ironic

2. Retrieve nova and ironic code, to apply bareon-ironic patches.

  .. code-block:: console

    # nova
    cd ~
    git clone https://git.openstack.org/openstack/nova
    cd nova
    git checkout -b local/newton origin/stable/newton
    cat ../bareon-ironic/patches/newton/nova/*.patch | git am

    # ironic
    cd ~
    git clone https://git.openstack.org/openstack/ironic
    cd ironic
    git checkout -b local/newton origin/stable/newton
    cat ../bareon-ironic/patches/newton/ironic/*.patch | git am


Configure and Deploy DevStack
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. Create configuration file "local.conf"

  .. code-block:: console

    cd ~/devstack

    cat > local.conf << 'CATEND'
    [[local|localrc]]
    IRONIC_BRANCH=local/newton
    NOVA_BRANCH=local/newton
    disable_service n-net
    enable_service n-api-meta
    enable_service n-novnc
    enable_service n-crt
    enable_service n-cell
    enable_service q-svc
    enable_service q-agt
    enable_service q-dhcp
    enable_service q-l3
    enable_service q-meta
    enable_service s-proxy
    enable_service s-object
    enable_service s-container
    enable_service s-account

    enable_plugin ironic https://github.com/openstack/ironic.git stable/newton
    enable_service ironic
    enable_service ir-api
    enable_service ir-cond
    disable_service tempest
    disable_service heat h-api h-api-cfn h-api-cw h-eng
    disable_service cinder c-api c-vol c-sch c-bak

    ADMIN_PASSWORD=111
    MYSQL_PASSWORD=111
    RABBIT_PASSWORD=111
    SERVICE_PASSWORD=111
    SERVICE_TOKEN=111
    SWIFT_HASH=123qweasdzxcnbvhgfytr654
    SWIFT_TEMPURL_KEY=123qweasdzxcnbvhgfytr654
    SWIFT_ENABLE_TEMPURLS=True
    SWIFT_LOOPBACK_DISK_SIZE=8G
    VERBOSE=True
    LOG_COLOR=True
    VIRT_DRIVER=ironic

    IRONIC_BAREMETAL_BASIC_OPS=True
    IRONIC_VM_COUNT=2
    IRONIC_VM_SSH_PORT=22
    IRONIC_VM_SPECS_RAM=3072
    IRONIC_VM_SPECS_DISK=4
    IRONIC_VM_EPHEMERAL_DISK=0
    IRONIC_ENABLED_DRIVERS=fake,pxe_ssh
    IRONIC_BUILD_DEPLOY_RAMDISK=False
    CATEND

2. Deploy DevStack

  .. code-block:: console

    ./stack.sh

  .. note::

    If ./stack.sh fails for any reason ./unstack.sh will undo the deployment after which ./stack.sh
    can be re-attempted.

Install Bareon-Ironic and Configure Related Settings
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. Install bareon-ironic

  .. code-block:: console

    cd ~/bareon-ironic
    python setup.py bdist_egg
    sudo easy_install dist/bareon_ironic-1.0.1.dev19-py2.7.egg

2.  Patch ironic configuration

  .. code-block:: console

    mkdir -p /opt/stack/data/bareon-ironic/master
    mkdir -p /opt/stack/data/rsync/master
    cd ~/devstack
    (
    source ./inc/ini-config
    iniset /etc/ironic/ironic.conf DEFAULT enabled_drivers 'fake,bare_swift_ssh,bare_rsync_ssh'
    iniset /etc/ironic/ironic.conf glance swift_temp_url_key 12345678900987654321
    iniset /etc/ironic/ironic.conf resources resource_root_path '/opt/stack/data/bareon-ironic'
    iniset /etc/ironic/ironic.conf resources resource_cache_master_path '/opt/stack/data/bareon-ironic/master'
    iniset /etc/ironic/ironic.conf bareon bareon_pxe_append_params 'nofb nomodeset vga=normal console=tty0 console=ttyS0,9600n8'
    iniset /etc/ironic/ironic.conf rsync rsync_root '/opt/stack/data/rsync'
    iniset /etc/ironic/ironic.conf pxe pxe_append_params 'nofb nomodeset vga=normal console=ttyS0 systemd.journald.forward_to_console=yes no_timer_chec'
    )

3. Because rsync can be used during node setup by bareon, we need to alter rsync daemon configuration.

  .. code-block:: console

    sudo sed -i 's/address = 127.0.0.1//' /etc/rsyncd.conf
    (
    echo '
    [ironic_rsync]
    uid = root
    gid = root
    path = /opt/stack/data/rsync/' | sudo tee -a /etc/rsyncd.conf
    )

4. Restart rsync daemon

  .. code-block:: console

    sudo systemctl restart rsyncd

5. Restart ironic services because changes have been made to ironic.conf.

  Join devstack screen session

  .. code-block:: console

    screen -r stack

  Switch to ``ir-cond`` view (``Ctrl+a Shift+"``) and restart ironic conductor. Do so by sending ``Ctrl+c`` to the active process, then running it again (``Up Arrow + Enter``). Perform the same actions for ``ir-api`` and detach (``Ctrl+a d``). For more information see: `<https://www.gnu.org/software/screen/manual/screen.html>`_.


Install Bareon and Build Images
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. Clone and install bareon on host

  .. code-block:: console

    cd ~
    git clone https://git.openstack.org/openstack/bareon
    cd bareon
    sudo pip install .

2. Build bareon images

  .. code-block:: console

    cd ~/bareon
    sudo yum install diskimage-builder
    ./bareon/tests_functional/image_build/centos_minimal.sh

  .. note::

    bareon images will built under /tmp/rft_image_build

3. Build deployment images

  .. code-block:: console

    ./bareon/tests_functional/image_build/sync_golden_images.sh

  .. note::

    deployment images will built under /tmp/rft_golden_images

4. Put bareon SSH key together with other ironic SSH keys

  .. code-block:: console

    cp -a /tmp/rft_image_build/bareon_key* ~/data/ironic/ssh_keys/


Register Bareon Images in OpenStack
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. Initialise OpenStack credentials

  .. code-block:: console

    source ~/devstack/openrc admin demo

2. Upload bareon image, kernel, target image and deployment config into glance

  .. code-block:: console

    export KERNEL=$(eval "$(openstack image create \
      -f shell \
      --disk-format raw --container-format bare \
      --file /tmp/rft_image_build/vmlinuz \
      bareon/kernel.1)"; echo $id)

    export INITRD=$(eval "$(openstack image create \
      -f shell \
      --disk-format raw --container-format bare \
      --file /tmp/rft_image_build/initramfs \
      bareon/initramfs.1)"; echo $id)

    export TARGET_IMAGE=$(eval "$(openstack image create \
      -f shell \
      --disk-format raw --container-format bare \
      --file /tmp/rft_golden_images/centos-7.1.1503.fpa_func_test.raw \
      local/centos-7.1.1503)"; echo $id)

    openstack image create \
      --disk-format raw --container-format bare \
      deploy_config << 'OPENSTACKEND'
    {
        "partitions_policy": "clean",
        "partitions": [
            {
                "type": "disk",
                "id": {
                    "type": "name",
                    "value": "vda"
                },
                "size": "2048 MiB",
                "volumes": [
                    {
                        "type": "partition",
                        "mount": "/",
                        "file_system": "ext4",
                        "size": "1536 MiB"
                    }
                ]
            }
        ]
    }
    OPENSTACKEND

3. Update ironic-node settings of the two devstack created ironic nodes. They should be named node-0 and node-1. You can check it via ``ironic node-list``.

  .. code-block:: console

    for NODE in node-0 node-1; do
      ironic node-update $NODE replace driver=bare_rsync_ssh
      ironic node-update $NODE add \
        driver_info/deploy_kernel=$KERNEL \
        driver_info/deploy_ramdisk=$INITRD \
        driver_info/bareon_username=root \
        driver_info/bareon_key_filename=/opt/stack/data/ironic/ssh_keys/bareon_key
    done

4. Create a new OpenStack keypair

  .. code-block:: console

    mkdir -p ~/auth
    (
    umask 0477
    nova keypair-add bareon-node-access > ~/auth/bareon-node-access
    )

Configure Networking (CentOS Only)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. Relax network security (don't do it on prodution systems).

  .. code-block:: console

    sudo sysctl net.bridge.bridge-nf-call-iptables=0
    sudo iptables -D INPUT -j REJECT --reject-with=icmp-host-prohibited

2. Fix routing

  .. code-block:: console

    sudo ip route add 10.0.0.0/26 via \
      "$(sudo ip netns exec "$(ip netns | grep '^qrouter-' | head -n1)" ip -oneline a | grep 'inet 172.24.4' | sed -e 's:^.*inet ::' -e 's:/.*$::')"

Deploy Nodes Using Bareon
-------------------------

Deploy node
^^^^^^^^^^^

  .. code-block:: console

    nova boot \
      --flavor baremetal \
      --image $TARGET_IMAGE \
      --nic net-name=private \
      --key-name bareon-node-access \
      --meta deploy_config=deploy_config \
      bareon-test

Monitor Deployment (Optional)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. Check when node is being deployed

  .. code-block:: console

    watch -d -n 1 sudo virsh list --all

2. View console output

  .. code-block:: console

    sudo tail -F -n 50 ~/data/ironic/logs/<node name>_console.log

3. SSH into node

  .. code-block:: console

    # Get the ip address of the node
    openstack server list

    ssh centos@<ip address>

  .. note::

    Depending on the image deployed on the node, a key may have to manually specified.
    The image created by ``./bareon/tests_functional/image_build/centos_minimal.sh``
    has a hardcoded public ssh key and so can be accessed as follows:

    .. code-block:: console

      wget -O ~/auth/id_rsa https://raw.githubusercontent.com/openstack/fuel-main/stable/8.0/bootstrap/ssh/id_rsa
      chmod 600 ~/auth/id_rsa

      ssh -i ~/auth/id_rsa centos@<ip address>

Troubleshooting
---------------

Unable to Delete Node
^^^^^^^^^^^^^^^^^^^^^

If it is not possible to reset the node using the Nova/Ironic CLI, then editing the database can be
performed as a last resort

.. code-block:: console

    mysql << 'MYSQLEND'
    use ironic;
    update nodes set provision_state='deploy failed' where id='<IRONIC_NODE_ID>';
    MYSQLEND

    ironic node-set-provision-state $NODE_ID deleted

Hardware Virtualisation
^^^^^^^^^^^^^^^^^^^^^^^

Check that the appropriate kernel modules are loaded for virtualisation.

.. code-block:: console

    modprobe kvm

    # depending on the cpu either:
    modprobe kvm-intel
    modprobe kvm-amd

    # check output
    lsmod | grep kvm
