#!/bin/sh

set -e

script="$0"
user="$1"
key="$2"
ROOT="$3"

if [ -n "$ROOT" ]; then
    mkdir -p "$ROOT/tmp"
    cp "$script" "$ROOT/tmp/$(basename "$0")"
    cp "$key" "$ROOT/tmp/upload-key.pub"
    exec chroot "$ROOT" "/tmp/$(basename "$0")" "$user" \
        /tmp/upload-key.pub
fi

if [ -z "$user" -o -z "$key" ]; then
    echo "Invalid arguments" >&2
    exit 1
fi

user_home="$(eval echo ~"$user")"
user_uid=$(getent passwd "$user" | cut -d: -f1)
user_gid=$(getent group "$user" | cut -d: -f1)

cd "$user_home"

mkdir -p .ssh
chmod 700 .ssh
cat "$key" >> .ssh/authorized_keys
chmod 600 .ssh/authorized_keys

chown -R "$user_uid:$user_gid" .ssh

rm -f "$key"
rm -f "$script"
