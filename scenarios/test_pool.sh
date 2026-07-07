#!/bin/bash

dd if=/dev/zero of="/tmp/zfs-test-disk" bs=1M count=512 status=progress
sudo zpool create -f zpool /tmp/zfs-test-disk
sudo zfs create -o mountpoint=none zpool/BACKUP
sudo zfs create -o mountpoint=none zpool/ROOT
sudo zfs create -o mountpoint=none zpool/SRVDATA
sudo zfs create -o mountpoint=none zpool/USERDATA
sudo zfs create -p zpool/ROOT/macos_kX7pM9/srv 
sudo zfs create -p zpool/ROOT/macos_kX7pM9/usr/local
sudo zfs create -p zpool/ROOT/macos_kX7pM9/var/games
sudo zfs create -p zpool/ROOT/macos_kX7pM9/var/lib
sudo zfs create -p zpool/ROOT/macos_kX7pM9/var/log
sudo zfs create -p zpool/ROOT/macos_kX7pM9/var/mail
sudo zfs create -p zpool/ROOT/macos_kX7pM9/var/spool
sudo zfs create -p zpool/ROOT/macos_kX7pM9/var/www
sudo zfs create -p zpool/SRVDATA/lxd_g1ezjj/buckets
sudo zfs create -p zpool/SRVDATA/lxd_g1ezjj/containers
sudo zfs create -p zpool/SRVDATA/lxd_g1ezjj/images
sudo zfs create -p zpool/USERDATA/home_aqqn2l
sudo zfs create -p zpool/USERDATA/root_aqqn2l
