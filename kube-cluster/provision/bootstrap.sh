#!/bin/bash
set -e

echo ">> Updating system packages"
apt-get update -y
apt-get upgrade -y

echo ">> Installing base utilities"
apt-get install -y curl vim net-tools htop lsb-release apt-transport-https ca-certificates gnupg sudo

echo ">> Disabling swap"
swapoff -a
sed -i '/ swap / s/^/#/' /etc/fstab

echo ">> Appending hostname to /etc/hosts"
echo "127.0.1.1 $(hostname)" >> /etc/hosts

echo ">> Enabling password authentication over SSH"
sed -i 's/^#\?PasswordAuthentication.*/PasswordAuthentication yes/' /etc/ssh/sshd_config
systemctl restart ssh

echo ">> Setting timezone to Europe/Belgrade"
timedatectl set-timezone Europe/Belgrade

echo ">> Done"
