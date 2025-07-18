#!/bin/bash
set -e

echo ">> Updating system packages"
apt-get update -y
apt-get upgrade -y

echo ">> Installing base utilities"
apt-get install -y curl vim net-tools htop lsb-release apt-transport-https ca-certificates gnupg sudo openssh-server

echo ">> Disabling swap"
swapoff -a
sed -i '/ swap / s/^/#/' /etc/fstab

echo ">> Appending hostname to /etc/hosts"
echo "127.0.1.1 $(hostname)" >> /etc/hosts

echo ">> Creating .ssh directory and copying public keys"
mkdir -p /home/vagrant/.ssh
cat /home/vagrant/host_ssh/authorized_keys > /home/vagrant/.ssh/authorized_keys

chmod 700 /home/vagrant/.ssh
chmod 600 /home/vagrant/.ssh/authorized_keys
chown -R vagrant:vagrant /home/vagrant/.ssh

echo ">> Disabling password authentication in SSH"
sed -i 's/^#\?PasswordAuthentication.*/PasswordAuthentication no/' /etc/ssh/sshd_config
sed -i 's/^#\?PubkeyAuthentication.*/PubkeyAuthentication yes/' /etc/ssh/sshd_config

echo ">> Restarting SSH service"
systemctl restart ssh

echo ">> Setting timezone"
timedatectl set-timezone Europe/Belgrade

echo ">> Done"
