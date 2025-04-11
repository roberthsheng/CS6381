#!/bin/bash
# Exit immediately if a command exits with a non-zero status.
set -e

echo "Disabling swap..."
sudo swapoff -a

echo "Commenting out swap entries in /etc/fstab..."
sudo sed -i '/ swap / s/^\(.*\)$/#\1/g' /etc/fstab

echo "Loading required kernel modules for containerd..."
sudo tee /etc/modules-load.d/containerd.conf > /dev/null <<EOF
overlay
br_netfilter
EOF

echo "Loading overlay and br_netfilter modules..."
sudo modprobe overlay
sudo modprobe br_netfilter

echo "Setting up sysctl parameters for Kubernetes networking..."
sudo tee /etc/sysctl.d/kubernetes.conf > /dev/null <<EOT
net.bridge.bridge-nf-call-ip6tables = 1
net.bridge.bridge-nf-call-iptables = 1
net.ipv4.ip_forward = 1
EOT

echo "Applying sysctl parameters..."
sudo sysctl --system

echo "Installing required packages..."
sudo apt install -y curl gnupg2 software-properties-common apt-transport-https ca-certificates

echo "Adding Docker's official GPG key..."
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmour -o /etc/apt/trusted.gpg.d/docker.gpg

echo "Adding Docker repository..."
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"

echo "Updating package list..."
sudo apt update

echo "Installing containerd..."
sudo apt install -y containerd.io

echo "Generating default containerd configuration file..."
containerd config default | sudo tee /etc/containerd/config.toml > /dev/null 2>&1

echo "Setting SystemdCgroup to true in containerd config..."
sudo sed -i 's/SystemdCgroup \= false/SystemdCgroup \= true/g' /etc/containerd/config.toml

echo "Restarting containerd..."
sudo systemctl restart containerd

echo "Enabling containerd to start on boot..."
sudo systemctl enable containerd

echo "Setup complete."

