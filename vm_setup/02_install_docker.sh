#!/bin/bash
# =============================================================================
# 02_install_docker.sh
# Run this as 'alex' after logging back in from a fresh SSH session.
# Installs Docker Engine + Docker Compose plugin (official method).
# After this script finishes: log out and SSH back in once more so
# the docker group membership takes effect.
# =============================================================================

set -euo pipefail

echo ""
echo "════════════════════════════════════════"
echo "  Step 1 — System update"
echo "════════════════════════════════════════"
sudo apt-get update -y
sudo apt-get upgrade -y

echo ""
echo "════════════════════════════════════════"
echo "  Step 2 — Installing prerequisites"
echo "════════════════════════════════════════"
sudo apt-get install -y \
    ca-certificates \
    curl \
    gnupg \
    lsb-release

echo ""
echo "════════════════════════════════════════"
echo "  Step 3 — Adding Docker GPG key"
echo "════════════════════════════════════════"
sudo install -m 0755 -d /etc/apt/keyrings

# Remove stale key if it exists (safe to re-run)
sudo rm -f /etc/apt/keyrings/docker.gpg

curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
    sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg
echo "✅  Docker GPG key added."

echo ""
echo "════════════════════════════════════════"
echo "  Step 4 — Adding Docker repository"
echo "════════════════════════════════════════"
echo \
    "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
    https://download.docker.com/linux/ubuntu \
    $(lsb_release -cs) stable" | \
    sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
echo "✅  Docker repository added."

echo ""
echo "════════════════════════════════════════"
echo "  Step 5 — Installing Docker Engine"
echo "════════════════════════════════════════"
sudo apt-get update -y
sudo apt-get install -y \
    docker-ce \
    docker-ce-cli \
    containerd.io \
    docker-buildx-plugin \
    docker-compose-plugin
echo "✅  Docker Engine installed."

echo ""
echo "════════════════════════════════════════"
echo "  Step 6 — Adding $USER to docker group"
echo "════════════════════════════════════════"
sudo usermod -aG docker "$USER"
echo "✅  '$USER' added to docker group."

echo ""
echo "════════════════════════════════════════"
echo "  Step 7 — Enabling Docker on boot"
echo "════════════════════════════════════════"
sudo systemctl enable docker
sudo systemctl start docker
echo "✅  Docker service enabled and started."

echo ""
echo "════════════════════════════════════════"
echo "  Verifying installation..."
echo "════════════════════════════════════════"
docker --version
docker compose version

echo ""
echo "════════════════════════════════════════"
echo "  ✅  Installation complete."
echo ""
echo "  IMPORTANT: The docker group membership"
echo "  does not take effect in this session."
echo ""
echo "  You must log out and SSH back in:"
echo "      exit"
echo "      ssh alex@<your-vm-ip>"
echo ""
echo "  Then verify with:"
echo "      docker run hello-world"
echo "════════════════════════════════════════"
echo ""
