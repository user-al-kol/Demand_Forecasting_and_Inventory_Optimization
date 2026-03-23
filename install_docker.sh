#!/bin/bash

set -e

echo "🟦 Ενημέρωση συστήματος..."
sudo apt update

echo "🟦 Εγκατάσταση απαραίτητων πακέτων..."
sudo apt install -y ca-certificates curl gnupg

echo "🟦 Προσθήκη Docker GPG key..."
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

echo "🟦 Προσθήκη Docker repository..."
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | \
sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

echo "🟦 Εγκατάσταση Docker Engine + Compose..."
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

echo "🟦 Προσθήκη του χρήστη στο docker group..."
sudo usermod -aG docker $USER

echo "🟩 Ολοκληρώθηκε η εγκατάσταση Docker!"
echo "ℹ️  Κάνε logout/login ή τρέξε τώρα:  newgrp docker"
echo "ℹ️  Για να δοκιμάσεις:  docker run hello-world"
