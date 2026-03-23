# On the VM, logged in as root or your initial sudo user:
bash 01_create_user.sh

# Log out completely
exit

# SSH back in as alex
ssh alex@<your-vm-ip>

# Run the Docker installer
bash 02_install_docker.sh

# Log out again (required for docker group)
exit

# SSH back in as alex one final time
ssh alex@<your-vm-ip>

# Verify — should run without sudo
docker run hello-world
docker compose version