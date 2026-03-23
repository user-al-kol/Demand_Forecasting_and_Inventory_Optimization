#!/bin/bash
# =============================================================================
# 01_create_user.sh
# Run this as root or with sudo on a fresh Ubuntu VM.
# Creates user 'alex', sets password, grants sudo rights.
# After this script finishes: log out and SSH back in as alex.
# =============================================================================

set -euo pipefail

# --- Config ------------------------------------------------------------------
NEW_USER="alex"
NEW_PASSWORD="alexf0rd0ck3r"          # Change this to your preferred password
# -----------------------------------------------------------------------------

echo ""
echo "════════════════════════════════════════"
echo "  Step 1 — Creating user: $NEW_USER"
echo "════════════════════════════════════════"

# Create user with home directory and bash shell
if id "$NEW_USER" &>/dev/null; then
    echo "⚠️  User '$NEW_USER' already exists — skipping creation."
else
    useradd -m -s /bin/bash "$NEW_USER"
    echo "✅  User '$NEW_USER' created."
fi

# Set password non-interactively
echo "$NEW_USER:$NEW_PASSWORD" | chpasswd
echo "✅  Password set for '$NEW_USER'."

# Add to sudo group
usermod -aG sudo "$NEW_USER"
echo "✅  '$NEW_USER' added to sudo group."

echo ""
echo "════════════════════════════════════════"
echo "  Done."
echo ""
echo "  Next steps:"
echo "  1. Log out of this session."
echo "  2. SSH back in as $NEW_USER:"
echo "       ssh $NEW_USER@<your-vm-ip>"
echo "  3. Run: bash 02_install_docker.sh"
echo "════════════════════════════════════════"
echo ""
