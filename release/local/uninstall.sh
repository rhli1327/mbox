#!/usr/bin/env bash

set -e -o pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/common.sh"

echo "Uninstalling mbox..."

if systemctl is-active --quiet mbox 2>/dev/null; then
    echo "Stopping mbox service..."
    sudo systemctl stop mbox
fi

if systemctl is-enabled --quiet mbox 2>/dev/null; then
    echo "Disabling mbox service..."
    sudo systemctl disable mbox
fi

echo "Removing files..."
sudo rm -rf "$INSTALL_DATA_PATH"
sudo rm -rf "$INSTALL_BIN_PATH/$BINARY_NAME"
sudo rm -rf "$INSTALL_CONFIG_PATH"
sudo rm -rf "$SYSTEMD_SERVICE_PATH/mbox.service"

echo "Reloading systemd..."
sudo systemctl daemon-reload

echo ""
echo "Uninstallation complete!"
