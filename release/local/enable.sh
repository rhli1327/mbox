#!/usr/bin/env bash

set -e -o pipefail

sudo systemctl enable mbox
sudo systemctl start mbox
sudo journalctl -u mbox --output cat -f
