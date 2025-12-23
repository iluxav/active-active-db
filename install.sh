#!/bin/sh
set -e

# a2db installer script
# Usage: curl -fsSL https://raw.githubusercontent.com/iluxav/active-active-db/main/install.sh | sh

REPO="iluxav/active-active-db"
BINARY_NAME="a2db"
INSTALL_DIR="/usr/local/bin"

# Detect OS
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
case "$OS" in
    linux)  OS="linux" ;;
    darwin) OS="darwin" ;;
    *)
        echo "Error: Unsupported OS: $OS"
        exit 1
        ;;
esac

# Detect architecture
ARCH=$(uname -m)
case "$ARCH" in
    x86_64|amd64)   ARCH="x86_64" ;;
    aarch64|arm64)  ARCH="aarch64" ;;
    *)
        echo "Error: Unsupported architecture: $ARCH"
        exit 1
        ;;
esac

ASSET_NAME="${BINARY_NAME}-${OS}-${ARCH}.tar.gz"
DOWNLOAD_URL="https://github.com/${REPO}/releases/latest/download/${ASSET_NAME}"

echo "Detected: ${OS}-${ARCH}"
echo "Downloading: ${DOWNLOAD_URL}"

# Create temp directory
TMP_DIR=$(mktemp -d)
trap "rm -rf ${TMP_DIR}" EXIT

# Download and extract
curl -fsSL "$DOWNLOAD_URL" | tar xz -C "$TMP_DIR"

# Install
if [ -w "$INSTALL_DIR" ]; then
    mv "${TMP_DIR}/${BINARY_NAME}" "${INSTALL_DIR}/"
    echo "Installed ${BINARY_NAME} to ${INSTALL_DIR}/${BINARY_NAME}"
else
    echo "Need sudo to install to ${INSTALL_DIR}"
    sudo mv "${TMP_DIR}/${BINARY_NAME}" "${INSTALL_DIR}/"
    echo "Installed ${BINARY_NAME} to ${INSTALL_DIR}/${BINARY_NAME}"
fi

# Verify
if command -v "$BINARY_NAME" >/dev/null 2>&1; then
    echo "Success! Run 'a2db --help' to get started."
else
    echo "Installed to ${INSTALL_DIR}/${BINARY_NAME}"
    echo "Make sure ${INSTALL_DIR} is in your PATH."
fi
