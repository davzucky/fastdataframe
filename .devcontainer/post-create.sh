#!/bin/bash
# Post-create setup script for the devcontainer

set -e

echo "Installing uv..."
pip install uv

echo "Syncing project dependencies..."
uv sync --all-extras

echo "Installing global npm packages..."
npm install -g @anthropic-ai/claude-code @fission-ai/openspec

echo "Installing Cursor..."
curl https://cursor.com/install -fsS | bash

echo "Installing prek (pre-commit manager)..."
uv tool install prek

echo "Setting up pre-commit hooks..."
prek install

echo "Post-create setup complete!"
