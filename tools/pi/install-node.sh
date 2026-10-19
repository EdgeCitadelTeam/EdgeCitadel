#!/bin/sh
set -eu
# Per-user runtime; never replaces the operating system Node installation.
base="$1"
mkdir -p "$base"
cd "$base"
version=v24.21.0
archive="node-$version-linux-x64.tar.xz"
if [ ! -x "node-$version-linux-x64/bin/node" ]; then
  curl -fsSLO "https://nodejs.org/dist/$version/$archive"
  curl -fsSLo SHASUMS256.txt "https://nodejs.org/dist/$version/SHASUMS256.txt"
  awk -v name="$archive" '$2 == name' SHASUMS256.txt | sha256sum -c -
  tar -xf "$archive"
fi
export PATH="$base/node-$version-linux-x64/bin:$PATH"
node --version
mkdir -p sdk
npm install --prefix "$base/sdk" --save-exact --no-audit --no-fund @earendil-works/pi-coding-agent@0.87.1
