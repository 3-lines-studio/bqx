#!/bin/sh
set -eu

cd "$(dirname "$0")/.."
rm -rf dist
mkdir dist

build() {
    os=$1
    arch=$2
    target=$3
    CGO_ENABLED=0 GOOS="$os" GOARCH="$arch" go build -trimpath -ldflags="-s -w" -o "dist/bqx-$target" .
    sha256sum "dist/bqx-$target" > "dist/bqx-$target.sha256"
}

build linux amd64 linux-x86_64
build linux arm64 linux-aarch64
build darwin arm64 darwin-aarch64

cd dist
sha256sum bqx-linux-x86_64 bqx-linux-aarch64 bqx-darwin-aarch64 > SHA256SUMS
