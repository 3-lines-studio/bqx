#!/bin/sh
set -eu

cd "$(dirname "$0")/.."
rm -rf dist
mkdir dist

checksum() {
    for file in "$@"; do
        if command -v sha256sum >/dev/null 2>&1; then
            hash=$(sha256sum "$file" | awk '{print $1}')
        elif command -v shasum >/dev/null 2>&1; then
            hash=$(shasum -a 256 "$file" | awk '{print $1}')
        elif command -v openssl >/dev/null 2>&1; then
            hash=$(openssl dgst -sha256 "$file" | awk '{print $NF}')
        else
            echo "package: sha256sum, shasum, or openssl is required" >&2
            return 1
        fi
        printf '%s  %s\n' "$hash" "$file"
    done
}

build() {
    os=$1
    arch=$2
    target=$3
    CGO_ENABLED=0 GOOS="$os" GOARCH="$arch" go build -trimpath -ldflags="-s -w" -o "dist/bqx-$target" .
    checksum "dist/bqx-$target" > "dist/bqx-$target.sha256"
}

build linux amd64 linux-x86_64
build linux arm64 linux-aarch64
build darwin arm64 darwin-aarch64

cd dist
checksum bqx-linux-x86_64 bqx-linux-aarch64 bqx-darwin-aarch64 > SHA256SUMS
