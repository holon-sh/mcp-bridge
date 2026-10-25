#!/bin/sh
set -eu

main() {
    case "$(uname -s)" in
        Darwin) os=darwin ;;
        Linux) os=linux ;;
        *) echo 'Supported systems: macOS and Linux.' >&2; exit 1 ;;
    esac
    case "$(uname -m)" in
        x86_64|amd64) arch=amd64 ;;
        arm64|aarch64) arch=arm64 ;;
        *) echo 'Supported architectures: amd64 and arm64.' >&2; exit 1 ;;
    esac
    if command -v sha256sum >/dev/null 2>&1; then
        checksum=sha256sum
    elif command -v shasum >/dev/null 2>&1; then
        checksum='shasum -a 256'
    else
        echo 'Install sha256sum or shasum first.' >&2
        exit 1
    fi

    repo=holon-sh/mcp-bridge
    archive="mcp-bridge_${os}_${arch}.tar.gz"
    directory="$HOME/.local/bin"
    temp=$(mktemp -d)
    trap 'rm -rf "$temp"' EXIT
    trap 'exit 1' HUP INT TERM

    if command -v gh >/dev/null 2>&1 && gh auth status --hostname github.com >/dev/null 2>&1; then
        version=$(gh release view --repo "$repo" --json tagName --jq .tagName)
        gh release download "$version" --repo "$repo" --dir "$temp" \
            --pattern "$archive" --pattern checksums.txt
    else
        url=$(curl -LsSf -o /dev/null -w '%{url_effective}' "https://github.com/$repo/releases/latest")
        version=${url##*/}
        for file in "$archive" checksums.txt; do
            curl -LsSf "https://github.com/$repo/releases/download/$version/$file" -o "$temp/$file"
        done
    fi
    expected=$(awk -v file="$archive" '$2 == file {print $1}' "$temp/checksums.txt")
    actual=$($checksum "$temp/$archive")
    actual=${actual%% *}
    if [ -z "$expected" ] || [ "$actual" != "$expected" ]; then
        echo 'Checksum verification failed; nothing was installed.' >&2
        exit 1
    fi
    tar -xzf "$temp/$archive" -C "$temp" mcp-bridge
    chmod 755 "$temp/mcp-bridge"
    mkdir -p "$directory"
    install -m 755 "$temp/mcp-bridge" "$directory/mcp-bridge"
    echo "Installed mcp-bridge $version to $directory/mcp-bridge"
    case ":$PATH:" in
        *":$directory:"*) ;;
        *) echo "Add $directory to your PATH to run mcp-bridge." ;;
    esac
}

main
