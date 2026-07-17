#!/bin/sh
# polymigrate installer.
#
#   curl -fsSL https://raw.githubusercontent.com/rahadiangg/polymigrate/master/install.sh | sh
#
# Environment overrides:
#   POLY_VERSION      install a specific tag (e.g. v0.1.0). Default: latest release.
#   POLY_INSTALL_DIR  install directory. Default: /usr/local/bin (falls back to
#                     $HOME/.local/bin if not writable and not root).
set -eu

REPO="rahadiangg/polymigrate"
BINARY="poly"
ARCHIVE_PREFIX="polymigrate"

info()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
warn()  { printf '\033[1;33mwarning:\033[0m %s\n' "$*" >&2; }
fail()  { printf '\033[1;31merror:\033[0m %s\n' "$*" >&2; exit 1; }

need() { command -v "$1" >/dev/null 2>&1 || fail "required command not found: $1"; }
need uname
need tar

# Prefer curl, fall back to wget.
if command -v curl >/dev/null 2>&1; then
  http_get() { curl -fsSL "$1"; }
  http_dl()  { curl -fsSL -o "$2" "$1"; }
elif command -v wget >/dev/null 2>&1; then
  http_get() { wget -qO- "$1"; }
  http_dl()  { wget -qO "$2" "$1"; }
else
  fail "need either curl or wget"
fi

# --- detect platform -------------------------------------------------------
os="$(uname -s)"
case "$os" in
  Linux)  os="linux" ;;
  Darwin) os="darwin" ;;
  *) fail "unsupported OS: $os (Windows: download the .zip from the releases page)" ;;
esac

arch="$(uname -m)"
case "$arch" in
  x86_64|amd64)   arch="amd64" ;;
  aarch64|arm64)  arch="arm64" ;;
  *) fail "unsupported architecture: $arch" ;;
esac

# --- resolve version -------------------------------------------------------
version="${POLY_VERSION:-}"
if [ -z "$version" ]; then
  info "resolving latest release..."
  version="$(http_get "https://api.github.com/repos/${REPO}/releases/latest" \
    | grep '"tag_name"' | head -n1 | cut -d'"' -f4)"
  [ -n "$version" ] || fail "could not determine latest release tag (set POLY_VERSION to install a specific one)"
fi

asset="${ARCHIVE_PREFIX}_${os}_${arch}.tar.gz"
base="https://github.com/${REPO}/releases/download/${version}"
info "installing ${BINARY} ${version} (${os}/${arch})"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT INT TERM

# --- download + verify -----------------------------------------------------
http_dl "${base}/${asset}" "${tmp}/${asset}" || fail "download failed: ${base}/${asset}"

if http_dl "${base}/checksums.txt" "${tmp}/checksums.txt" 2>/dev/null; then
  sum=""
  if command -v sha256sum >/dev/null 2>&1; then
    sum="$(sha256sum "${tmp}/${asset}" | awk '{print $1}')"
  elif command -v shasum >/dev/null 2>&1; then
    sum="$(shasum -a 256 "${tmp}/${asset}" | awk '{print $1}')"
  fi
  if [ -n "$sum" ]; then
    want="$(grep " ${asset}\$" "${tmp}/checksums.txt" | awk '{print $1}')"
    [ "$sum" = "$want" ] || fail "checksum mismatch for ${asset} (got ${sum}, want ${want})"
    info "checksum verified"
  else
    warn "no sha256 tool found; skipping checksum verification"
  fi
else
  warn "checksums.txt not found; skipping checksum verification"
fi

tar -xzf "${tmp}/${asset}" -C "$tmp"
[ -f "${tmp}/${BINARY}" ] || fail "archive did not contain expected binary '${BINARY}'"
chmod +x "${tmp}/${BINARY}"

# --- choose install dir ----------------------------------------------------
dir="${POLY_INSTALL_DIR:-/usr/local/bin}"
if [ ! -d "$dir" ] || [ ! -w "$dir" ]; then
  if [ "$(id -u)" = "0" ]; then
    mkdir -p "$dir"
  elif command -v sudo >/dev/null 2>&1 && [ -d "$dir" ]; then
    info "installing to ${dir} (requires sudo)"
    sudo install -m 0755 "${tmp}/${BINARY}" "${dir}/${BINARY}"
    info "installed ${dir}/${BINARY}"
    "${dir}/${BINARY}" version || true
    exit 0
  else
    dir="${HOME}/.local/bin"
    warn "/usr/local/bin not writable; installing to ${dir}"
    mkdir -p "$dir"
  fi
fi

install -m 0755 "${tmp}/${BINARY}" "${dir}/${BINARY}"
info "installed ${dir}/${BINARY}"

case ":${PATH}:" in
  *":${dir}:"*) ;;
  *) warn "${dir} is not on your PATH — add it, e.g.:  export PATH=\"${dir}:\$PATH\"" ;;
esac

"${dir}/${BINARY}" version || true
