#!/bin/sh
# Install Forge on this host: clone the repo if needed, link
# ~/.local/bin/forge to it (forge update finds the checkout through that
# link), and put ~/.local/bin on PATH in the shell startup file.  Safe to
# run again.
#
# Usage:
#   scripts/install.sh                         # from a checkout: link it
#   ssh HOST sh -s < scripts/install.sh        # set up another host
#
# FORGE_DIR (default ~/code/chess/forge) and FORGE_REPO_URL override the
# checkout location and the clone URL.
set -eu

repo_url=${FORGE_REPO_URL:-git@github.com:ilAYAli/Forge.git}
bin_dir=$HOME/.local/bin
link=$bin_dir/forge

# Run from a checkout, link that checkout; piped over ssh, use FORGE_DIR.
script_dir=
case $0 in
    */install.sh) script_dir=$(cd -- "$(dirname -- "$0")" && pwd -P) ;;
esac
if [ -z "${FORGE_DIR:-}" ] && [ -n "$script_dir" ] && [ -f "$script_dir/../forge.py" ]; then
    dir=$(cd -- "$script_dir/.." && pwd -P)
else
    dir=${FORGE_DIR:-$HOME/code/chess/forge}
fi

for tool in git python3; do
    command -v "$tool" >/dev/null 2>&1 || { echo "install: $tool not found" >&2; exit 1; }
done
# Forge's own ssh commands put /opt/homebrew/bin and /usr/local/bin first, so
# an old system python3 is only a problem when forge is run by hand.
python3 -c 'import sys; sys.exit(sys.version_info < (3, 11))' ||
    echo "warning: Forge needs Python 3.11 or newer; python3 on PATH is $(python3 -V 2>&1)" >&2

if [ -f "$dir/forge.py" ]; then
    echo "checkout: $dir ($(git -C "$dir" log -1 --format=%h))"
elif [ -e "$dir" ]; then
    echo "install: $dir exists but has no forge.py" >&2
    exit 1
else
    mkdir -p -- "$(dirname -- "$dir")"
    git clone -q "$repo_url" "$dir"
    echo "cloned: $dir ($(git -C "$dir" log -1 --format=%h))"
fi
chmod +x "$dir/forge.py"

mkdir -p -- "$bin_dir"
if [ -L "$link" ] && [ "$(readlink "$link")" = "$dir/forge.py" ]; then
    echo "link: $link -> $dir/forge.py"
else
    if [ -e "$link" ] || [ -L "$link" ]; then
        echo "link: replacing $link (was $(readlink "$link" 2>/dev/null || echo 'a regular file'))"
    fi
    ln -sfn "$dir/forge.py" "$link"
    echo "link: $link -> $dir/forge.py"
fi

# Put ~/.local/bin on PATH for the login shell.  zsh reads ~/.zshenv in every
# shell, including ssh commands; bash reads ~/.bashrc for ssh commands and
# ~/.profile for logins; other shells read ~/.profile.
case ${SHELL:-} in
    */zsh) rc_files="$HOME/.zshenv" ;;
    */bash) rc_files="$HOME/.bashrc $HOME/.profile" ;;
    *) rc_files="$HOME/.profile" ;;
esac
for rc in $rc_files; do
    if [ -f "$rc" ] && grep -q '\.local/bin' "$rc"; then
        echo "path: $rc already sets ~/.local/bin"
        continue
    fi
    cat >>"$rc" <<'EOF'

# Added by Forge scripts/install.sh
case ":$PATH:" in *":$HOME/.local/bin:"*) ;; *) export PATH="$HOME/.local/bin:$PATH" ;; esac
EOF
    echo "path: added ~/.local/bin to $rc"
done
case ":$PATH:" in
    *":$bin_dir:"*) ;;
    *) echo "path: open a new shell, or run: export PATH=\"\$HOME/.local/bin:\$PATH\"" ;;
esac
