#!/bin/sh
# Builds a .charm by hand (charmcraft needs LXD): ops is installed with pip for ubuntu@22.04 (python 3.10) and made pure Python.
# Usage: build.sh <jk-test|jk-test-client> [arm64|amd64 [output.charm [revision]]]
# Defaults: host arch, <repo>/bin/<charm>.charm, the revision in the charm's `revision` file (the stamp shown in the status message). Relative paths are relative to the caller's directory.
set -eu
CHARM=${1:?usage: build.sh <jk-test|jk-test-client> [arch [output.charm [revision]]]}
ROOT=$(cd "$(dirname "$0")" && pwd)
HERE=$ROOT/$CHARM
test -f "$HERE/metadata.yaml" || { echo "unknown charm $CHARM" >&2; exit 1; }
OUT=${3:-$ROOT/../bin/$CHARM.charm}
mkdir -p "$(dirname "$OUT")"
OUT=$(cd "$(dirname "$OUT")" && pwd)/$(basename "$OUT")
cd "$HERE"
case "${2:-$(uname -m)}" in
  arm64|aarch64) ARCH=arm64; PLAT=manylinux2014_aarch64 ;;
  amd64|x86_64) ARCH=amd64; PLAT=manylinux2014_x86_64 ;;
  *) echo "unknown arch $2" >&2; exit 1 ;;
esac
STAGE=$(mktemp -d)
trap 'rm -rf "$STAGE"' EXIT
mkdir "$STAGE/lib"
cp -R metadata.yaml config.yaml requirements.txt dispatch src revision "$STAGE/"
test ! -f actions.yaml || cp actions.yaml "$STAGE/"
if [ -n "${4:-}" ]; then echo "$4" > "$STAGE/revision"; fi
sed "s/ARCH/$ARCH/" manifest.yaml > "$STAGE/manifest.yaml"
python3 -m pip install --quiet --disable-pip-version-check --target "$STAGE/lib" --only-binary=:all: --platform "$PLAT" --python-version 3.10 --implementation cp -r requirements.txt
# PyYAML's wheel carries a C extension; dropping it makes yaml fall back to pure Python (arch independent).
find "$STAGE/lib" -name '*.so' -delete
find "$STAGE/lib" -name __pycache__ -type d -prune -exec rm -rf {} +
rm -rf "$STAGE/lib/bin"
python3 - "$STAGE" "$OUT" <<'PY'
import os, sys, zipfile
stage, out = sys.argv[1:]
names = []
for root, dirs, files in os.walk(stage):
    dirs.sort()
    for f in sorted(files):
        p = os.path.join(root, f)
        names.append(os.path.relpath(p, stage))
with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
    for n in sorted(names):
        info = zipfile.ZipInfo(n, (2020, 1, 1, 0, 0, 0))
        mode = 0o755 if n in ("dispatch", "src/charm.py") else 0o644
        info.external_attr = (0o100000 | mode) << 16
        info.compress_type = zipfile.ZIP_DEFLATED
        with open(os.path.join(stage, n), "rb") as fh:
            z.writestr(info, fh.read())
PY
echo "$OUT"
