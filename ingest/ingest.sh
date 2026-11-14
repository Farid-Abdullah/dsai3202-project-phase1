#!/usr/bin/env bash
# DSAI3202 Project Phase 1 - batch ingestion of REES46 "2019-Nov.csv" into the Lab-3 lake.
# Adapted from Lab-3 (download -> hash/manifest -> AzCopy to raw -> read-back verify -> idempotent retry).
#
# Run on the Lab-2 VM:   bash ingest/ingest.sh all
# Or one step at a time: bash ingest/ingest.sh preflight|download|extract|manifest|upload|verify|retry|landing
# Every step is safe to repeat: finished work is detected and reused, raw is never overwritten.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"

# ---- settings -------------------------------------------------------------
[ -f ~/.config/dsai3202/lab3.env ] && . ~/.config/dsai3202/lab3.env   # RG, SA, SUB_ID, SID from Lab-3
: "${SA:?SA (lake name) is empty - run: source ~/.config/dsai3202/lab3.env}"
KAGGLE_DATASET="mkechinov/ecommerce-behavior-data-from-multi-category-store"
SOURCE_FILE="2019-Nov.csv"
SOURCE_URL="https://www.kaggle.com/datasets/${KAGGLE_DATASET}"
WORK="${WORK:-$HOME/project-ingest}"          # large local files (NOT in the git repo)
EV="$REPO/evidence/ingestion"                  # small evidence files (committed)
RAW_PREFIX_BASE="rees46/2019-Nov"              # final raw path: raw/rees46/2019-Nov/sha256=<12 hex>/
export AZCOPY_AUTO_LOGIN_TYPE=AZCLI
export PATH="$HOME/.local/bin:$PATH"
mkdir -p "$WORK/download" "$WORK/source" "$EV"

say()  { printf '\n\033[1;34m== %s\033[0m\n' "$*"; }
ok()   { printf '\033[1;32mOK\033[0m %s\n' "$*"; }
die()  { printf '\033[1;31mSTOP\033[0m %s\n' "$*" >&2; exit 1; }
pipi() { python3 -m pip install --user -q "$@" 2>/dev/null || python3 -m pip install --user -q --break-system-packages "$@"; }

raw_prefix() {   # versioned path derived from the content hash -> same content = same path
  [ -f "$EV/source-manifest.json" ] || die "no manifest yet - run the manifest step"
  local h; h=$(python3 -c "import json;print(json.load(open('$EV/source-manifest.json'))['file']['sha256'][:12])")
  echo "$RAW_PREFIX_BASE/sha256=$h"
}

# ---- steps ----------------------------------------------------------------
preflight() {
  say "1/7 preflight: sign-in, lake, disk, tools"
  az account show --query '{Subscription:name,User:user.name}' -o table || die "run: az login --use-device-code"
  az storage account show -n "$SA" --query '{Lake:name,Region:location,HNS:isHnsEnabled}' -o table
  az storage fs exists -n raw --account-name "$SA" --auth-mode login -o tsv | grep -qi true \
    || die "cannot see the raw container with your sign-in (wrong lake or account?)"
  local free_gb; free_gb=$(df -BG --output=avail "$WORK" | tail -1 | tr -dc 0-9)
  echo "free disk under $WORK: ${free_gb} GB (need ~13 GB: archive + 9 GB csv)"
  [ "$free_gb" -ge 13 ] || [ -s "$WORK/source/$SOURCE_FILE" ] \
    || die "not enough disk. Free space (e.g. delete ~/Lab-03-student/work/source/*.json - they are verified in the lake) or resize the VM disk"
  command -v azcopy >/dev/null || die "azcopy missing (it was installed in Lab-3: bash ~/Lab-03-student/scripts/prepare-vm.sh)"
  echo "installing/upgrading kaggle CLI (new access_token format needs a recent version)..."; pipi --upgrade kaggle
  command -v kaggle >/dev/null || die "kaggle CLI not on PATH (expected in ~/.local/bin)"
  python3 -c "import azure.storage.blob, azure.identity" 2>/dev/null || pipi azure-storage-blob azure-identity
  [ -f ~/kaggle.json ] && mkdir -p ~/.kaggle && mv ~/kaggle.json ~/.kaggle/
  if [ -f ~/.kaggle/access_token ] && [ -z "${KAGGLE_API_TOKEN:-}" ]; then
    export KAGGLE_API_TOKEN="$(tr -d '[:space:]' < ~/.kaggle/access_token)"   # new-style token; older CLIs read the env var
  fi
  if [ -z "${KAGGLE_API_TOKEN:-}" ] && [ -z "${KAGGLE_KEY:-}" ] && [ ! -f ~/.kaggle/kaggle.json ]; then
    die "no Kaggle credentials: put the token in ~/.kaggle/access_token (chmod 600), then rerun"
  fi
  chmod 600 ~/.kaggle/* 2>/dev/null || true
  kaggle datasets files "$KAGGLE_DATASET" >/dev/null || die "Kaggle rejected the token or cannot see the dataset"
  ok "Kaggle token works"
  ok "preflight passed"
}

download() {
  say "2/7 download $SOURCE_FILE from Kaggle"
  [ -f ~/.kaggle/access_token ] && [ -z "${KAGGLE_API_TOKEN:-}" ] && export KAGGLE_API_TOKEN="$(tr -d '[:space:]' < ~/.kaggle/access_token)"
  if [ -s "$WORK/source/$SOURCE_FILE" ]; then ok "extracted file already present - download skipped"; return; fi
  kaggle datasets files "$KAGGLE_DATASET" --csv > "$EV/kaggle-file-listing.csv" || true
  if ! ls "$WORK/download/"* >/dev/null 2>&1; then
    kaggle datasets download "$KAGGLE_DATASET" -f "$SOURCE_FILE" -p "$WORK/download" \
      || { echo "single-file download failed - downloading the whole dataset archive instead";
           kaggle datasets download "$KAGGLE_DATASET" -p "$WORK/download"; }
  else ok "archive already downloaded - reusing it"; fi
  date -u +%Y-%m-%dT%H:%M:%SZ > "$EV/download-utc.txt"
  ls -l "$WORK/download"
}

extract() {
  say "3/7 extract $SOURCE_FILE (streaming, no full copy in memory)"
  if [ -s "$WORK/source/$SOURCE_FILE" ]; then ok "already extracted"; return; fi
  python3 -I - "$WORK/download" "$WORK/source/$SOURCE_FILE" "$SOURCE_FILE" <<'PY'
import sys, zipfile, gzip, shutil, pathlib
src_dir, out, member = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]), sys.argv[3]
files = [p for p in src_dir.iterdir() if p.is_file()]
tmp = out.with_suffix(".partial")
for p in files:
    if zipfile.is_zipfile(p):
        with zipfile.ZipFile(p) as z:
            names = [n for n in z.namelist() if n.endswith(member)]
            if names:
                with z.open(names[0]) as r, open(tmp, "wb") as w: shutil.copyfileobj(r, w, 16 << 20)
                break
    elif p.name.endswith(".gz"):
        with gzip.open(p) as r, open(tmp, "wb") as w: shutil.copyfileobj(r, w, 16 << 20); break
    elif p.name == member:
        shutil.move(p, tmp); break
else:
    sys.exit(f"STOP: {member} not found in {[f.name for f in files]}")
tmp.replace(out)
print("extracted", out, out.stat().st_size, "bytes")
PY
}

manifest() {
  say "4/7 manifest: size, SHA-256, row count, header (one pass over 9 GB, ~2-4 min)"
  if [ -f "$EV/source-manifest.json" ]; then ok "manifest exists - not regenerated (delete it only if the source changed)"; cat "$EV/source-manifest.json"; return; fi
  python3 -I - "$WORK" "$SOURCE_FILE" "$SOURCE_URL" "$KAGGLE_DATASET" "$EV" <<'PY'
import sys, hashlib, json, time, pathlib, csv, io
work, name, url, slug, ev = pathlib.Path(sys.argv[1]), sys.argv[2], sys.argv[3], sys.argv[4], pathlib.Path(sys.argv[5])
def digest(path, count_lines=False):
    h, n, lines, t0, last, last_byte = hashlib.sha256(), 0, 0, time.time(), time.time(), b""
    with open(path, "rb") as f:
        while chunk := f.read(16 << 20):
            h.update(chunk); n += len(chunk)
            if count_lines: lines += chunk.count(b"\n"); last_byte = chunk[-1:]
            if time.time() - last > 5: print(f"  {path.name}: {n/2**30:.2f} GiB hashed", flush=True); last = time.time()
    if count_lines and n and last_byte != b"\n": lines += 1      # last line without a trailing newline
    return h.hexdigest(), n, lines
f = work / "source" / name
sha, size, lines = digest(f, True)
with open(f, "rb") as fh: header = fh.readline().decode().strip()
archives = []
for a in sorted((work / "download").glob("*")):
    if a.is_file():
        s, n, _ = digest(a); archives.append({"name": a.name, "bytes": n, "sha256": s})
listed = None
lst = ev / "kaggle-file-listing.csv"
if lst.exists():
    for row in csv.DictReader(io.StringIO(lst.read_text())):
        if row.get("name") == name: listed = row
m = {
  "dataset": "eCommerce behavior data from multi category store (REES46 Open CDP)",
  "kaggle_dataset": slug, "source_url": url, "license_note": "free to use with credit to Kaggle page and REES46",
  "file": {"name": name, "bytes": size, "sha256": sha, "lines_including_header": lines,
           "data_rows": lines - 1, "header": header.split(",")},
  "kaggle_listing_for_file": listed,
  "transport_archives": archives,
  "downloaded_utc": (ev / "download-utc.txt").read_text().strip() if (ev / "download-utc.txt").exists() else None,
  "manifest_created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
}
(ev / "source-manifest.json").write_text(json.dumps(m, indent=2))
print(json.dumps(m, indent=2))
PY
  ok "manifest saved to evidence/ingestion/source-manifest.json"
}

azput() {  # $1 local file, $2 container path ; never overwrites
  azcopy copy "$1" "https://$SA.blob.core.windows.net/raw/$2" --overwrite=false --put-md5 --output-level essential
}

upload() {
  say "5/7 upload to raw/$(raw_prefix)/ with --overwrite=false"
  local P; P=$(raw_prefix)
  azput "$WORK/source/$SOURCE_FILE" "$P/$SOURCE_FILE" 2>&1 | tee "$EV/azcopy-first-run.txt"
  azput "$EV/source-manifest.json"  "$P/_manifest/source-manifest.json" 2>&1 | tee -a "$EV/azcopy-first-run.txt"
  echo "raw/$P" > "$EV/raw-path.txt"
  ok "uploaded (or skipped if already present). Next: verify"
}

verify() {
  say "6/7 read the Azure copy back and compare SHA-256 (~2-5 min)"
  python3 "$HERE/verify_blob.py" "$SA" "$(raw_prefix)/$SOURCE_FILE" "$EV/source-manifest.json" "$EV/raw-verify.json"
  python3 "$HERE/verify_blob.py" "$SA" "$(raw_prefix)" --receipts "$EV/receipts-before.tsv"
}

retry() {
  say "7/7 repeat the same ingestion - nothing may change"
  local P; P=$(raw_prefix)
  azput "$WORK/source/$SOURCE_FILE" "$P/$SOURCE_FILE" 2>&1 | tee "$EV/azcopy-retry.txt"
  azput "$EV/source-manifest.json"  "$P/_manifest/source-manifest.json" 2>&1 | tee -a "$EV/azcopy-retry.txt"
  python3 "$HERE/verify_blob.py" "$SA" "$P" --receipts "$EV/receipts-after.tsv"
  if diff "$EV/receipts-before.tsv" "$EV/receipts-after.tsv" > "$EV/retry-diff.txt"; then
    { echo "RETRY PROOF: PASS"; echo "receipts identical (name, bytes, ETag, last-modified) after a second ingestion";
      grep -E "Number of File Transfers (Completed|Skipped)" "$EV/azcopy-retry.txt" || true; } | tee "$EV/retry-proof.txt"
  else
    echo "RETRY PROOF: FAIL - receipts differ, see retry-diff.txt" | tee "$EV/retry-proof.txt"; exit 1
  fi
}

landing() {
  say "8 landing copy for Databricks: processed/project/landing/$(raw_prefix)/"
  # Databricks has read/write on processed + curated but deliberately no grant on raw (the compute cannot touch the
  # preserved originals). It reads a hash-verified, byte-identical copy instead. Raw stays the system of record.
  local P="project/landing/$(raw_prefix)"
  for f in "$WORK/source/$SOURCE_FILE:$P/$SOURCE_FILE" "$EV/source-manifest.json:$P/_manifest/source-manifest.json"; do
    azcopy copy "${f%%:*}" "https://$SA.blob.core.windows.net/processed/${f#*:}" --overwrite=false --put-md5 --output-level essential
  done 2>&1 | tee "$EV/azcopy-landing.txt"
  CONTAINER=processed python3 "$HERE/verify_blob.py" "$SA" "$P/$SOURCE_FILE" "$EV/source-manifest.json" "$EV/landing-verify.json"
}

case "${1:-all}" in
  all) preflight; download; extract; manifest; upload; verify; retry; landing
       say "DONE. Raw source: $(cat "$EV/raw-path.txt")"; ls -l "$EV" ;;
  preflight|download|extract|manifest|upload|verify|retry|landing) "$1" ;;
  *) die "unknown step $1" ;;
esac
