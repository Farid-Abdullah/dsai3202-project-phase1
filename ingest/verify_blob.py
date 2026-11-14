"""Read raw blobs back from the lake and prove what is stored there.

  verify:   python3 verify_blob.py LAKE BLOB_PATH MANIFEST.json OUT.json
            streams the blob from Azure, hashes every byte, compares with the local manifest.
  receipts: python3 verify_blob.py LAKE PREFIX --receipts OUT.tsv
            lists name, bytes, ETag, last-modified under raw/PREFIX (used before/after the retry).

Auth: your Azure CLI sign-in (same identity as Lab-3, Storage Blob Data Contributor). No keys.
"""
import hashlib
import json
import sys
import os
import time

from azure.identity import AzureCliCredential
from azure.storage.blob import BlobServiceClient


def container(lake):
    svc = BlobServiceClient(f"https://{lake}.blob.core.windows.net", credential=AzureCliCredential())
    return svc.get_container_client(os.environ.get("CONTAINER", "raw"))


def receipts(lake, prefix, out):
    rows = sorted(
        (b.name, b.size, b.etag.strip('"'), b.last_modified.isoformat())
        for b in container(lake).list_blobs(name_starts_with=prefix.rstrip("/") + "/")
    )
    if not rows:
        sys.exit(f"STOP: nothing under raw/{prefix}")
    with open(out, "w") as f:
        f.write("name\tbytes\tetag\tlast_modified\n")
        for r in rows:
            f.write("\t".join(map(str, r)) + "\n")
    print(open(out).read())


def verify(lake, blob_path, manifest_path, out):
    m = json.load(open(manifest_path))["file"]
    blob = container(lake).get_blob_client(blob_path)
    props = blob.get_blob_properties()
    h, n, t0, last = hashlib.sha256(), 0, time.time(), time.time()
    for chunk in blob.download_blob(max_concurrency=8).chunks():
        h.update(chunk)
        n += len(chunk)
        if time.time() - last > 5:
            print(f"  read back {n / 2**30:.2f} / {props.size / 2**30:.2f} GiB", flush=True)
            last = time.time()
    result = {
        "blob": f"{os.environ.get('CONTAINER', 'raw')}/{blob_path}",
        "azure_bytes": n,
        "azure_sha256": h.hexdigest(),
        "manifest_bytes": m["bytes"],
        "manifest_sha256": m["sha256"],
        "etag": props.etag.strip('"'),
        "seconds": round(time.time() - t0, 1),
    }
    result["match"] = result["azure_sha256"] == m["sha256"] and n == m["bytes"]
    json.dump(result, open(out, "w"), indent=2)
    print(json.dumps(result, indent=2))
    print("MATCH source <-> raw" if result["match"] else "MISMATCH - do not continue, keep this evidence")
    sys.exit(0 if result["match"] else 1)


if __name__ == "__main__":
    if len(sys.argv) == 5 and sys.argv[3] == "--receipts":
        receipts(sys.argv[1], sys.argv[2], sys.argv[4])
    elif len(sys.argv) == 5:
        verify(*sys.argv[1:5])
    else:
        sys.exit(__doc__)
