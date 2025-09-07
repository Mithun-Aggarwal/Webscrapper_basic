#!/usr/bin/env python3
"""
Sync a Vector Store from a manifest.jsonl (SSOT).

Primary key: file_url
Version gate: sha256
Metadata written (only): file_url, source_page, last_modified, sha256, size_bytes,
                         discovered_at, http_status, status, file_path

Features
- Dry run by default (prints plan, writes sync_report.json + vector_inventory.jsonl)
- --apply to execute, --prune to delete store items missing from manifest
- --pdf-root to resolve relative file_path; --require-local to forbid HTTP download
- Batch uploads via vector_stores.file_batches.upload_and_poll(...)
- Metadata-only updates when sha256 unchanged
- Re-upload + replace when sha256 changed
- Oversize/failed buckets for audit

Requires: pip install openai tqdm python-dotenv requests
Env: OPENAI_API_KEY must be set
"""

import argparse, json, os, sys, time
from dataclasses import dataclass, asdict
from typing import Dict, List, Tuple, Optional
from pathlib import Path

import requests
from openai import OpenAI
from dotenv import load_dotenv
from tqdm import tqdm

# ---------- Constants ----------
KEEP_METADATA_KEYS = {
    "file_url",
    "source_page",
    "last_modified",
    "sha256",
    "size_bytes",
    "discovered_at",
    "http_status",
    "status",
    "file_path",
}
DEFAULT_MAX_FILE_SIZE_MB = 512

# ---------- Data ----------
@dataclass
class ManifestRow:
    file_url: str
    source_page: Optional[str] = None
    last_modified: Optional[str] = None
    sha256: Optional[str] = None
    size_bytes: Optional[int] = None
    discovered_at: Optional[str] = None
    http_status: Optional[int] = None
    status: Optional[str] = None
    file_path: Optional[str] = None

    def minimal_metadata(self) -> Dict:
        md = {k: getattr(self, k) for k in KEEP_METADATA_KEYS}
        return {k: v for k, v in md.items() if v is not None}

@dataclass
class StoreEntry:
    file_id: str
    file_url: Optional[str]
    sha256: Optional[str]
    metadata: Dict

# ---------- Helpers ----------
def fail(msg: str):
    print(f"ERROR: {msg}", file=sys.stderr); sys.exit(1)

def read_manifest_jsonl(path: Path) -> Dict[str, ManifestRow]:
    out: Dict[str, ManifestRow] = {}
    with path.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"⚠️  Bad JSON (line {i}): {e}"); continue
            url = obj.get("file_url")
            if not url:
                continue
            out[url] = ManifestRow(
                file_url=url,
                source_page=obj.get("source_page"),
                last_modified=obj.get("last_modified"),
                sha256=obj.get("sha256"),
                size_bytes=obj.get("size_bytes"),
                discovered_at=obj.get("discovered_at"),
                http_status=obj.get("http_status"),
                status=obj.get("status"),
                file_path=obj.get("file_path"),
            )
    return out

def list_store_entries(client: OpenAI, vector_store_id: str, page_limit: int = 100) -> Dict[str, StoreEntry]:
    """Paginate with limit<=100."""
    by_url: Dict[str, StoreEntry] = {}
    after = None
    while True:
        resp = client.vector_stores.files.list(vector_store_id=vector_store_id, limit=min(100, page_limit), after=after)
        data = getattr(resp, "data", []) or []
        for item in data:
            file_id = getattr(item, "id", None)
            meta = getattr(item, "metadata", None)
            if meta is None:
                full = client.vector_stores.files.retrieve(vector_store_id=vector_store_id, file_id=file_id)
                meta = getattr(full, "metadata", {}) or {}
            file_url = meta.get("file_url")
            sha256 = meta.get("sha256")
            if file_url:
                by_url[file_url] = StoreEntry(file_id=file_id, file_url=file_url, sha256=sha256, metadata=meta)
        has_more = getattr(resp, "has_more", None)
        after = getattr(resp, "last_id", None)
        if has_more is not None:
            if not has_more: break
        else:
            if after is None or len(data) < min(100, page_limit): break
    return by_url

def ensure_vector_store(client: OpenAI, vsid: Optional[str]) -> str:
    if vsid: return vsid
    vs = client.vector_stores.create(name="PBAC-Library")
    print(f"Created vector store: {vs.id}")
    return vs.id

def needs_metadata_update(current_md: Dict, desired_md: Dict) -> bool:
    for k in KEEP_METADATA_KEYS:
        if current_md.get(k) != desired_md.get(k):
            return True
    return False

def resolve_local_path(row: ManifestRow, pdf_root: Optional[Path]) -> Optional[Path]:
    """Resolve robustly so we handle roots that already include 'out/www...'."""
    if not row.file_path: return None
    p = Path(row.file_path)
    candidates = []
    if p.is_absolute(): candidates.append(p)
    else:
        if pdf_root:
            candidates.append((pdf_root / p).resolve())
            if pdf_root.parent: candidates.append((pdf_root.parent / p).resolve())
        candidates.append(p.resolve())  # relative to CWD
    for c in candidates:
        if c.exists() and c.is_file(): return c
    return None

def maybe_download(row: ManifestRow, timeout=60) -> Optional[Path]:
    try:
        r = requests.get(row.file_url, timeout=timeout, stream=True)
        if r.status_code == 200:
            tmp = Path("./.tmp_downloads"); tmp.mkdir(parents=True, exist_ok=True)
            fname = row.file_url.split("/")[-1] or "downloaded.pdf"
            local = tmp / fname
            with local.open("wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk: f.write(chunk)
            return local
        print(f"⚠️  GET {row.file_url} -> {r.status_code}"); return None
    except Exception as e:
        print(f"⚠️  Download failed for {row.file_url}: {e}"); return None

def upload_batch(client: OpenAI, vector_store_id: str, paths: List[Tuple[Path, ManifestRow]]) -> List[str]:
    """
    Upload a batch of files to the vector store.
    Return the list of new file_ids (same order as 'paths').
    IMPORTANT: pass a list of file handles, not dicts.
    """
    open_files = [p.open("rb") for p, _ in paths]
    try:
        batch = client.vector_stores.file_batches.upload_and_poll(
            vector_store_id=vector_store_id,
            files=open_files   # ✅ correct shape
        )
    finally:
        for f in open_files:
            try: f.close()
            except Exception: pass

    # IDs may be in .file_ids or in .data[*].id depending on SDK
    created_ids = []
    if hasattr(batch, "file_ids") and batch.file_ids:
        created_ids = list(batch.file_ids)
    elif hasattr(batch, "data") and batch.data:
        created_ids = [getattr(item, "id", None) for item in batch.data if getattr(item, "id", None)]
    else:
        # As a last resort, diff the store before/after (expensive) — skipped here
        pass
    return created_ids

def set_file_metadata(client: OpenAI, vector_store_id: str, file_id: str, metadata: Dict):
    try:
        client.vector_stores.files.update_attributes(vector_store_id=vector_store_id, file_id=file_id, metadata=metadata)
    except Exception:
        client.vector_stores.files.update(vector_store_id=vector_store_id, file_id=file_id, metadata=metadata)  # fallback

def delete_files(client: OpenAI, vector_store_id: str, file_ids: List[str]):
    for fid in tqdm(file_ids, desc="deleting", leave=False):
        client.vector_stores.files.delete(vector_store_id=vector_store_id, file_id=fid)

# ---------- Main ----------
def main():
    ap = argparse.ArgumentParser(description="Sync Vector Store from manifest.jsonl (SSOT).")
    ap.add_argument("--manifest", required=True, type=Path)
    ap.add_argument("--vector-store-id", default=None)
    ap.add_argument("--batch-size", type=int, default=200)
    ap.add_argument("--max-file-size-mb", type=int, default=DEFAULT_MAX_FILE_SIZE_MB)
    ap.add_argument("--apply", action="store_true", help="Execute changes (default: dry-run).")
    ap.add_argument("--prune", action="store_true", help="Delete store files not present in manifest.")
    ap.add_argument("--pdf-root", type=Path, default=None, help="Prefix to resolve relative file_path.")
    ap.add_argument("--require-local", action="store_true", help="Do not download; only use local files.")
    ap.add_argument("--no-dotenv", action="store_true", help="Skip loading .env")
    args = ap.parse_args()

    if not args.no_dotenv: load_dotenv()
    if not os.getenv("OPENAI_API_KEY"):
        fail("OPENAI_API_KEY is not set. export OPENAI_API_KEY=sk-... then rerun.")

    client = OpenAI()
    vsid = ensure_vector_store(client, args.vector_store_id)

    if not args.manifest.exists():
        fail(f"Manifest not found: {args.manifest}")

    manifest = read_manifest_jsonl(args.manifest)
    print(f"Loaded {len(manifest)} rows from manifest: {args.manifest}")

    store_by_url = list_store_entries(client, vsid)
    print(f"Vector store currently has {len(store_by_url)} files keyed by file_url.")

    manifest_urls = set(manifest.keys())
    store_urls = set(store_by_url.keys())

    to_add = sorted(list(manifest_urls - store_urls))
    in_both = sorted(list(manifest_urls & store_urls))
    to_delete_urls = sorted(list(store_urls - manifest_urls))

    to_reupload, to_update_meta = [], []
    for url in in_both:
        row = manifest[url]; entry = store_by_url[url]
        desired = row.minimal_metadata()
        if row.sha256 and entry.sha256 and row.sha256 != entry.sha256:
            to_reupload.append(url)
        elif needs_metadata_update(entry.metadata or {}, desired):
            to_update_meta.append(url)

    print("\n=== PLAN ===")
    print(f"Adds (new): {len(to_add)}")
    print(f"Updates (reupload content): {len(to_reupload)}")
    print(f"Updates (metadata-only): {len(to_update_meta)}")
    print(f"Deletes (not in manifest): {len(to_delete_urls)} {'(will run)' if args.prune else '(skipped unless --prune)'}")

    report = {
        "vector_store_id": vsid,
        "counts": {
            "manifest_rows": len(manifest),
            "store_rows": len(store_by_url),
            "adds": len(to_add),
            "reuploads": len(to_reupload),
            "metadata_updates": len(to_update_meta),
            "deletes": len(to_delete_urls) if args.prune else 0,
        },
        "timestamp": int(time.time()),
        "dry_run": not args.apply,
        "prune": args.prune,
    }

    # Always emit inventory on dry-run too
    write_vector_inventory(client, vsid, store_by_url)

    if not args.apply:
        with open("sync_report.json", "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        print("\nDry-run only. Plan saved to sync_report.json  |  Inventory: vector_inventory.jsonl")
        return

    max_bytes = args.max_file_size_mb * 1024 * 1024
    oversize, failed = [], []

    def prepare_paths(urls: List[str]) -> List[Tuple[Path, ManifestRow]]:
        prepared = []
        for u in urls:
            row = manifest[u]
            if row.size_bytes and row.size_bytes > max_bytes:
                oversize.append(asdict(row)); continue
            p = resolve_local_path(row, args.pdf_root)
            if p is None:
                if args.require_local:
                    failed.append({"reason": "local_missing_require_local", "row": asdict(row)}); continue
                p = maybe_download(row)
            if p is None or not p.exists():
                failed.append({"reason": "missing_local_or_download_failed", "row": asdict(row)}); continue
            if p.stat().st_size > max_bytes:
                oversize.append(asdict(row)); continue
            prepared.append((p, row))
        return prepared

    # ADD NEW
    for i in range(0, len(to_add), args.batch_size):
        batch = to_add[i:i + args.batch_size]
        prep = prepare_paths(batch)
        if not prep: continue
        print(f"\nUploading batch {i//args.batch_size + 1} ({len(prep)} files)...")
        created_ids = upload_batch(client, vsid, prep)
        # Map created IDs back to rows by order
        for (p, row), fid in tqdm(list(zip(prep, created_ids)), desc=f"metadata (adds {i//args.batch_size+1})", leave=False):
            if not fid:
                failed.append({"reason": "missing_file_id_after_upload", "row": asdict(row)}); continue
            try:
                set_file_metadata(client, vsid, fid, row.minimal_metadata())
            except Exception as e:
                failed.append({"reason": f"metadata_update_failed: {e}", "row": asdict(row), "file_id": fid})

    # REUPLOAD (content change)
    for i in range(0, len(to_reupload), args.batch_size):
        batch = to_reupload[i:i + args.batch_size]
        prep = prepare_paths(batch)
        if not prep: continue
        print(f"\nReuploading batch {i//args.batch_size + 1} ({len(prep)} files)...")
        created_ids = upload_batch(client, vsid, prep)
        for (p, row), fid in tqdm(list(zip(prep, created_ids)), desc=f"metadata (reupload {i//args.batch_size+1})", leave=False):
            if not fid:
                failed.append({"reason": "missing_file_id_after_upload_reupload", "row": asdict(row)}); continue
            try:
                set_file_metadata(client, vsid, fid, row.minimal_metadata())
                # delete old
                old_entry = list_store_entries(client, vsid).get(row.file_url)
                if old_entry:
                    client.vector_stores.files.delete(vector_store_id=vsid, file_id=old_entry.file_id)
            except Exception as e:
                failed.append({"reason": f"reupload_or_delete_failed: {e}", "row": asdict(row), "new_file_id": fid})

    # METADATA-ONLY
    for url in tqdm(to_update_meta, desc="metadata-only updates"):
        row = manifest[url]; entry = store_by_url[url]
        try:
            set_file_metadata(client, vsid, entry.file_id, row.minimal_metadata())
        except Exception as e:
            failed.append({"reason": f"metadata_update_failed: {e}", "row": asdict(row), "file_id": entry.file_id})

    # PRUNE
    if args.prune and to_delete_urls:
        delete_ids = [store_by_url[u].file_id for u in to_delete_urls if u in store_by_url]
        print(f"\nPruning {len(delete_ids)} files...")
        delete_files(client, vsid, delete_ids)

    # Refresh inventory & write report
    store_by_url = list_store_entries(client, vsid)
    write_vector_inventory(client, vsid, store_by_url)

    report["oversize"] = len(oversize); report["failed"] = len(failed)
    with open("sync_report.json", "w", encoding="utf-8") as f: json.dump(report, f, indent=2)
    if oversize:
        with open("oversize.jsonl", "w", encoding="utf-8") as f:
            for row in oversize: f.write(json.dumps(row) + "\n")
    if failed:
        with open("failed.jsonl", "w", encoding="utf-8") as f:
            for row in failed: f.write(json.dumps(row) + "\n")

    print("\n✅ Sync complete.")
    print("Report: sync_report.json  |  Inventory: vector_inventory.jsonl")
    if oversize: print(f"⚠️  Oversize: {len(oversize)} (see oversize.jsonl)")
    if failed:   print(f"⚠️  Failed:   {len(failed)} (see failed.jsonl)")

def write_vector_inventory(client: OpenAI, vector_store_id: str, store_by_url: Dict[str, StoreEntry]):
    inv_path = Path("vector_inventory.jsonl")
    with inv_path.open("w", encoding="utf-8") as f:
        for _, entry in store_by_url.items():
            obj = {
                "vector_store_id": vector_store_id,
                "file_id": entry.file_id,
                **{k: v for k, v in (entry.metadata or {}).items() if k in KEEP_METADATA_KEYS},
            }
            f.write(json.dumps(obj) + "\n")

if __name__ == "__main__":
    main()
