#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:?Usage: ./collect_unique_pdfs.sh <ROOT_DIR> <OUT_DIR>}"
OUT="${2:?Usage: ./collect_unique_pdfs.sh <ROOT_DIR> <OUT_DIR>}"

mkdir -p "$OUT"
declare -A SEEN

# find all PDFs (case-insensitive), skip the output dir itself
while IFS= read -r -d '' f; do
  hash="$(sha256sum "$f" | awk '{print $1}')"
  [[ -n "${SEEN[$hash]:-}" ]] && continue
  SEEN[$hash]=1

  base="$(basename "$f")"
  dest="$OUT/$base"

  # if a different file with same name already exists, disambiguate using hash
  if [[ -e "$dest" ]]; then
    existing_hash="$(sha256sum "$dest" | awk '{print $1}')"
    if [[ "$existing_hash" == "$hash" ]]; then
      # same content already copied
      continue
    else
      ext="${base##*.}"; name="${base%.*}"
      dest="$OUT/${name}_${hash:0:8}.${ext}"
    fi
  fi

  # copy preserving timestamps/permissions; swap 'cp -a' with 'ln' to hard-link
  cp -a "$f" "$dest"
  echo "Copied: $f -> $dest"
done < <(find "$ROOT" -type f -iname '*.pdf' -not -path "$OUT/*" -print0)
