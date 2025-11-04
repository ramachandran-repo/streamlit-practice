#!/usr/bin/env python3
import re
from pathlib import Path

# --- config ---
input_file = "notebook_source.sql"
output_file = "table_refs.txt"

KEYWORDS = [
    # read/query
    "from", "join", "inner join", "left join", "right join", "full join", "cross join", "lateral view",
    # modify existing data
    "insert into", "insert overwrite", "update", "delete from", "merge into",
    # maintenance / mgmt
    "optimize", "vacuum", "analyze table", "refresh table", "refresh view",
    "cache table", "uncache table",
    # metadata
    "describe table", "desc table",
    "show tables in", "show views in",
]

IDENT = r"(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_\$]*)"
PATTERN = re.compile(
    r"(?:"
    + "|".join(re.escape(k) for k in KEYWORDS)
    + r")\s+(" + IDENT + r"(?:\s*\.\s*" + IDENT + r"){1,3})",
    re.IGNORECASE | re.DOTALL,
)

TRAILING_PUNCT = re.compile(r"[,\);\s]+$")
SPLIT_DOT = re.compile(r"\s*\.\s*")

def clean_token(raw: str) -> str:
    raw = TRAILING_PUNCT.sub("", raw)
    parts = [p.strip() for p in SPLIT_DOT.split(raw)]
    parts = [p[1:-1] if len(p) >= 2 and p.startswith("`") and p.endswith("`") else p for p in parts]
    if parts and " " in parts[-1]:
        parts[-1] = parts[-1].split()[0]
    return ".".join(parts)

def main():
    text = Path(input_file).read_text(encoding="utf-8", errors="ignore")

    unique_refs = set()
    for m in PATTERN.finditer(text):
        ref = clean_token(m.group(1))
        if "." in ref:  # exclude temp/local single-part names
            unique_refs.add(ref)

    print(f"Found {len(unique_refs)} unique references (2–4 parts):\n")
    for r in sorted(unique_refs, key=str.lower):
        print(r)

    with open(output_file, "w", encoding="utf-8") as f:
        f.writelines(r + "\n" for r in sorted(unique_refs, key=str.lower))
    print(f"\nSaved to {output_file}")

if __name__ == "__main__":
    main()
