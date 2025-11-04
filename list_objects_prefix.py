import re

filename = "notebook_source.sql"
keywords = [
    "from", "join", "inner join", "left join", "right join", "full join", "cross join",
    "lateral view",
    "insert into", "insert overwrite", "update", "delete from", "merge into",
    "optimize", "vacuum", "analyze table", "refresh table", "refresh view",
    "cache table", "uncache table", "describe table", "desc table",
    "show tables in", "show views in"
]

pattern = re.compile(
    r"(?:"
    + "|".join(re.escape(k) for k in keywords)
    + r")\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)",
    re.IGNORECASE
)

unique_refs = set()

with open(filename, "r", encoding="utf-8") as f:
    text = f.read()

for m in pattern.finditer(text):
    table_ref = m.group(1).strip().rstrip(";")
    unique_refs.add(table_ref)

print("Existing table/view references found:\n")
for ref in sorted(unique_refs):
    print(ref)
