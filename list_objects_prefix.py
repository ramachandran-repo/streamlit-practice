# simple one-time script

prefix = "main."
filename = "notebook_source.sql"

unique_entries = set()

with open(filename, "r", encoding="utf-8") as f:
    for line in f.read().splitlines():
        for token in line.split():
            if token.startswith(prefix):
                unique_entries.add(token)

print(f"Found {len(unique_entries)} unique entries:\n")
for item in sorted(unique_entries):
    print(item)
