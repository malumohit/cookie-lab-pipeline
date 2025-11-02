import yaml, os
from pathlib import Path

cfg = yaml.safe_load(Path(r"C:\cookie-lab\matrix.yaml").read_text())

missing = []
for e in cfg.get("extensions", []):
    for key in ("firefox_path", "chromium_path"):
        p = e.get(key)
        if not p:
            continue
        if not os.path.exists(p):
            missing.append((e.get("name"), key, p))

print("Checked extensions. Missing paths:" if missing else "All extension paths exist.")
for name, key, p in missing:
    print(f"- {name} [{key}]: {p}")

print("\nChecking links:")
for i, url in enumerate(cfg.get("links", []), 1):
    if not isinstance(url, str) or "http" not in url:
        print(f"- Link #{i} looks odd: {url!r}")
