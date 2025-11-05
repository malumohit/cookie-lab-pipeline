import argparse
import sys
import time
from pathlib import Path
import yaml

from runner_firefox_manual import run_one as run_one_firefox
from runner_chromium_manual import run_one as run_one_chromium

CHROMIUM_FAMILY = ("chrome", "edge", "brave", "opera")

def resolve_extension_path(ext: dict, browser_name: str) -> str | None:
    b = browser_name.lower()
    if b == "firefox":
        return ext.get("firefox_path")
    if b in CHROMIUM_FAMILY:
        return ext.get("chromium_path")
    return None

def parse_args():
    p = argparse.ArgumentParser(description="Cookie-test pipeline with resume + privacy levels")
    p.add_argument("--matrix", default=r"C:\cookie-lab\matrix.yaml")
    p.add_argument("--start-browser", default=None)
    p.add_argument("--start-extension", default=None)
    p.add_argument("--start-link", type=int, default=1)
    p.add_argument("--only-extension", default=None)
    p.add_argument("--redirect-window", type=float, default=6.0)
    # NEW: choose a privacy level defined in matrix.yaml -> privacy_levels
    p.add_argument(
        "--privacy",
        default=None,
        help="Privacy profile name to use (must match matrix.yaml privacy_levels for the browser family)",
    return p.parse_args()

def load_matrix(path: str) -> dict:
    cfg = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    for e in cfg.get("extensions", []):
        if "name" in e and e["name"] is not None: e["name"] = str(e["name"])
        if "version" in e and e["version"] is not None: e["version"] = str(e["version"])
    for b in cfg.get("browsers", []):
        if "name" in b and b["name"] is not None: b["name"] = str(b["name"])
        if "binary" in b and b["binary"] is not None: b["binary"] = str(b["binary"])
    return cfg

def pick_runner(browser_name: str):
    b = browser_name.lower()
    if b == "firefox":
        return run_one_firefox
    if b in CHROMIUM_FAMILY:
        return run_one_chromium
    return None

def _privacy_iter(cfg: dict, bname: str):
    """Yield privacy level dicts for this browser."""
    # Allow ‘firefox’, ‘chromium’, and specific chromium brands
    pl = cfg.get("privacy_levels", {})
    if bname.lower() == "firefox":
        return pl.get("firefox", [{"name": "default"}])
    # brave/opera/edge/chrome map to chromium unless a brand section exists
    return pl.get(bname.lower(), pl.get("chromium", [{"name": "default"}]))

def run_pipeline(cfg: dict, start_browser=None, start_ext=None, start_link_idx=1,
                 only_extension=None, redirect_window=6.0, privacy_name: str | None = None):
    master = Path(cfg["master_workbook"])
    output = Path(cfg["output_workbook"])
    browsers = cfg.get("browsers", [])
    extensions = cfg.get("extensions", [])
    links = cfg.get("links", [])
    if not browsers or not extensions or not links:
        print("matrix.yaml must include non-empty browsers/extensions/links.", file=sys.stderr)
        sys.exit(1)

    if start_browser:
        b_start_idx = next((i for i, b in enumerate(browsers)
                            if b.get("name","").lower()==start_browser.lower()), None)
        if b_start_idx is None:
            raise SystemExit(f"Browser '{start_browser}' not found")
    else:
        b_start_idx = 0

    if start_ext:
        e_start_idx = next((i for i, e in enumerate(extensions)
                            if e.get("name","").lower()==start_ext.lower()), None)
        if e_start_idx is None:
            raise SystemExit(f"Extension '{start_ext}' not found")
    else:
        e_start_idx = 0

    if start_link_idx < 1 or start_link_idx > len(links):
        raise SystemExit(f"--start-link must be between 1 and {len(links)}")
    l_start_idx = start_link_idx - 1

    job_no = 0

    for bi in range(b_start_idx, len(browsers)):
        bcfg = browsers[bi]
        bname = bcfg["name"]
        runner = pick_runner(bname)
        if runner is None:
            print(f"(skip) browser '{bname}' not implemented."); continue

        # iterate privacy levels for this browser
        for pl in _privacy_iter(cfg, bname):
            privacy_name = pl.get("name","default")
            privacy_prefs = pl.get("prefs", {})
            privacy_flags = pl.get("flags", [])

            e_iter = range(e_start_idx, len(extensions)) if bi == b_start_idx else range(0, len(extensions))
            for ei in e_iter:
                ext = extensions[ei]
                ext_name = ext["name"]
                ext_ver = str(ext.get("version", ""))
                if only_extension and ext_name.lower() != only_extension.lower():
                    continue

                ext_path = resolve_extension_path(ext, bname)
                if not ext_path:
                    print(f"(skip) {bname}: '{ext_name}' missing package for this browser.")
                    continue

                l_iter = range(l_start_idx, len(links)) if (bi == b_start_idx and ei == e_start_idx) else range(0, len(links))
                for li in l_iter:
                    link = links[li]
                    job_no += 1
                    job_id = f"job-{bname.lower()}-{ext_name.lower().replace(' ','_')}-{privacy_name}-{job_no:04d}"
                    job = {
                        "job_id": job_id,
                        "browser": bname,
                        "browser_binary": bcfg.get("binary"),
                        "extension_name": ext_name,
                        "extension_version": ext_ver,
                        "extension_path": ext_path,
                        "affiliate_link": link,
                        "extension_ordinal": ei + 1,
                        "redirect_window_sec": float(redirect_window),
                        "privacy_name": privacy_name,
                        "privacy_prefs": privacy_prefs,
                        "privacy_flags": privacy_flags,
                    }
                    print(f"\n=== RUN {job_id} ===")
                    try:
                        runner(job, master, output)
                    except Exception as e:
                        print(f"!! ERROR in {job_id}: {e.__class__.__name__}: {e}")
                    time.sleep(1.5)

                if only_extension and ext_name.lower() == only_extension.lower():
                    print(f"Only-extension '{only_extension}' completed. Exiting.")
                    return

            e_start_idx = 0
            l_start_idx = 0

if __name__ == "__main__":
    args = parse_args()
    cfg = load_matrix(args.matrix)
    run_pipeline(
        cfg,
        start_browser=args.start_browser,
        start_ext=args.start_extension,
        start_link_idx=args.start_link,
        only_extension=args.only_extension,
        redirect_window=args.redirect_window,
        privacy_name=args.privacy,
    )
