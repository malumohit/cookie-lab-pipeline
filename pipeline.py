import argparse
import sys
import time
from pathlib import Path

import yaml

# Runners
from runner_firefox_manual import run_one as run_one_firefox
from runner_chromium_manual import run_one as run_one_chromium

CHROMIUM_FAMILY = ("chrome", "edge", "brave", "opera")


def resolve_extension_path(ext: dict, browser_name: str) -> str | None:
    """Pick the correct extension package path for the browser."""
    b = browser_name.lower()
    if b == "firefox":
        return ext.get("firefox_path")
    if b in CHROMIUM_FAMILY:
        return ext.get("chromium_path")
    return None


def parse_args():
    p = argparse.ArgumentParser(description="Cookie-test pipeline with resume controls")
    p.add_argument(
        "--matrix",
        default=r"C:\cookie-lab\matrix.yaml",
        help="Path to matrix.yaml (default: C:\\cookie-lab\\matrix.yaml)",
    )
    p.add_argument(
        "--start-browser",
        default=None,
        help="Browser name to start from (e.g., firefox, chrome, edge, brave, opera)",
    )
    p.add_argument(
        "--start-extension",
        default=None,
        help='Extension name to start from (e.g., "Perkspot")',
    )
    p.add_argument(
        "--start-link",
        type=int,
        default=1,
        help="1-based link index in links list to start from (default: 1)",
    )
    p.add_argument(
        "--only-extension",
        default=None,
        help='If set, only run this extension (e.g., "Perkspot") and then stop',
    )
    return p.parse_args()


def load_matrix(path: str) -> dict:
    cfg = yaml.safe_load(Path(path).read_text())
    # normalize names/versions to strings
    for e in cfg.get("extensions", []):
        if "name" in e and e["name"] is not None:
            e["name"] = str(e["name"])
        if "version" in e and e["version"] is not None:
            e["version"] = str(e["version"])
    for b in cfg.get("browsers", []):
        if "name" in b and b["name"] is not None:
            b["name"] = str(b["name"])
        if "binary" in b and b["binary"] is not None:
            b["binary"] = str(b["binary"])
    return cfg


def pick_runner(browser_name: str):
    b = browser_name.lower()
    if b == "firefox":
        return run_one_firefox
    if b in CHROMIUM_FAMILY:
        return run_one_chromium
    return None


def run_pipeline(
    cfg: dict,
    start_browser: str | None = None,
    start_ext: str | None = None,
    start_link_idx: int = 1,
    only_extension: str | None = None,
):
    master = Path(cfg["master_workbook"])
    output = Path(cfg["output_workbook"])

    browsers = cfg.get("browsers", [])
    extensions = cfg.get("extensions", [])
    links = cfg.get("links", [])

    if not browsers or not extensions or not links:
        print("matrix.yaml must include non-empty browsers/extensions/links.", file=sys.stderr)
        sys.exit(1)

    # Browser start index
    if start_browser:
        b_start_idx = next(
            (i for i, b in enumerate(browsers) if b.get("name", "").lower() == start_browser.lower()),
            None,
        )
        if b_start_idx is None:
            raise SystemExit(f"Browser '{start_browser}' not found in matrix.yaml")
    else:
        b_start_idx = 0

    # Extension start index
    if start_ext:
        e_start_idx = next(
            (i for i, e in enumerate(extensions) if e.get("name", "").lower() == start_ext.lower()),
            None,
        )
        if e_start_idx is None:
            raise SystemExit(f"Extension '{start_ext}' not found in matrix.yaml")
    else:
        e_start_idx = 0

    # Link start index (convert 1-based to 0-based)
    if start_link_idx < 1 or start_link_idx > len(links):
        raise SystemExit(f"--start-link must be between 1 and {len(links)}")
    l_start_idx = start_link_idx - 1

    job_no = 0

    for bi in range(b_start_idx, len(browsers)):
        bcfg = browsers[bi]
        bname = bcfg["name"]
        runner = pick_runner(bname)
        if runner is None:
            print(f"(skip) browser '{bname}' not implemented.")
            continue

        # extension iteration
        e_iter = range(e_start_idx, len(extensions)) if bi == b_start_idx else range(0, len(extensions))
        for ei in e_iter:
            ext = extensions[ei]
            ext_name = ext["name"]
            ext_ver = str(ext.get("version", ""))

            # honor --only-extension if set
            if only_extension and ext_name.lower() != only_extension.lower():
                continue

            # resolve per-browser path (firefox_path / chromium_path)
            ext_path = resolve_extension_path(ext, bname)
            if not ext_path:
                print(f"(skip) {bname}: '{ext_name}' has no compatible package (firefox_path/chromium_path missing).")
                # if only_extension was requested and it's missing for this browser, continue to next browser
                continue

            # links iteration
            l_iter = range(l_start_idx, len(links)) if (bi == b_start_idx and ei == e_start_idx) else range(0, len(links))
            for li in l_iter:
                link = links[li]
                job_no += 1
                job_id = f"job-{bname.lower()}-{ext_name.lower().replace(' ', '_')}-{job_no:04d}"

                # extension ordinal is its 1-based index in the full extension list
                ext_global_ordinal = ei + 1

                job = {
                    "job_id": job_id,
                    "browser": bname,
                    "browser_binary": bcfg.get("binary"),  # used by runner_chromium_manual (optional)
                    "extension_name": ext_name,
                    "extension_version": ext_ver,
                    "extension_path": ext_path,
                    "affiliate_link": link,
                    "merchant": "",
                    "extension_ordinal": ext_global_ordinal,
                }

                print(f"\n=== RUN {job_id} ===")
                try:
                    runner(job, master, output)
                except Exception as e:
                    # Log and continue
                    print(f"!! ERROR in {job_id}: {e.__class__.__name__}: {e}")
                time.sleep(1.5)

            # if only this extension was requested, stop after finishing it
            if only_extension and ext_name.lower() == only_extension.lower():
                print(f"Only-extension '{only_extension}' completed. Exiting.")
                return

        # reset start pointers after first browser pass
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
    )
