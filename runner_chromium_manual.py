# runner_chromium_manual.py — manual-browse runner for Chrome/Edge/Brave/Opera
import time, hashlib, tempfile, shutil, os
from urllib.parse import urlparse, unquote
from pathlib import Path
from datetime import datetime

from selenium import webdriver
from selenium.common.exceptions import NoSuchWindowException
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.edge.options import Options as EdgeOptions

from excel_writer import (
    append_clean_data_row,
    append_diagnostics,
    append_cookie_comparison,
)

TARGET_ORDER = [
    "NV_MC_LC",
    "NV_MC_FC",
    "__attentive_utm_param_campaign",
    "__attentive_utm_param_source",
    "NV_ECM_TK_LC",
]
TARGET_SET = set(TARGET_ORDER)
CHROMIUM_FAMILY = ("chrome", "edge", "brave", "opera")


def _h(v: str) -> str:
    return hashlib.sha256((v or "").encode("utf-8")).hexdigest()[:16]


def _cookie_frame_full(c: dict) -> dict:
    return {
        "name": c.get("name"),
        "value": c.get("value") or "",
        "value_hash": _h(c.get("value")),
        "domain": c.get("domain"),
        "path": c.get("path"),
        "expiry": c.get("expiry"),
        "httpOnly": c.get("httpOnly"),
        "secure": c.get("secure"),
        "sameSite": c.get("sameSite"),
    }


def _snapshot_targets(cookies):
    out = {}
    for c in cookies:
        n = c["name"]
        if n in TARGET_SET:
            out[n] = {"value": c["value"], "hash": c["value_hash"]}
    return out


def _ensure_window_open(driver):
    """Guarantee at least one open window for nav/cookie ops."""
    try:
        handles = driver.window_handles
        if not handles:
            driver.switch_to.new_window("window")
            return
        driver.switch_to.window(handles[0])
    except Exception:
        try:
            driver.switch_to.new_window("window")
        except Exception:
            pass


def _mk_chromium_driver(browser_name: str,
                        extension_path: str | None,
                        binary_path: str | None,
                        profile_dir: Path | None):
    """Create a Chrome/Edge/Brave/Opera driver with optional extension + clean profile."""
    b = (browser_name or "chrome").lower()

    def _apply_common(opts):
        # Clean, isolated temporary profile
        if profile_dir:
            opts.add_argument(f"--user-data-dir={str(profile_dir)}")
            opts.add_argument("--no-first-run")
            opts.add_argument("--no-default-browser-check")
        # Stability flags (esp. in VMs/CI)
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--no-sandbox")
        # Load extension (unpacked dir or .crx)
        if extension_path:
            if os.path.isdir(extension_path):
                opts.add_argument(f"--load-extension={extension_path}")
            elif extension_path.lower().endswith(".crx"):
                opts.add_extension(extension_path)
            else:
                print(f"[WARN] Unknown extension path type for Chromium: {extension_path}")

    if b == "edge":
        opts = EdgeOptions()
        _apply_common(opts)
        return webdriver.Edge(options=opts)

    # Chrome / Brave / Opera use Chrome driver API with different binaries if provided
    opts = ChromeOptions()
    if binary_path:
        opts.binary_location = binary_path
    _apply_common(opts)
    return webdriver.Chrome(options=opts)


def run_one(job: dict, src_workbook: Path, out_workbook: Path):
    """
    Manual flow (Chromium family):
      1) Launch target Chromium browser with (optional) extension in a fresh temp profile.
      2) Open affiliate link; YOU browse to checkout.
      3) On checkout, press 'y' to take BEFORE snapshot.
      4) Click extension popup; press ENTER to take AFTER snapshot + log new tabs.
    """
    browser = (job.get("browser") or "chrome").lower()
    assert browser in CHROMIUM_FAMILY, f"Unsupported Chromium browser: {browser}"

    ext_ordinal = job.get("extension_ordinal", 0)
    prefix = f"{ext_ordinal}." if ext_ordinal else ""
    extension_path = job.get("extension_path")
    binary_path = job.get("browser_binary")  # optional from matrix.yaml

    # Fresh temporary profile (isolated cookies/cache)
    profile_dir = Path(tempfile.mkdtemp(prefix=f"{browser}_profile_"))

    driver = _mk_chromium_driver(browser, extension_path, binary_path, profile_dir)
    try:
        # Navigate to link; retry once if window vanished
        url = job["affiliate_link"]
        for attempt in (1, 2):
            try:
                _ensure_window_open(driver)
                driver.get(url)
                break
            except NoSuchWindowException:
                if attempt == 1:
                    try:
                        driver.quit()
                    except Exception:
                        pass
                    driver = _mk_chromium_driver(browser, extension_path, binary_path, profile_dir)
                    continue
                else:
                    raise

        print("\n=== MANUAL NAVIGATION (Chromium) ===")
        print("Please navigate to CHECKOUT (log in / guest as needed).")
        print("When you are at CHECKOUT, type 'y' + Enter. Type 's' to skip.")

        try:
            caps = driver.capabilities or {}
            browser_version = caps.get("browserVersion") or caps.get("version") or ""
        except Exception:
            browser_version = ""
        domain = urlparse(driver.current_url or url).netloc

        before_coupon_cookies = None
        while True:
            try:
                ans = input("Are you at CHECKOUT now? [y]es / [s]kip / [n]o: ").strip().lower()
            except Exception:
                ans = ""

            if ans in ("y", "yes"):
                _ensure_window_open(driver)
                before_coupon_cookies = [_cookie_frame_full(c) for c in driver.get_cookies()]
                domain = urlparse(driver.current_url or url).netloc
                break

            elif ans in ("s", "skip"):
                try:
                    _ensure_window_open(driver)
                    before_coupon_cookies = [_cookie_frame_full(c) for c in driver.get_cookies()]
                except Exception as e:
                    print(f"[WARN] could not read cookies before skip: {e}")
                    before_coupon_cookies = []
                try:
                    domain = urlparse(driver.current_url or url).netloc
                except Exception:
                    domain = urlparse(url).netloc

                print("Skipping coupon step for this run as requested.")
                after_coupon_cookies = before_coupon_cookies
                new_tabs = []
                _write_all(job, src_workbook, out_workbook, browser, browser_version, domain,
                           before_coupon_cookies, after_coupon_cookies, new_tabs, prefix)
                return

            else:
                print("OK, waiting… (press 's' to skip)")
                time.sleep(5)

        # === Human popup step ===
        print("\n=== ACTION ===")
        print("Click your extension's Apply/Activate popup now, then press ENTER here.")
        pre_handles = set(driver.window_handles)
        try:
            input()
        except Exception:
            pass

        time.sleep(5)  # allow background tabs to open
        post_handles = set(driver.window_handles)
        new_handles = list(post_handles - pre_handles)
        new_tabs = []
        for h in new_handles:
            try:
                driver.switch_to.window(h)
                new_tabs.append({"title": driver.title or "", "url": driver.current_url or ""})
            except Exception:
                new_tabs.append({"title": "", "url": ""})

        # switch back
        try:
            orig = list(pre_handles)[0]
            driver.switch_to.window(orig)
        except Exception:
            pass

        _ensure_window_open(driver)
        after_coupon_cookies = [_cookie_frame_full(c) for c in driver.get_cookies()]

        _write_all(job, src_workbook, out_workbook, browser, browser_version, domain,
                   before_coupon_cookies, after_coupon_cookies, new_tabs, prefix)

    finally:
        try:
            driver.quit()
        except Exception:
            pass
        # remove temp profile dir
        try:
            shutil.rmtree(profile_dir, ignore_errors=True)
        except Exception:
            pass


def _write_all(job, src_workbook, out_workbook, browser, browser_version, domain,
               before_cookies, after_cookies, new_tabs, prefix):
    before_targets = _snapshot_targets(before_cookies)
    after_targets = _snapshot_targets(after_cookies)

    def val_before(name):
        v = before_targets.get(name, {}).get("value", "")
        return (prefix + v) if v else v

    def val_after(name):
        v = after_targets.get(name, {}).get("value", "")
        return (prefix + v) if v else v

    include_decoded = False
    wide = {
        "Plugin": job.get("extension_name", ""),
        "Browser": browser.capitalize(),
        "Browser Version": browser_version,
        "Website": domain,
        "Affiliate Link": job.get("affiliate_link", ""),
    }
    for ck in TARGET_ORDER:
        wide[f"{ck} (Before)"] = val_before(ck)
        wide[f"{ck} (After)"] = val_after(ck)
        if include_decoded:
            wide[f"{ck} (Before, Decoded)"] = unquote(before_targets.get(ck, {}).get("value", "") or "")
            wide[f"{ck} (After, Decoded)"] = unquote(after_targets.get(ck, {}).get("value", "") or "")

    # diffs + counts
    def key(c): return (c["name"], c["domain"], c["path"])
    bmap = {key(c): c for c in before_cookies}
    amap = {key(c): c for c in after_cookies}
    added = [amap[k] for k in amap.keys() - bmap.keys()]
    changed = []
    for k in amap.keys() & bmap.keys():
        if amap[k]["value_hash"] != bmap[k]["value_hash"]:
            changed.append({"before": bmap[k], "after": amap[k]})

    ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    clean_row = {
        "Timestamp": ts,
        "Test ID": job.get("job_id", ""),
        "Browser": browser.capitalize(),
        "Browser Version": browser_version,
        "Extension": job.get("extension_name", ""),
        "Extension Version": job.get("extension_version", ""),
        "Merchant": domain,
        "Affiliate Link": job.get("affiliate_link", ""),
        "Coupon Applied?": "",
        "New Pages Opened": str(len(new_tabs)),
        "Cookies Added (count)": str(len(added)),
        "Cookies Changed (count)": str(len(changed)),
        "HAR Path": "",
        "Screenshots": "",
        "Status": "SUCCESS",
        "Failure Reason": "",
        "Notes": f"CookieComparisonRow=1; Tabs={len(new_tabs)}",
    }

    append_cookie_comparison(out_workbook, wide)
    append_clean_data_row(src_workbook, out_workbook, clean_row)

    diag_rows = []
    for ck in TARGET_ORDER:
        b = before_targets.get(ck, {})
        a = after_targets.get(ck, {})
        change = "UNCHANGED"
        if b and not a:
            change = "REMOVED"
        elif a and not b:
            change = "ADDED"
        elif b and a and b.get("hash") != a.get("hash"):
            change = "CHANGED"
        if change != "UNCHANGED":
            diag_rows.append({
                "Test ID": clean_row["Test ID"],
                "Browser": clean_row["Browser"],
                "Browser Version": clean_row["Browser Version"],
                "Extension": clean_row["Extension"],
                "Extension Version": clean_row["Extension Version"],
                "Merchant": domain,
                "Affiliate Link": job.get("affiliate_link", ""),
                "Cookie Name": ck,
                "Change": change,
                "Before Hash": b.get("hash", ""),
                "After Hash": a.get("hash", ""),
                "Observed At": ts,
            })
    for tab in new_tabs:
        diag_rows.append({
            "Test ID": clean_row["Test ID"],
            "Browser": clean_row["Browser"],
            "Browser Version": clean_row["Browser Version"],
            "Extension": clean_row["Extension"],
            "Extension Version": clean_row["Extension Version"],
            "Merchant": domain,
            "Affiliate Link": job.get("affiliate_link", ""),
            "Cookie Name": "(new_tab)",
            "Change": tab.get("title", ""),
            "Before Hash": "",
            "After Hash": tab.get("url", ""),
            "Observed At": ts,
        })

    append_diagnostics(out_workbook, diag_rows)
    print(f"✔ Wrote: Clean_Data + Diagnostics + Cookie Field Comparison ({browser}).")
