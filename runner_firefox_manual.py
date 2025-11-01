# runner_firefox_manual.py — manual-browse runner
import time, hashlib
from urllib.parse import urlparse, unquote
from pathlib import Path
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.firefox.options import Options

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


def run_one(job: dict, src_workbook: Path, out_workbook: Path):
    """
    Manual flow:
      1) Open the affiliate link and install the extension.
      2) YOU browse to checkout (login/guest/etc.).
      3) When you're at checkout, press Y — we take the 'before coupon' snapshot.
      4) Click the extension popup; press ENTER — we take the 'after coupon' snapshot and log new tabs.
    """
    ext_ordinal = job.get("extension_ordinal", 0)
    prefix = f"{ext_ordinal}." if ext_ordinal else ""

    opts = Options()
    # opts.add_argument("-headless")  # usually keep visible
    driver = webdriver.Firefox(options=opts)
    try:
        # Install extension temporarily
        driver.install_addon(job["extension_path"], temporary=True)

        # Open the link; YOU take it from here to checkout
        driver.get(job["affiliate_link"])

        # Prompt loop: confirm when you're at checkout
        print("\n=== MANUAL NAVIGATION ===")
        print("Browser opened. Please navigate to CHECKOUT (log in / guest as needed).")
        print("When you are at the CHECKOUT page, type 'y' + Enter to continue.")
        print("Or type 's' + Enter to skip the coupon step for this run.")

        before_coupon_cookies = None
        browser_ver = driver.capabilities.get("browserVersion", "")
        domain = urlparse(driver.current_url or job.get("affiliate_link", "")).netloc

        while True:
            try:
                ans = input("Are you at CHECKOUT now? [y]es / [s]kip / [n]o: ").strip().lower()
            except Exception:
                ans = ""

            if ans in ("y", "yes"):
                # Take the baseline just before you apply coupons
                before_coupon_cookies = [_cookie_frame_full(c) for c in driver.get_cookies()]
                domain = urlparse(driver.current_url or job.get("affiliate_link", "")).netloc
                break

            elif ans in ("s", "skip"):
                try:
                    # only collect cookies if a window is still open
                    handles = driver.window_handles
                    if handles:
                        driver.switch_to.window(handles[0])
                        before_coupon_cookies = [_cookie_frame_full(c) for c in driver.get_cookies()]
                    else:
                        print("No browser window open; proceeding with empty cookie snapshot.")
                        before_coupon_cookies = []
                except Exception as e:
                    print(f"Warning: could not read cookies before skip ({e}). Proceeding empty.")
                    before_coupon_cookies = []

                try:
                    domain = urlparse(driver.current_url or job.get("affiliate_link", "")).netloc
                except Exception:
                    domain = job.get("affiliate_link", "")

                print("Skipping coupon step for this run as requested.")
                after_coupon_cookies = before_coupon_cookies
                new_tabs = []
                goto_comparison_and_write(
                    job, src_workbook, out_workbook,
                    driver, browser_ver, domain,
                    before_coupon_cookies, after_coupon_cookies,
                    new_tabs, prefix
                )
                return

            else:
                print("OK, I'll keep waiting. (Tip: you can press 's' to skip.)")
                time.sleep(5)

        # === Extension popup step ===
        print("\n=== ACTION ===")
        print("Click your extension's Apply/Activate popup now.")
        print("When you've clicked it, press ENTER here.")
        pre_handles = set(driver.window_handles)
        try:
            input()
        except Exception:
            pass

        # allow background tabs to open
        time.sleep(5)
        post_handles = set(driver.window_handles)
        new_handles = list(post_handles - pre_handles)
        new_tabs = []
        for h in new_handles:
            try:
                driver.switch_to.window(h)
                new_tabs.append({"title": driver.title or "", "url": driver.current_url or ""})
            except Exception:
                new_tabs.append({"title": "", "url": ""})

        # switch back if we still can
        try:
            orig = list(pre_handles)[0]
            driver.switch_to.window(orig)
        except Exception:
            pass

        # AFTER snapshot
        after_coupon_cookies = [_cookie_frame_full(c) for c in driver.get_cookies()]

        goto_comparison_and_write(
            job, src_workbook, out_workbook,
            driver, browser_ver, domain,
            before_coupon_cookies, after_coupon_cookies,
            new_tabs, prefix
        )

    finally:
        try:
            driver.quit()
        except Exception:
            pass


def goto_comparison_and_write(job, src_workbook, out_workbook,
                              driver, browser_ver, domain,
                              before_cookies, after_cookies,
                              new_tabs, prefix):
    # Build wide comparison row (raw values) from BEFORE_COUPON vs AFTER_COUPON
    before_targets = _snapshot_targets(before_cookies)
    after_targets = _snapshot_targets(after_cookies)

    def val_before(name):
        v = before_targets.get(name, {}).get("value", "")
        return (prefix + v) if v else v

    def val_after(name):
        v = after_targets.get(name, {}).get("value", "")
        return (prefix + v) if v else v

    include_decoded = False  # set True to add decoded columns too
    wide = {
        "Plugin": job.get("extension_name", ""),
        "Browser": "Firefox",
        "Browser Version": browser_ver,
        "Website": domain,
        "Affiliate Link": job.get("affiliate_link", ""),
    }

    for ck in TARGET_ORDER:
        wide[f"{ck} (Before)"] = val_before(ck)
        wide[f"{ck} (After)"] = val_after(ck)
        if include_decoded:
            wide[f"{ck} (Before, Decoded)"] = unquote(before_targets.get(ck, {}).get("value", "") or "")
            wide[f"{ck} (After, Decoded)"] = unquote(after_targets.get(ck, {}).get("value", "") or "")

    # Diagnostics: target cookie hash diffs + tabs
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
        "Browser": "Firefox",
        "Browser Version": browser_ver,
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
    print("✔ Wrote: Clean_Data + Diagnostics + Cookie Field Comparison (manual mode).")
