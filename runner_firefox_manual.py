# runner_firefox_manual.py — manual-browse runner (with redirect+refresh & dynamic cookie diffs)
# now with sanitized dynamic headers (avoids f-string backslash issues)
# and a "Do you see the extension popup?" prompt recorded to Clean_Data.

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

# ---------- header sanitizing helpers ----------

def _sanitize_cookie_name(name: str) -> str:
    """Turn any cookie name into a safe column prefix (no newlines/tabs)."""
    if name is None:
        return "Cookie:UNKNOWN"
    safe = name.replace("\r", " ").replace("\n", " ").replace("\t", " ").strip()
    return f"Cookie:{safe}" if not safe.startswith("Cookie:") else safe

def _before_key(name: str) -> str:
    return _sanitize_cookie_name(name) + " (Before)"

def _after_key(name: str) -> str:
    return _sanitize_cookie_name(name) + " (After)"

# ---------- navigation/redirect helpers ----------

def _get_nav_marker(driver):
    """Rough navigation/refresh timestamp marker."""
    try:
        return driver.execute_script(
            "return (performance.timeOrigin||performance.timing?.navigationStart)||Date.now();"
        )
    except Exception:
        return None

def _observe_redirect_refresh_and_tabs(driver, pre_url, pre_nav_ts, pre_handles, window_sec=6.0):
    """
    Watch briefly after the user clicks the extension button to catch:
      - same-tab redirect (URL change)
      - same-tab refresh (navigation timestamp change without URL change)
      - new background tabs (collect title+URL)
    """
    t0 = time.time()
    seen_handles = set(pre_handles)
    new_tabs = []
    redirect_url = ""
    refreshed = False

    while (time.time() - t0) < window_sec:
        # detect new tabs
        try:
            handles = set(driver.window_handles)
        except Exception:
            handles = set()
        for h in list(handles - seen_handles):
            try:
                driver.switch_to.window(h)
                new_tabs.append({"title": driver.title or "", "url": driver.current_url or ""})
            except Exception:
                new_tabs.append({"title": "", "url": ""})
            finally:
                seen_handles.add(h)

        # go back to original if possible
        try:
            driver.switch_to.window(list(pre_handles)[0])
        except Exception:
            pass

        # detect same-tab redirect/refresh
        try:
            curr_url = driver.current_url or ""
        except Exception:
            curr_url = ""
        nav_ts = _get_nav_marker(driver)

        if curr_url and pre_url and curr_url != pre_url and not redirect_url:
            redirect_url = curr_url

        if nav_ts is not None and pre_nav_ts is not None and nav_ts != pre_nav_ts:
            if (not redirect_url) and (curr_url == pre_url):
                refreshed = True

        time.sleep(0.2)

    # prefer first new-tab URL as "redirect" if no same-tab redirect seen
    if not redirect_url and new_tabs:
        redirect_url = new_tabs[0].get("url", "") or ""

    # return focus
    try:
        driver.switch_to.window(list(pre_handles)[0])
    except Exception:
        pass

    return redirect_url, refreshed, new_tabs

# ---------- main runner ----------

def run_one(job: dict, src_workbook: Path, out_workbook: Path):
    """
    Manual flow:
      1) Open the affiliate link and install the extension.
      2) YOU browse to checkout (login/guest/etc.).
      3) At checkout, we ask whether you SEE the extension popup (recorded).
      4) You press ENTER after clicking the popup (or icon) — we sample redirect/refresh for ~6s,
         take 'after coupon' snapshot, and log new tabs.
    """
    ext_ordinal = job.get("extension_ordinal", 0)
    prefix = f"{ext_ordinal}." if ext_ordinal else ""

    opts = Options()
    # opts.add_argument("-headless")
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
        popup_seen = ""  # record user's answer
        browser_ver = driver.capabilities.get("browserVersion", "") or driver.capabilities.get("version", "")
        domain = urlparse(driver.current_url or job.get("affiliate_link", "")).netloc

        while True:
            try:
                ans = input("Are you at CHECKOUT now? [y]es / [s]kip / [n]o: ").strip().lower()
            except Exception:
                ans = ""

            if ans in ("y", "yes"):
                # Take baseline cookies
                before_coupon_cookies = [_cookie_frame_full(c) for c in driver.get_cookies()]
                domain = urlparse(driver.current_url or job.get("affiliate_link", "")).netloc

                # NEW: Ask if the extension popup is already visible
                while True:
                    try:
                        q = input("Do you see the extension popup right now? [y]es / [n]o: ").strip().lower()
                    except Exception:
                        q = ""
                    if q in ("y", "yes"):
                        popup_seen = "Yes"
                        break
                    if q in ("n", "no"):
                        popup_seen = "No"
                        break
                    print("Please type 'y' or 'n'.")
                break

            elif ans in ("s", "skip"):
                try:
                    before_coupon_cookies = [_cookie_frame_full(c) for c in driver.get_cookies()]
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
                redirect_url_final = ""
                refreshed = False
                popup_seen = "Skipped"  # NEW
                goto_comparison_and_write(
                    job, src_workbook, out_workbook,
                    driver, browser_ver, domain,
                    before_coupon_cookies, after_coupon_cookies,
                    new_tabs, prefix,
                    redirect_url_final, refreshed,
                    popup_seen
                )
                return

            else:
                print("OK, I'll keep waiting. (Tip: you can press 's' to skip.)")
                time.sleep(5)

        # === Extension popup step ===
        print("\n=== ACTION ===")
        if popup_seen == "Yes":
            print("Great — click the popup now to apply/activate.")
        else:
            print("No popup? Click the extension’s toolbar button to apply/activate.")
        print("When you've clicked it, press ENTER here.")
        pre_handles = set(driver.window_handles)
        pre_url = driver.current_url or ""
        pre_nav_ts = _get_nav_marker(driver)

        try:
            input()
        except Exception:
            pass

        # Observe redirects/refresh/new tabs
        redirect_url, refreshed, new_tabs = _observe_redirect_refresh_and_tabs(
            driver, pre_url, pre_nav_ts, pre_handles, window_sec=float(job.get("redirect_window_sec", 6.0))
        )

        # AFTER snapshot
        after_coupon_cookies = [_cookie_frame_full(c) for c in driver.get_cookies()]

        goto_comparison_and_write(
            job, src_workbook, out_workbook,
            driver, browser_ver, domain,
            before_coupon_cookies, after_coupon_cookies,
            new_tabs, prefix,
            redirect_url, refreshed,
            popup_seen
        )

    finally:
        try:
            driver.quit()
        except Exception:
            pass

# ---------- output building ----------

def goto_comparison_and_write(job, src_workbook, out_workbook,
                              driver, browser_ver, domain,
                              before_cookies, after_cookies,
                              new_tabs, prefix,
                              redirect_url_final, refreshed,
                              popup_seen):
    # Build semicolon-joined lists of any new tabs
    new_tab_urls = "; ".join([t.get("url","") for t in new_tabs if t.get("url")])
    new_tab_titles = "; ".join([t.get("title","") for t in new_tabs if t.get("title")])

    # Build wide comparison row for TARGETS (raw values) + dynamic for all changed cookies
    before_targets = _snapshot_targets(before_cookies)
    after_targets  = _snapshot_targets(after_cookies)

    def val_before(name):
        return (prefix + (before_targets.get(name, {}).get("value", "") or "")) if before_targets.get(name) else ""

    def val_after(name):
        return (prefix + (after_targets.get(name, {}).get("value", "") or "")) if after_targets.get(name) else ""

    wide = {
        "Plugin": job.get("extension_name", ""),
        "Browser": "Firefox",
        "Browser Version": browser_ver,
        "Website": domain,
        "Affiliate Link": job.get("affiliate_link", ""),
    }
    # TARGETS first
    for ck in TARGET_ORDER:
        wide[f"{ck} (Before)"] = val_before(ck)
        wide[f"{ck} (After)"]  = val_after(ck)

    # Dynamic: add all NON-TARGET cookies that changed (added/removed/value-changed)
    def key(c): return (c["name"], c["domain"], c["path"])
    bmap = {key(c): c for c in before_cookies}
    amap = {key(c): c for c in after_cookies}

    changed_names = set()
    for k in amap.keys() - bmap.keys():
        changed_names.add(amap[k]["name"])  # added
    for k in bmap.keys() - amap.keys():
        changed_names.add(bmap[k]["name"])  # removed
    for k in amap.keys() & bmap.keys():
        if amap[k]["value_hash"] != bmap[k]["value_hash"]:
            changed_names.add(amap[k]["name"])

    for name in sorted(changed_names):
        if name in TARGET_SET:
            continue
        bvals = [c["value"] for c in before_cookies if c["name"] == name]
        avals = [c["value"] for c in after_cookies  if c["name"] == name]
        wide[_before_key(name)] = (prefix + bvals[0]) if bvals else ""
        wide[_after_key(name)]  = (prefix + avals[0]) if avals else ""

    # Diagnostics/counts
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
        "Extension Popup Seen?": popup_seen,      # NEW
        "New Pages Opened": str(len(new_tabs)),
        "Cookies Added (count)": str(len(added)),
        "Cookies Changed (count)": str(len(changed)),
        "Redirect URL": redirect_url_final,       # NEW
        "Refreshed?": "Yes" if refreshed else "No",
        "New Tab URLs": new_tab_urls,             # NEW
        "New Tab Titles": new_tab_titles,         # NEW
        "HAR Path": "",
        "Screenshots": "",
        "Status": "SUCCESS",
        "Failure Reason": "",
        "Notes": f"CookieComparisonRow=1; Tabs={len(new_tabs)}",
        "Redirect Window (s)": str(job.get("redirect_window_sec", 6.0)),
    }

    append_cookie_comparison(out_workbook, wide)
    append_clean_data_row(src_workbook, out_workbook, clean_row)

    diag_rows = []
    for ck in TARGET_ORDER:
        b = next((c for c in before_cookies if c["name"] == ck), None)
        a = next((c for c in after_cookies  if c["name"] == ck), None)
        b_hash = b and b.get("value_hash")
        a_hash = a and a.get("value_hash")
        change = "UNCHANGED"
        if b and not a: change = "REMOVED"
        elif a and not b: change = "ADDED"
        elif b and a and b_hash != a_hash: change = "CHANGED"
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
                "Before Hash": b_hash or "",
                "After Hash": a_hash or "",
                "Observed At": ts
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
            "Change": tab.get("title",""),
            "Before Hash": "",
            "After Hash": tab.get("url",""),
            "Observed At": ts
        })
    append_diagnostics(out_workbook, diag_rows)

    print("✔ Wrote: Clean_Data + Diagnostics + Cookie Field Comparison (manual Firefox; redirect/refresh + dynamic cookies).")
