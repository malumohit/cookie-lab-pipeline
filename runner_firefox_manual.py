# runner_firefox_manual.py — Firefox manual runner
# DEFAULT: normal window. If privacy prefs set browser.privatebrowsing.autostart=true, runs Private.
# Tweaks implemented:
#   1) LANDING snapshot (immediately after opening the affiliate link).
#   2) Install the extension *right before ACTION* so it cannot pollute the 'Before' snapshot.
#   3) Record Landing/Before/After hosts.
#   4) Capture *only* the 'campaign' cookie for Landing / Before / After.
#   5) Diagnostics logs only 'campaign' change state.

import time, hashlib
from urllib.parse import urlparse
from pathlib import Path
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.firefox.options import Options

from excel_writer import (
    append_clean_data_row,
    append_diagnostics,
    append_cookie_comparison,
)

# ===== Single-target cookie: 'campaign' (case-insensitive exact name) =====
TARGET_NAME = "campaign"

def _is_campaign(raw_name: str) -> bool:
    return isinstance(raw_name, str) and raw_name.lower() == TARGET_NAME

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

def _get_campaign_value(cookies):
    for c in cookies:
        if _is_campaign(c.get("name")):
            return c.get("value") or ""
    return ""

def _get_nav_marker(driver):
    try:
        return driver.execute_script(
            "return (performance.timeOrigin||performance.timing?.navigationStart)||Date.now();"
        )
    except Exception:
        return None

def _observe_redirect_refresh_and_tabs(driver, pre_url, pre_nav_ts, pre_handles, window_sec=6.0):
    t0 = time.time()
    seen_handles = set(pre_handles)
    new_tabs = []
    redirect_url = ""
    refreshed = False
    while (time.time() - t0) < window_sec:
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
        try:
            driver.switch_to.window(list(pre_handles)[0])
        except Exception:
            pass
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
    if not redirect_url and new_tabs:
        redirect_url = new_tabs[0].get("url", "") or ""
    try:
        driver.switch_to.window(list(pre_handles)[0])
    except Exception:
        pass
    return redirect_url, refreshed, new_tabs

def run_one(job: dict, src_workbook: Path, out_workbook: Path):
    """
    DEFAULT: normal window.
    If matrix privacy prefs set Private, Firefox will run in Private and (if specified)
    allow extensions in Private.
    """
    opts = Options()

    # Apply any privacy prefs from matrix.yaml privacy_levels.firefox[*].prefs
    for k, v in (job.get("privacy_prefs") or {}).items():
        opts.set_preference(str(k), v)

    driver = webdriver.Firefox(options=opts)
    try:
        # ---- Open the link FIRST (no extension yet), then take LANDING snapshot ----
        driver.get(job["affiliate_link"])
        landing_host = urlparse(driver.current_url or job.get("affiliate_link", "")).netloc
        landing_cookies = [_cookie_frame_full(c) for c in driver.get_cookies()]

        print("\n=== MANUAL NAVIGATION ===")
        print("Firefox opened. Please navigate to CHECKOUT (log in / guest as needed).")
        print("When you are at the CHECKOUT page, type 'y' + Enter to continue.")
        print("Or type 's' + Enter to skip the coupon step for this run.")

        before_coupon_cookies = None
        popup_seen = ""
        browser_ver = driver.capabilities.get("browserVersion", "") or driver.capabilities.get("version", "")

        while True:
            try:
                ans = input("Are you at CHECKOUT now? [y]es / [s]kip / [n]o: ").strip().lower()
            except Exception:
                ans = ""

            if ans in ("y", "yes"):
                # Take BEFORE snapshot BEFORE installing the extension to avoid contamination.
                before_coupon_cookies = [_cookie_frame_full(c) for c in driver.get_cookies()]
                before_host = urlparse(driver.current_url or job.get("affiliate_link", "")).netloc

                # ---- Install the extension ONLY NOW (right before ACTION) ----
                # This ensures the extension cannot set cookies that appear in 'Before'.
                try:
                    driver.install_addon(job["extension_path"], temporary=True)
                except Exception as e:
                    print(f"Warning: could not install extension before action: {e}")

                # Ask if the extension popup is visible now
                while True:
                    try:
                        q = input("Do you see the extension popup right now? [y]es / [n]o: ").strip().lower()
                    except Exception:
                        q = ""
                    if q in ("y", "yes"):
                        popup_seen = "Yes"; break
                    if q in ("n", "no"):
                        popup_seen = "No"; break
                    print("Please type 'y' or 'n'.")
                break

            elif ans in ("s", "skip"):
                # Even when skipping, we still install right before we write rows (for parity),
                # but After==Before so extension effect is neutralized.
                try:
                    before_coupon_cookies = [_cookie_frame_full(c) for c in driver.get_cookies()]
                except Exception as e:
                    print(f"Warning: could not read cookies before skip ({e}). Proceeding empty.")
                    before_coupon_cookies = []
                before_host = urlparse(driver.current_url or job.get("affiliate_link", "")).netloc

                try:
                    driver.install_addon(job["extension_path"], temporary=True)
                except Exception as e:
                    print(f"Warning: could not install extension before skip-write: {e}")

                print("Skipping coupon step for this run as requested.")
                after_coupon_cookies = before_coupon_cookies
                after_host = before_host
                new_tabs = []
                redirect_url_final = ""
                refreshed = False
                popup_seen = "Skipped"
                goto_comparison_and_write(
                    job, src_workbook, out_workbook,
                    driver, browser_ver,
                    landing_host, before_host, after_host,
                    landing_cookies, before_coupon_cookies, after_coupon_cookies,
                    new_tabs, redirect_url_final, refreshed, popup_seen
                )
                return

            else:
                print("OK, still waiting. (Tip: you can press 's' to skip.)")
                time.sleep(5)

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

        redirect_url, refreshed, new_tabs = _observe_redirect_refresh_and_tabs(
            driver, pre_url, pre_nav_ts, pre_handles, window_sec=float(job.get("redirect_window_sec", 6.0))
        )
        after_coupon_cookies = [_cookie_frame_full(c) for c in driver.get_cookies()]
        after_host = urlparse(driver.current_url or job.get("affiliate_link", "")).netloc

        goto_comparison_and_write(
            job, src_workbook, out_workbook,
            driver, browser_ver,
            landing_host, before_host, after_host,
            landing_cookies, before_coupon_cookies, after_coupon_cookies,
            new_tabs, redirect_url, refreshed, popup_seen
        )

    finally:
        try:
            driver.quit()
        except Exception:
            pass

def goto_comparison_and_write(job, src_workbook, out_workbook,
                              driver, browser_ver,
                              landing_host, before_host, after_host,
                              landing_cookies, before_cookies, after_cookies,
                              new_tabs, redirect_url_final, refreshed, popup_seen):

    # Extract only 'campaign' values at each snapshot
    def _val(cset): 
        for c in cset:
            if _is_campaign(c.get("name")):
                return c.get("value") or ""
        return ""

    landing_campaign = _val(landing_cookies)
    before_campaign  = _val(before_cookies)
    after_campaign   = _val(after_cookies)

    prefix = f"{job.get('extension_ordinal',0)}." if job.get("extension_ordinal") else ""

    wide = {
        "Plugin": job.get("extension_name", ""),
        "Browser": "Firefox",
        "Browser Privacy Level": job.get("privacy_name", ""),
        "Browser Version": browser_ver,
        "Website (Landing)": landing_host,
        "Website (Before)": before_host,
        "Website (After)": after_host,
        "Affiliate Link": job.get("affiliate_link", ""),
        "campaign (Landing)": prefix + landing_campaign if landing_campaign else "",
        "campaign (Before)":  prefix + before_campaign  if before_campaign  else "",
        "campaign (After)":   prefix + after_campaign   if after_campaign   else "",
    }

    # Clean_Data summary
    ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    new_tab_urls   = "; ".join([t.get("url","") for t in new_tabs if t.get("url")])
    new_tab_titles = "; ".join([t.get("title","") for t in new_tabs if t.get("title")])

    clean_row = {
        "Timestamp": ts,
        "Test ID": job.get("job_id", ""),
        "Browser": "Firefox",
        "Browser Privacy Level": job.get("privacy_name", ""),
        "Browser Version": browser_ver,
        "Extension": job.get("extension_name", ""),
        "Extension Version": job.get("extension_version", ""),
        "Merchant (Landing)": landing_host,
        "Merchant (Before)": before_host,
        "Merchant (After)": after_host,
        "Affiliate Link": job.get("affiliate_link", ""),
        "Extension Popup Seen?": popup_seen,
        "Redirect URL": redirect_url_final,
        "Refreshed?": "Yes" if refreshed else "No",
        "New Pages Opened": str(len(new_tabs)),
        "New Tab URLs": new_tab_urls,
        "New Tab Titles": new_tab_titles,
        "Status": "SUCCESS",
        "Failure Reason": "",
        "Notes": "Only-campaign-capture",
        "Redirect Window (s)": str(job.get("redirect_window_sec", 6.0)),
    }

    # Diagnostics: only 'campaign'
    diag_rows = []
    def _hash(v): return _h(v) if v is not None else ""
    b_val = _get_campaign_value(before_cookies)
    a_val = _get_campaign_value(after_cookies)
    b_hash = _hash(b_val)
    a_hash = _hash(a_val)

    if b_val and not a_val:
        change = "REMOVED"
    elif a_val and not b_val:
        change = "ADDED"
    elif b_val and a_val and (b_hash != a_hash):
        change = "CHANGED"
    else:
        change = "UNCHANGED"

    if change != "UNCHANGED":
        diag_rows.append({
            "Test ID": clean_row["Test ID"],
            "Browser": clean_row["Browser"],
            "Browser Version": clean_row["Browser Version"],
            "Extension": clean_row["Extension"],
            "Extension Version": clean_row["Extension Version"],
            "Merchant (Before)": before_host,
            "Merchant (After)": after_host,
            "Affiliate Link": job.get("affiliate_link", ""),
            "Cookie Name": "campaign",
            "Change": change,
            "Before Hash": b_hash or "",
            "After Hash": a_hash or "",
            "Observed At": ts
        })

    append_cookie_comparison(out_workbook, wide)
    append_clean_data_row(src_workbook, out_workbook, clean_row)
    append_diagnostics(out_workbook, diag_rows)
    print("✔ Wrote: Clean_Data + Diagnostics + Cookie Field Comparison (Firefox, campaign-only).")
