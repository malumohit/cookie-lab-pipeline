# runner_firefox_manual.py — manual-browse runner (Firefox)
# DEFAULT: normal window. If privacy prefs set browser.privatebrowsing.autostart=true, runs Private.
# Redirect/refresh watch, dynamic cookie diffs, sanitized headers, popup seen, wildcard targets.

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

# === TARGET COOKIES (case-insensitive, simple '*' wildcards supported) ===
TARGET_ORDER = [
    "NV_MC_LC","NV_MC_FC","NV_ECM_TK_LC",
    "__attentive_utm_param_campaign","__attentive_utm_param_source","__attentive_utm_param_medium",
    "__attentive_utm_param_term","__attentive_utm_param_content",
    "campaign","campaign_id","campaign_date","campaign_source","campaign_medium","campaign_name",
    "utm_source","utm_medium","utm_campaign","utm_term","utm_content",
    "affid","aff_id","affiliate","affiliate_id","affiliate_source","affsource","aff_source","affname",
    "aff_sub","aff_sub2","aff_sub3","aff_sub4","aff_sub5","subid","sub_id",
    "awinaffid","awcid","awcr","aw_referrer","aw_click_id",
    "cjevent","cjData",
    "irclickid","irgwc","irpid","iradid","iradname",
    "sscid","scid",
    "prms","prm_expid","prm_click",
    "gclid","gclsrc","dclid","fbclid","msclkid","ttclid","twclid","yclid",
    "_ga","_ga_*","_gid","_gat","_gat_*","_gcl_au","_gcl_aw","_gcl_dc",
    "_fbp","_fbc","_uetsid","_uetvid","_tt_enable_cookie","_ttp","_pin_unauth","_rdt_uuid",
    "AMCV_","s_cc","s_sq","mbox","mboxEdgeCluster",
    "ref","referrer","source","campaignCode","promo","promocode","coupon","coupon_code",
    "session_id","sessionid","sid",
]

_CANON = {name.lower(): name for name in TARGET_ORDER if not name.endswith("*")}
_PREFIXES = [name[:-1].lower() for name in TARGET_ORDER if name.endswith("*")]

def _is_target_name(raw_name: str) -> str | None:
    if not raw_name:
        return None
    ln = raw_name.lower()
    if ln in _CANON:
        return _CANON[ln]
    for p in _PREFIXES:
        if ln.startswith(p):
            return raw_name
    return None

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
        cname = c.get("name") or ""
        canon = _is_target_name(cname)
        if canon:
            val = c.get("value") or ""
            out[canon] = {"value": val, "hash": _h(val)}
    return out

def _sanitize_cookie_name(name: str) -> str:
    if name is None:
        return "Cookie:UNKNOWN"
    safe = name.replace("\r", " ").replace("\n", " ").replace("\t", " ").strip()
    return f"Cookie:{safe}" if not safe.startswith("Cookie:") else safe

def _before_key(name: str) -> str: return _sanitize_cookie_name(name) + " (Before)"
def _after_key(name: str)  -> str: return _sanitize_cookie_name(name) + " (After)"

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
        # Install extension temporarily
        driver.install_addon(job["extension_path"], temporary=True)

        driver.get(job["affiliate_link"])

        print("\n=== MANUAL NAVIGATION ===")
        print("Firefox opened. Please navigate to CHECKOUT (log in / guest as needed).")
        print("When you are at the CHECKOUT page, type 'y' + Enter to continue.")
        print("Or type 's' + Enter to skip the coupon step for this run.")

        before_coupon_cookies = None
        popup_seen = ""
        browser_ver = driver.capabilities.get("browserVersion", "") or driver.capabilities.get("version", "")
        domain = urlparse(driver.current_url or job.get("affiliate_link", "")).netloc

        while True:
            try:
                ans = input("Are you at CHECKOUT now? [y]es / [s]kip / [n]o: ").strip().lower()
            except Exception:
                ans = ""

            if ans in ("y", "yes"):
                before_coupon_cookies = [_cookie_frame_full(c) for c in driver.get_cookies()]
                domain = urlparse(driver.current_url or job.get("affiliate_link", "")).netloc
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
                popup_seen = "Skipped"
                goto_comparison_and_write(
                    job, src_workbook, out_workbook,
                    driver, browser_ver, domain,
                    before_coupon_cookies, after_coupon_cookies,
                    new_tabs,
                    redirect_url_final, refreshed,
                    popup_seen
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

        goto_comparison_and_write(
            job, src_workbook, out_workbook,
            driver, browser_ver, domain,
            before_coupon_cookies, after_coupon_cookies,
            new_tabs,
            redirect_url, refreshed,
            popup_seen
        )

    finally:
        try:
            driver.quit()
        except Exception:
            pass

def goto_comparison_and_write(job, src_workbook, out_workbook,
                              driver, browser_ver, domain,
                              before_cookies, after_cookies,
                              new_tabs,
                              redirect_url_final, refreshed,
                              popup_seen):
    new_tab_urls = "; ".join([t.get("url","") for t in new_tabs if t.get("url")])
    new_tab_titles = "; ".join([t.get("title","") for t in new_tabs if t.get("title")])

    before_targets = _snapshot_targets(before_cookies)
    after_targets  = _snapshot_targets(after_cookies)

    prefix = f"{job.get('extension_ordinal',0)}." if job.get("extension_ordinal") else ""

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
    for ck in sorted(before_targets.keys() | after_targets.keys(), key=lambda x: x.lower()):
        wide[f"{ck} (Before)"] = val_before(ck)
        wide[f"{ck} (After)"]  = val_after(ck)

    def key(c): return (c["name"], c["domain"], c["path"])
    bmap = {key(c): c for c in before_cookies}
    amap = {key(c): c for c in after_cookies}

    changed_names = set()
    for k in amap.keys() - bmap.keys(): changed_names.add(amap[k]["name"])
    for k in bmap.keys() - amap.keys(): changed_names.add(bmap[k]["name"])
    for k in amap.keys() & bmap.keys():
        if amap[k]["value_hash"] != bmap[k]["value_hash"]:
            changed_names.add(amap[k]["name"])

    for name in sorted(changed_names, key=lambda x: (x or "").lower()):
        if _is_target_name(name):
            continue
        bvals = [c["value"] for c in before_cookies if c["name"] == name]
        avals = [c["value"] for c in after_cookies  if c["name"] == name]
        wide[_before_key(name)] = (prefix + bvals[0]) if bvals else ""
        wide[_after_key(name)]  = (prefix + avals[0]) if avals else ""

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
        "Browser Privacy Level": job.get("privacy_name", ""),
        "Browser Version": browser_ver,
        "Extension": job.get("extension_name", ""),
        "Extension Version": job.get("extension_version", ""),
        "Merchant": domain,
        "Affiliate Link": job.get("affiliate_link", ""),
        "Coupon Applied?": "",
        "Extension Popup Seen?": popup_seen,
        "New Pages Opened": str(len(new_tabs)),
        "Cookies Added (count)": str(len(added)),
        "Cookies Changed (count)": str(len(changed)),
        "Redirect URL": redirect_url_final,
        "Refreshed?": "Yes" if refreshed else "No",
        "New Tab URLs": new_tab_urls,
        "New Tab Titles": new_tab_titles,
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
    for ck in sorted(before_targets.keys() | after_targets.keys(), key=lambda x: x.lower()):
        b_hash = before_targets.get(ck, {}).get("hash")
        a_hash = after_targets.get(ck, {}).get("hash")
        change = "UNCHANGED"
        if ck in before_targets and ck not in after_targets: change = "REMOVED"
        elif ck in after_targets and ck not in before_targets: change = "ADDED"
        elif (b_hash is not None) and (a_hash is not None) and b_hash != a_hash: change = "CHANGED"
        if change != "UNCHANGED":
            diag_rows.append({
                "Test ID": clean_row["Test ID"],
                "Browser": clean_row["Browser"],
                "Browser Version": clean_row["Browser Version"],
                "Extension": job.get("extension_name", ""),
                "Extension Version": job.get("extension_version", ""),
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
            "Extension": job.get("extension_name", ""),
            "Extension Version": job.get("extension_version", ""),
            "Merchant": domain,
            "Affiliate Link": job.get("affiliate_link", ""),
            "Cookie Name": "(new_tab)",
            "Change": tab.get("title",""),
            "Before Hash": "",
            "After Hash": tab.get("url",""),
            "Observed At": ts
        })
    append_diagnostics(out_workbook, diag_rows)

    print("✔ Wrote: Clean_Data + Diagnostics + Cookie Field Comparison (Firefox).")
