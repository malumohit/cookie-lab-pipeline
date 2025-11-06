# runner_chromium_manual.py — minimal Chromium runner (Chrome/Edge/Brave/Opera)
# No extension auto-loading. You handle the extension manually.
# Fresh temp profile per run; redirect/refresh/new-tab watch; cookie diffs; writes to Excel.

import time
import hashlib
import tempfile
import shutil
from urllib.parse import urlparse
from pathlib import Path
from datetime import datetime

from selenium import webdriver
from selenium.common.exceptions import NoSuchWindowException, SessionNotCreatedException
from selenium.webdriver.chrome.options import Options as ChromeOptions

# Optional fallback to webdriver_manager if Selenium Manager struggles
try:
    from selenium.webdriver.chrome.service import Service as ChromeService
    from webdriver_manager.chrome import ChromeDriverManager
    _WDM_AVAILABLE = True
except Exception:
    _WDM_AVAILABLE = False

from excel_writer import append_clean_data_row, append_diagnostics, append_cookie_comparison

# ===== TARGET COOKIES (case-insensitive; '*' = prefix wildcard; names ending with '_' treated as prefix, e.g., 'AMCV_') =====
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

# Build case-insensitive canonical map + wildcard/prefix lists
_CANON_EXACT = {name.lower(): name for name in TARGET_ORDER if not name.endswith("*") and not name.endswith("_")}
_PREFIXES = []
for name in TARGET_ORDER:
    if name.endswith("*"):
        _PREFIXES.append(name[:-1].lower())  # wildcard prefix e.g. "_ga_"
    elif name.endswith("_"):
        _PREFIXES.append(name.lower())       # treat trailing underscore as prefix (e.g., "AMCV_")

def _is_target_name(raw_name: str) -> str | None:
    """
    Return canonical target key if raw_name matches (case-insensitive).
    '*' entries act as 'starts with'. Entries ending with '_' also treated as 'starts with'.
    For wildcard/prefix matches, return the ACTUAL cookie name so each becomes its own column.
    """
    if not raw_name:
        return None
    ln = raw_name.lower()
    if ln in _CANON_EXACT:
        return _CANON_EXACT[ln]
    for p in _PREFIXES:
        if ln.startswith(p):
            return raw_name  # keep actual name as column (e.g., _ga_XXXX)
    return None

# ===== Cookie helpers =====

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
    """Map: canonical_name -> {'value','hash'} including wildcard expansions."""
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
    safe = name.replace("\r"," ").replace("\n"," ").replace("\t"," ").strip()
    return safe if safe.startswith("Cookie:") else f"Cookie:{safe}"

def _before_key(name: str) -> str: return _sanitize_cookie_name(name) + " (Before)"
def _after_key(name: str)  -> str: return _sanitize_cookie_name(name) + " (After)"

# ===== Nav helpers =====

def _get_nav_marker(driver):
    try:
        return driver.execute_script(
            "return (performance.timeOrigin||performance.timing?.navigationStart)||Date.now();"
        )
    except Exception:
        return None

def _observe_redirect_refresh_and_tabs(driver, pre_url, pre_nav_ts, pre_handles, window_sec=6.0):
    """Watch briefly to catch same-tab redirect, refresh, and new tabs."""
    t0 = time.time()
    seen_handles = set(pre_handles)
    new_tabs, redirect_url, refreshed = [], "", False

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

        # return focus to original
        try:
            driver.switch_to.window(list(pre_handles)[0])
        except Exception:
            pass

        # detect redirect/refresh in same tab
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

# ===== Driver =====

def _make_driver(browser_binary: str | None, privacy_flags, privacy_prefs):
    """Minimal Chrome launcher. No extensions. Fresh temp profile each run."""
    opts = ChromeOptions()
    profile_dir = Path(tempfile.mkdtemp(prefix="chrome_profile_"))
    opts.add_argument(f"--user-data-dir={str(profile_dir)}")

    if browser_binary:
        opts.binary_location = browser_binary

    # QoL
    opts.add_argument("--disable-backgrounding-occluded-windows")
    opts.add_argument("--disable-notifications")
    opts.add_argument("--no-first-run")
    opts.add_argument("--no-default-browser-check")

    for f in (privacy_flags or []):
        opts.add_argument(f)

    if privacy_prefs:
        opts.add_experimental_option("prefs", privacy_prefs)

    # Try native Selenium Manager first, fallback to webdriver_manager if needed
    try:
        driver = webdriver.Chrome(options=opts)
        driver._temp_profile_dir = profile_dir
        return driver
    except SessionNotCreatedException:
        if not _WDM_AVAILABLE:
            raise
        service = ChromeService(ChromeDriverManager().install())
        driver = webdriver.Chrome(options=opts, service=service)
        driver._temp_profile_dir = profile_dir
        return driver

# ===== Main (called by pipeline) =====

def run_one(job: dict, src_workbook: Path, out_workbook: Path):
    """
    Flow:
      1) Launch Chrome at the link (no extension auto-load).
      2) You manually load/operate the extension (if needed).
      3) At checkout prompt, capture BEFORE cookies.
      4) You click popup/toolbar, we watch briefly, then capture AFTER cookies.
      5) Write to Excel and exit.
    """
    driver = _make_driver(
        job.get("browser_binary"),
        job.get("privacy_flags") or [],
        job.get("privacy_prefs") or {},
    )
    temp_profile = getattr(driver, "_temp_profile_dir", None)

    try:
        url = job["affiliate_link"]

        # Navigate (retry once if window disappeared)
        for attempt in (1, 2):
            try:
                driver.get(url)
                break
            except NoSuchWindowException:
                if attempt == 1:
                    try: driver.quit()
                    except Exception: pass
                    driver = _make_driver(
                        job.get("browser_binary"),
                        job.get("privacy_flags") or [],
                        job.get("privacy_prefs") or {},
                    )
                    temp_profile = getattr(driver, "_temp_profile_dir", None)
                    continue
                else:
                    raise

        print("\n=== MANUAL NAVIGATION ===")
        print("Chrome is open at the link. Load/use the extension manually as needed.")
        print("Browse yourself to the CHECKOUT page.")
        print("When you are at CHECKOUT: type 'y' + Enter to continue (or 's' to skip).")

        before_coupon_cookies = None
        popup_seen = ""  # your answer recorded below
        caps = driver.capabilities or {}
        browser_ver = caps.get("browserVersion") or caps.get("version") or ""
        domain = urlparse(driver.current_url or url).netloc

        # Wait for checkout confirmation
        while True:
            try:
                ans = input("Are you at CHECKOUT now? [y]es / [s]kip / [n]o: ").strip().lower()
            except Exception:
                ans = ""

            if ans in ("y", "yes"):
                before_coupon_cookies = [_cookie_frame_full(c) for c in driver.get_cookies()]
                domain = urlparse(driver.current_url or url).netloc

                # Ask if the extension popup is visible now
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
                    domain = urlparse(driver.current_url or url).netloc
                except Exception:
                    domain = url
                print("Skipping coupon step for this run as requested.")
                after_coupon_cookies = before_coupon_cookies
                new_tabs = []
                redirect_url = ""
                refreshed = False
                popup_seen = "Skipped"
                _write_rows(job, src_workbook, out_workbook, driver, browser_ver, domain,
                            before_coupon_cookies, after_coupon_cookies, new_tabs,
                            redirect_url, refreshed, popup_seen)
                return

            else:
                print("OK, still waiting. (Tip: press 's' to skip.)")
                time.sleep(4)

        # === Your action step ===
        print("\n=== ACTION ===")
        print("If a popup is visible, click it now; otherwise click the extension's toolbar button.")
        print("Press ENTER here right after you do that.")
        pre_handles = set(driver.window_handles)
        pre_url = driver.current_url or ""
        pre_nav_ts = _get_nav_marker(driver)
        try:
            input()
        except Exception:
            pass

        # Observe short window for redirect/refresh/new tabs, then AFTER cookies
        redirect_url, refreshed, new_tabs = _observe_redirect_refresh_and_tabs(
            driver, pre_url, pre_nav_ts, pre_handles, window_sec=float(job.get("redirect_window_sec", 6.0))
        )
        after_coupon_cookies = [_cookie_frame_full(c) for c in driver.get_cookies()]

        _write_rows(job, src_workbook, out_workbook, driver, browser_ver, domain,
                    before_coupon_cookies, after_coupon_cookies, new_tabs,
                    redirect_url, refreshed, popup_seen)

    finally:
        try:
            driver.quit()
        except Exception:
            pass
        # delete temp profile to avoid cross-run leakage
        if temp_profile:
            try:
                shutil.rmtree(temp_profile, ignore_errors=True)
            except Exception:
                pass

# ===== Output builders =====

def _write_rows(job, src_workbook, out_workbook, driver, browser_ver, domain,
                before_cookies, after_cookies, new_tabs, redirect_url_final, refreshed, popup_seen):
    prefix = f"{job.get('extension_ordinal',0)}." if job.get("extension_ordinal") else ""

    new_tab_urls   = "; ".join([t.get("url","")   for t in new_tabs if t.get("url")])
    new_tab_titles = "; ".join([t.get("title","") for t in new_tabs if t.get("title")])

    before_targets = _snapshot_targets(before_cookies)
    after_targets  = _snapshot_targets(after_cookies)

    def val_before(name): return (prefix + (before_targets.get(name, {}).get("value","") or "")) if name in before_targets else ""
    def val_after(name):  return (prefix + (after_targets.get(name,  {}).get("value","") or "")) if name in after_targets  else ""

    wide = {
        "Plugin": job.get("extension_name",""),
        "Browser": job.get("browser","Chromium"),
        "Browser Privacy Level": job.get("privacy_name",""),
        "Browser Version": browser_ver,
        "Website": domain,
        "Affiliate Link": job.get("affiliate_link",""),
    }

    # Include union of target keys (so wildcard targets e.g. _ga_XXXX show as concrete columns)
    for ck in sorted(before_targets.keys() | after_targets.keys(), key=lambda x: x.lower()):
        wide[f"{ck} (Before)"] = val_before(ck)
        wide[f"{ck} (After)"]  = val_after(ck)

    # Also include all NON-target cookies that changed (added/removed/value-changed)
    def key(c): return (c["name"], c["domain"], c["path"])
    bmap = {key(c): c for c in before_cookies}
    amap = {key(c): c for c in after_cookies}

    changed_names = set()
    for k in amap.keys() - bmap.keys(): changed_names.add(amap[k]["name"])  # added
    for k in bmap.keys() - amap.keys(): changed_names.add(bmap[k]["name"])  # removed
    for k in amap.keys() & bmap.keys():
        if amap[k]["value_hash"] != bmap[k]["value_hash"]:
            changed_names.add(amap[k]["name"])

    # Only add non-targets here (targets already in the section above)
    for name in sorted(changed_names, key=lambda x: (x or "").lower()):
        if _is_target_name(name):
            continue
        bvals = [c["value"] for c in before_cookies if c["name"] == name]
        avals = [c["value"] for c in after_cookies  if c["name"] == name]
        wide[_before_key(name)] = (prefix + bvals[0]) if bvals else ""
        wide[_after_key(name)]  = (prefix + avals[0]) if avals else ""

    # Diagnostics + Clean_Data
    added = [amap[k] for k in amap.keys() - bmap.keys()]
    changed = []
    for k in amap.keys() & bmap.keys():
        if amap[k]["value_hash"] != bmap[k]["value_hash"]:
            changed.append({"before": bmap[k], "after": amap[k]})

    ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    clean_row = {
        "Timestamp": ts,
        "Test ID": job.get("job_id",""),
        "Browser": job.get("browser","Chromium"),
        "Browser Privacy Level": job.get("privacy_name",""),
        "Browser Version": browser_ver,
        "Extension": job.get("extension_name",""),
        "Extension Version": job.get("extension_version",""),
        "Merchant": domain,
        "Affiliate Link": job.get("affiliate_link",""),
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

    # Diagnostics: record any target cookie changes + new tab info
    diag_rows = []
    for ck in sorted(before_targets.keys() | after_targets.keys(), key=lambda x: x.lower()):
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
                "Affiliate Link": job.get("affiliate_link",""),
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
            "Affiliate Link": job.get("affiliate_link",""),
            "Cookie Name": "(new_tab)",
            "Change": tab.get("title",""),
            "Before Hash": "",
            "After Hash": tab.get("url",""),
            "Observed At": ts
        })
    append_diagnostics(out_workbook, diag_rows)

    print("✔ Wrote: Clean_Data + Diagnostics + Cookie Field Comparison (Chromium minimal, wildcard targets).")
