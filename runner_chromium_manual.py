# runner_chromium_manual.py — manual-browse runner (Chrome/Edge/Brave/Opera)
# FIX: remove webdriver-manager `path=` arg (works with older versions).
# Still: pins driver via webdriver-manager, loads unpacked extension, avoids Selenium Manager hang,
# tracks privacy level, redirect/refresh, dynamic cookie diffs, and "Extension Popup Seen?".

import os
import json
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
from selenium.webdriver.chrome.service import Service as ChromeService
try:
    from selenium.webdriver.edge.options import Options as EdgeOptions
    from selenium.webdriver.edge.service import Service as EdgeService
except Exception:
    EdgeOptions = None
    EdgeService = None

# webdriver-manager (no `path=` usage to support older versions)
try:
    from webdriver_manager.chrome import ChromeDriverManager
    from webdriver_manager.microsoft import EdgeChromiumDriverManager
    _WDM_OK = True
except Exception:
    _WDM_OK = False

from excel_writer import append_clean_data_row, append_diagnostics, append_cookie_comparison

# ===== TARGET COOKIES =========================================================
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
_CANON = {n.lower(): n for n in TARGET_ORDER if not n.endswith("*")}
_PREFIXES = [n[:-1].lower() for n in TARGET_ORDER if n.endswith("*")]

def _is_target_name(n: str|None)->str|None:
    if not n: return None
    ln = n.lower()
    if ln in _CANON: return _CANON[ln]
    for p in _PREFIXES:
        if ln.startswith(p): return n
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
        nm = c.get("name") or ""
        canon = _is_target_name(nm)
        if canon:
            val = c.get("value") or ""
            out[canon] = {"value": val, "hash": _h(val)}
    return out

# ----- header sanitizers -----
def _sanitize_cookie_name(name: str) -> str:
    if name is None: return "Cookie:UNKNOWN"
    safe = name.replace("\r"," ").replace("\n"," ").replace("\t"," ").strip()
    return safe if safe.startswith("Cookie:") else f"Cookie:{safe}"
def _before_key(name): return _sanitize_cookie_name(name) + " (Before)"
def _after_key(name):  return _sanitize_cookie_name(name) + " (After)"

# ----- nav/redirect helpers -----
def _get_nav_marker(driver):
    try:
        return driver.execute_script("return (performance.timeOrigin||performance.timing?.navigationStart)||Date.now();")
    except Exception:
        return None

def _observe_redirect_refresh_and_tabs(driver, pre_url, pre_nav_ts, pre_handles, window_sec=6.0):
    t0 = time.time()
    seen = set(pre_handles)
    new_tabs, redirect_url, refreshed = [], "", False
    while (time.time()-t0) < window_sec:
        try: handles = set(driver.window_handles)
        except Exception: handles = set()
        for h in list(handles - seen):
            try:
                driver.switch_to.window(h)
                new_tabs.append({"title": driver.title or "", "url": driver.current_url or ""})
            except Exception:
                new_tabs.append({"title": "", "url": ""})
            finally:
                seen.add(h)
        try: driver.switch_to.window(list(pre_handles)[0])
        except Exception: pass

        try: curr_url = driver.current_url or ""
        except Exception: curr_url = ""
        nav_ts = _get_nav_marker(driver)

        if curr_url and pre_url and curr_url != pre_url and not redirect_url:
            redirect_url = curr_url
        if nav_ts is not None and pre_nav_ts is not None and nav_ts != pre_nav_ts:
            if (not redirect_url) and (curr_url == pre_url):
                refreshed = True
        time.sleep(0.2)

    if not redirect_url and new_tabs:
        redirect_url = new_tabs[0].get("url","") or ""
    try: driver.switch_to.window(list(pre_handles)[0])
    except Exception: pass
    return redirect_url, refreshed, new_tabs

# ===== binary detection =====
def _common_paths_for(browser: str):
    if browser == "chrome":
        return [
            Path(r"C:\Program Files\Google\Chrome\Application\chrome.exe"),
            Path(r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"),
            Path(os.path.expandvars(r"%LOCALAPPDATA%\Google\Chrome\Application\chrome.exe")),
        ]
    if browser == "brave":
        return [
            Path(os.path.expandvars(r"%LOCALAPPDATA%\BraveSoftware\Brave-Browser\Application\brave.exe")),
            Path(r"C:\Program Files\BraveSoftware\Brave-Browser\Application\brave.exe"),
        ]
    if browser == "opera":
        return [
            Path(os.path.expandvars(r"%LOCALAPPDATA%\Programs\Opera\opera.exe")),
            Path(r"C:\Program Files\Opera\opera.exe"),
        ]
    return []

def _auto_find_browser_binary(browser: str) -> str | None:
    for p in _common_paths_for(browser):
        try:
            if p.exists():
                return str(p)
        except Exception:
            pass
    return None

# ===== options/services =====
def _apply_common_args(opts, profile_dir: Path|None, privacy_flags, privacy_prefs):
    if profile_dir:
        opts.add_argument(f"--user-data-dir={str(profile_dir)}")
    opts.add_argument("--no-first-run")
    opts.add_argument("--no-default-browser-check")
    opts.add_argument("--disable-backgrounding-occluded-windows")
    opts.add_argument("--disable-notifications")
    opts.add_argument("--remote-allow-origins=*")
    for f in (privacy_flags or []):
        fl = str(f).strip().lower()
        if fl in ("--incognito","--inprivate"):
            print("[warn] skipping incognito/private flag so extension UI can show")
            continue
        opts.add_argument(f)
    if privacy_prefs:
        try: opts.add_experimental_option("prefs", privacy_prefs)
        except Exception: pass
    try:
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option("useAutomationExtension", False)
    except Exception:
        pass

def _normalize_unpacked_dir(ext_path: str | None) -> str | None:
    if not ext_path: return None
    p = Path(ext_path)
    if not p.exists():
        print(f"[warn] Extension path does not exist: {p}")
        return None
    if p.is_file():
        print(f"[warn] '{p}' is a file. Unpack the CRX; point chromium_path to the folder with manifest.json.")
        return None
    if not (p / "manifest.json").exists():
        print(f"[warn] No manifest.json in '{p}'. Not a valid unpacked extension.")
        return None
    return p.resolve().as_posix()

def _read_manifest_version(ext_dir: str | None) -> int | None:
    if not ext_dir: return None
    try:
        data = json.loads(Path(ext_dir, "manifest.json").read_text(encoding="utf-8"))
        return int(data.get("manifest_version", 0))
    except Exception:
        return None

def _chrome_service_with_wdm(log_dir: Path) -> ChromeService:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"chromedriver_{int(time.time())}.log"
    if not _WDM_OK:
        print(f"[driver-log] {log_path.as_posix()} (wdm not installed; Selenium Manager may run)")
        return ChromeService(log_output=str(log_path))
    # NOTE: no `path=` here (works across webdriver-manager versions)
    driver_path = ChromeDriverManager().install()
    print(f"[wdm] ChromeDriver: {driver_path}")
    print(f"[driver-log] {log_path.as_posix()}")
    return ChromeService(executable_path=driver_path, log_output=str(log_path))

def _edge_service_with_wdm(log_dir: Path) -> EdgeService:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"edgedriver_{int(time.time())}.log"
    if not (_WDM_OK and EdgeService is not None):
        print(f"[driver-log] {log_path.as_posix()} (wdm not installed; Selenium Manager may run)")
        return EdgeService(log_output=str(log_path))
    driver_path = EdgeChromiumDriverManager().install()
    print(f"[wdm] EdgeDriver: {driver_path}")
    print(f"[driver-log] {log_path.as_posix()}")
    return EdgeService(executable_path=driver_path, log_output=str(log_path))

# ===== driver makers =====
def _make_chrome_like(browser: str, browser_binary: str|None, ext_dir: str|None,
                      profile_dir: Path|None, privacy_flags, privacy_prefs, log_dir: Path):
    opts = ChromeOptions()
    if browser_binary:
        opts.binary_location = browser_binary
    else:
        auto = _auto_find_browser_binary(browser)
        if auto:
            print(f"[auto] {browser} binary: {auto}")
            opts.binary_location = auto
        else:
            print(f"[auto] {browser} binary not found; relying on default discovery")
    _apply_common_args(opts, profile_dir, privacy_flags, privacy_prefs)
    if ext_dir:
        mv = _read_manifest_version(ext_dir)
        print(f"[info] Loading unpacked extension from: {ext_dir} (manifest_version={mv})")
        opts.add_argument(f"--disable-extensions-except={ext_dir}")
        opts.add_argument(f"--load-extension={ext_dir}")
    else:
        print("[info] Launching without extension.")
    service = _chrome_service_with_wdm(log_dir)
    return webdriver.Chrome(options=opts, service=service)

def _make_edge(browser_binary: str|None, ext_dir: str|None,
               profile_dir: Path|None, privacy_flags, privacy_prefs, log_dir: Path):
    if EdgeOptions is None or EdgeService is None:
        raise RuntimeError("Edge WebDriver not available.")
    opts = EdgeOptions()
    if browser_binary:
        try: opts.binary_location = browser_binary
        except Exception: pass
    _apply_common_args(opts, profile_dir, privacy_flags, privacy_prefs)
    if ext_dir:
        mv = _read_manifest_version(ext_dir)
        print(f"[info] Loading unpacked extension (Edge) from: {ext_dir} (manifest_version={mv})")
        opts.add_argument(f"--disable-extensions-except={ext_dir}")
        opts.add_argument(f"--load-extension={ext_dir}")
    else:
        print("[info] Launching Edge without extension.")
    service = _edge_service_with_wdm(log_dir)
    return webdriver.Edge(options=opts, service=service)

def _make_driver(job_browser: str, browser_binary: str|None, ext_path: str|None,
                 profile_dir: Path|None, privacy_flags, privacy_prefs, log_dir: Path):
    ext_dir = _normalize_unpacked_dir(ext_path)
    b = (job_browser or "").lower()
    if b == "edge":
        return _make_edge(browser_binary, ext_dir, profile_dir, privacy_flags, privacy_prefs, log_dir)
    elif b in ("chrome","brave","opera"):
        return _make_chrome_like(b, browser_binary, ext_dir, profile_dir, privacy_flags, privacy_prefs, log_dir)
    else:
        return _make_chrome_like(b, browser_binary, ext_dir, profile_dir, privacy_flags, privacy_prefs, log_dir)

# ===== main flow =====
def run_one(job: dict, src_workbook: Path, out_workbook: Path):
    ext_ordinal = job.get("extension_ordinal", 0)
    prefix = f"{ext_ordinal}." if ext_ordinal else ""

    tmp_root = Path(tempfile.mkdtemp(prefix=f"{job.get('browser','chromium')}_profile_"))
    profile_dir = tmp_root / "user_data"
    logs_dir = tmp_root / "logs"
    profile_dir.mkdir(parents=True, exist_ok=True)

    try:
        driver = _make_driver(
            job.get("browser"), job.get("browser_binary"), job.get("extension_path"),
            profile_dir, job.get("privacy_flags") or [], job.get("privacy_prefs") or {}, logs_dir
        )
    except SessionNotCreatedException as e_a:
        print(f"[tierA-error] {e_a}")
        try:
            driver = _make_driver(
                job.get("browser"), job.get("browser_binary"), None,
                profile_dir, job.get("privacy_flags") or [], job.get("privacy_prefs") or {}, logs_dir
            )
        except SessionNotCreatedException as e_b:
            print(f"[tierB-error] {e_b}")
            driver = _make_driver(
                job.get("browser"), job.get("browser_binary"), None,
                None, job.get("privacy_flags") or [], job.get("privacy_prefs") or {}, logs_dir
            )

    try:
        url = job["affiliate_link"]
        for attempt in (1,2):
            try:
                driver.get(url); break
            except NoSuchWindowException:
                if attempt == 1:
                    try: driver.quit()
                    except Exception: pass
                    driver = _make_driver(
                        job.get("browser"), job.get("browser_binary"), job.get("extension_path"),
                        profile_dir, job.get("privacy_flags") or [], job.get("privacy_prefs") or {}, logs_dir
                    )
                    continue
                else:
                    raise

        print("\n=== MANUAL NAVIGATION ===")
        print("Browser opened. Please navigate to CHECKOUT (log in / guest as needed).")
        print("When you are at the CHECKOUT page, type 'y' + Enter to continue.")
        print("Or type 's' + Enter to skip the coupon step for this run.")

        before_coupon_cookies = None
        popup_seen = ""
        caps = driver.capabilities or {}
        browser_ver = caps.get("browserVersion") or caps.get("version") or ""
        domain = urlparse(driver.current_url or job.get("affiliate_link","")).netloc

        while True:
            try: ans = input("Are you at CHECKOUT now? [y]es / [s]kip / [n]o: ").strip().lower()
            except Exception: ans = ""
            if ans in ("y","yes"):
                before_coupon_cookies = [_cookie_frame_full(c) for c in driver.get_cookies()]
                domain = urlparse(driver.current_url or job.get("affiliate_link","")).netloc
                while True:
                    try: q = input("Do you see the extension popup right now? [y]es / [n]o: ").strip().lower()
                    except Exception: q = ""
                    if q in ("y","yes"): popup_seen="Yes"; break
                    if q in ("n","no"):  popup_seen="No";  break
                    print("Please type 'y' or 'n'.")
                break
            elif ans in ("s","skip"):
                try:
                    before_coupon_cookies = [_cookie_frame_full(c) for c in driver.get_cookies()]
                except Exception as e:
                    print(f"Warning: could not read cookies before skip ({e}). Proceeding empty.")
                    before_coupon_cookies = []
                try: domain = urlparse(driver.current_url or job.get("affiliate_link","")).netloc
                except Exception: domain = job.get("affiliate_link","")
                print("Skipping coupon step for this run as requested.")
                after_coupon_cookies = before_coupon_cookies
                new_tabs = []; redirect_url = ""; refreshed = False; popup_seen = "Skipped"
                goto_comparison_and_write(job, src_workbook, out_workbook, driver, browser_ver, domain,
                                          before_coupon_cookies, after_coupon_cookies, new_tabs, prefix,
                                          redirect_url, refreshed, popup_seen)
                return
            else:
                print("OK, I'll keep waiting. (Tip: you can press 's' to skip.)")
                time.sleep(5)

        print("\n=== ACTION ===")
        print("Click the popup (or toolbar icon) to apply/activate, then press ENTER here.")
        pre_handles = set(driver.window_handles)
        pre_url = driver.current_url or ""
        pre_nav_ts = _get_nav_marker(driver)
        try: input()
        except Exception: pass

        redirect_url, refreshed, new_tabs = _observe_redirect_refresh_and_tabs(
            driver, pre_url, pre_nav_ts, pre_handles, window_sec=float(job.get("redirect_window_sec",6.0))
        )
        after_coupon_cookies = [_cookie_frame_full(c) for c in driver.get_cookies()]

        goto_comparison_and_write(job, src_workbook, out_workbook, driver, browser_ver, domain,
                                  before_coupon_cookies, after_coupon_cookies, new_tabs, prefix,
                                  redirect_url, refreshed, popup_seen)

    finally:
        try: driver.quit()
        except Exception: pass
        try: shutil.rmtree(str(tmp_root), ignore_errors=True)
        except Exception: pass

# ===== output =====
def goto_comparison_and_write(job, src_workbook: Path, out_workbook: Path,
                              driver, browser_ver, domain,
                              before_cookies, after_cookies,
                              new_tabs, prefix,
                              redirect_url_final, refreshed,
                              popup_seen):
    new_tab_urls = "; ".join([t.get("url","") for t in new_tabs if t.get("url")])
    new_tab_titles = "; ".join([t.get("title","") for t in new_tabs if t.get("title")])

    before_targets = _snapshot_targets(before_cookies)
    after_targets  = _snapshot_targets(after_cookies)

    def val_before(name): return (prefix + (before_targets.get(name,{}).get("value","") or "")) if name in before_targets else ""
    def val_after(name):  return (prefix + (after_targets.get(name,{}).get("value","") or "")) if name in after_targets else ""

    wide = {
        "Plugin": job.get("extension_name",""),
        "Browser": job.get("browser","Chromium"),
        "Browser Privacy Level": job.get("privacy_name",""),
        "Browser Version": browser_ver,
        "Website": domain,
        "Affiliate Link": job.get("affiliate_link",""),
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
        if amap[k]["value_hash"] != bmap[k]["value_hash"]: changed_names.add(amap[k]["name"])

    for name in sorted(changed_names, key=lambda s: (s or "").lower()):
        if _is_target_name(name): continue
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
        "Redirect Window (s)": str(job.get("redirect_window_sec",6.0)),
    }

    append_cookie_comparison(out_workbook, wide)
    append_clean_data_row(src_workbook, out_workbook, clean_row)

    diag_rows = []
    for ck in sorted(before_targets.keys() | after_targets.keys(), key=lambda x: x.lower()):
        b = next((c for c in before_cookies if c["name"] == ck), None)
        a = next((c for c in after_cookies  if c["name"] == ck), None)
        b_hash = b and b.get("value_hash")
        a_hash = a and a.get("value_hash")
        change = "UNCHANGED"
        if b and not a: change="REMOVED"
        elif a and not b: change="ADDED"
        elif b and a and b_hash!=a_hash: change="CHANGED"
        if change!="UNCHANGED":
            diag_rows.append({
                "Test ID": clean_row["Test ID"], "Browser": clean_row["Browser"],
                "Browser Version": clean_row["Browser Version"],
                "Extension": clean_row["Extension"], "Extension Version": clean_row["Extension Version"],
                "Merchant": domain, "Affiliate Link": job.get("affiliate_link",""),
                "Cookie Name": ck, "Change": change,
                "Before Hash": b_hash or "", "After Hash": a_hash or "",
                "Observed At": ts
            })
    for tab in new_tabs:
        diag_rows.append({
            "Test ID": clean_row["Test ID"], "Browser": clean_row["Browser"],
            "Browser Version": clean_row["Browser Version"],
            "Extension": clean_row["Extension"], "Extension Version": clean_row["Extension Version"],
            "Merchant": domain, "Affiliate Link": job.get("affiliate_link",""),
            "Cookie Name": "(new_tab)", "Change": tab.get("title",""),
            "Before Hash": "", "After Hash": tab.get("url",""),
            "Observed At": ts
        })
    append_diagnostics(out_workbook, diag_rows)

    print("✔ Wrote: Clean_Data + Diagnostics + Cookie Field Comparison (manual Chromium).")
