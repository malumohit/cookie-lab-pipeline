# runner_chromium_puppeteer.py — Chromium via Puppeteer (Node)
# - Launches Chrome/Edge/Brave/Opera headful with an UNPACKED extension dir
# - Honors privacy flags from matrix.yaml
# - Prompts exactly like your manual flow
# - Writes Cookie Field Comparison, Clean_Data, Diagnostics using excel_writer

import json, subprocess, tempfile, shutil, hashlib, time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

from excel_writer import append_clean_data_row, append_diagnostics, append_cookie_comparison

# ======= Target cookie list (same superset you’ve been using) =======
TARGET_ORDER = [
    "NV_MC_LC","NV_MC_FC","NV_ECM_TK_LC",
    "__attentive_utm_param_campaign","__attentive_utm_param_source",
    "__attentive_utm_param_medium","__attentive_utm_param_term","__attentive_utm_param_content",
    "campaign","campaign_id","campaign_date","campaign_source","campaign_medium","campaign_name",
    "utm_source","utm_medium","utm_campaign","utm_term","utm_content",
    "affid","aff_id","affiliate","affiliate_id","affiliate_source",
    "affsource","aff_source","affname","aff_sub","aff_sub2","aff_sub3","aff_sub4","aff_sub5",
    "subid","sub_id","awinaffid","awcid","awcr","aw_referrer","aw_click_id",
    "cjevent","cjData","irclickid","irgwc","irpid","iradid","iradname",
    "sscid","scid","prms","prm_expid","prm_click",
    "gclid","gclsrc","dclid","fbclid","msclkid","ttclid","twclid","yclid",
    "_ga","_ga_*","_gid","_gat","_gat_*","_gcl_au","_gcl_aw","_gcl_dc",
    "_fbp","_fbc","_uetsid","_uetvid","_tt_enable_cookie","_ttp",
    "_pin_unauth","_rdt_uuid","AMCV_","s_cc","s_sq","mbox","mboxEdgeCluster",
    "ref","referrer","source","campaignCode","promo","promocode","coupon","coupon_code",
    "session_id","sessionid","sid",
]
_CANON = {n.lower(): n for n in TARGET_ORDER if not n.endswith("*")}
_PREFIXES = [n[:-1].lower() for n in TARGET_ORDER if n.endswith("*")]

def _is_target_name(raw: str):
    if not raw: return None
    ln = raw.lower()
    if ln in _CANON: return _CANON[ln]
    for p in _PREFIXES:
        if ln.startswith(p):
            return raw
    return None

def _h(v: str) -> str:
    return hashlib.sha256((v or "").encode("utf-8")).hexdigest()[:16]

def _cookie_frame_full(c: dict) -> dict:
    # Normalize Puppeteer cookie -> our schema
    return {
        "name": c.get("name"),
        "value": c.get("value") or "",
        "value_hash": _h(c.get("value")),
        "domain": c.get("domain"),
        "path": c.get("path"),
        "expiry": c.get("expires") if c.get("expires", 0) not in (None, -1, 0) else None,
        "httpOnly": c.get("httpOnly"),
        "secure": c.get("secure"),
        "sameSite": c.get("sameSite"),
    }

def _sanitize_cookie_name(name: str) -> str:
    if name is None: return "Cookie:UNKNOWN"
    safe = name.replace("\r"," ").replace("\n"," ").replace("\t"," ").strip()
    return f"Cookie:{safe}" if not safe.startswith("Cookie:") else safe

def _before_key(name: str) -> str: return _sanitize_cookie_name(name) + " (Before)"
def _after_key(name: str) -> str:  return _sanitize_cookie_name(name) + " (After)"

def _snapshot_targets(cookies):
    out = {}
    for c in cookies:
        nm = c.get("name") or ""
        canon = _is_target_name(nm)
        if canon:
            v = c.get("value") or ""
            out[canon] = {"value": v, "hash": _h(v)}
    return out

def _union_changed_names(before_list, after_list):
    def key(c): return (c["name"], c.get("domain"), c.get("path"))
    bmap = {key(c): c for c in before_list}
    amap = {key(c): c for c in after_list}
    changed = set()
    for k in amap.keys() - bmap.keys(): changed.add(amap[k]["name"])
    for k in bmap.keys() - amap.keys(): changed.add(bmap[k]["name"])
    for k in amap.keys() & bmap.keys():
        if (amap[k].get("value_hash") != bmap[k].get("value_hash")):
            changed.add(amap[k]["name"])
    return changed, bmap, amap

def run_one(job: dict, src_workbook: Path, out_workbook: Path):
    # Prepare temp workspace for logs (also where Node creates the profile)
    tmp_root = Path(tempfile.mkdtemp(prefix=f"{job.get('browser','chrome')}_pupp_"))
    try:
        # Build Node command
        node_file = Path(__file__).with_name("puppeteer_chromium_runner.mjs")
        if not node_file.exists():
            raise RuntimeError(f"Missing {node_file.name} next to {__file__}. Place the .mjs file there.")

        # Ensure extension path is an UNPACKED folder
        ext_dir = job.get("extension_path") or ""
        if ext_dir and Path(ext_dir).is_file():
            raise RuntimeError("For Puppeteer, point 'chromium_path' in matrix.yaml to an UNPACKED extension folder (with manifest.json).")

        flags = (job.get("privacy_flags") or [])
        cmd = [
            "node",
            str(node_file),
            f'--url={job.get("affiliate_link","")}',
            f'--ext={ext_dir}',
            f'--redirectWindow={job.get("redirect_window_sec", 6.0)}',
            f'--browserName={job.get("browser","chrome")}',
            f'--jobId={job.get("job_id","")}',
            f'--privacyName={job.get("privacy_name","")}',
        ]
        if job.get("browser_binary"):
            cmd.append(f'--binary={job["browser_binary"]}')
        if flags:
            cmd.append(f'--privacyFlags={",".join(flags)}')

        # Stream user prompts through the same terminal
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if not proc.stdout:
            raise RuntimeError(f"Puppeteer runner produced no output.\nSTDERR:\n{proc.stderr}")

        data = json.loads(proc.stdout)

        if data.get("error"):
            raise RuntimeError(f"Puppeteer error: {data['error']}")

        # Build normalized cookie arrays
        before = [_cookie_frame_full(c) for c in (data.get("beforeCookies") or [])]
        after  = [_cookie_frame_full(c) for c in (data.get("afterCookies")  or [])]
        new_tabs = data.get("newTabs") or []
        redirect_url = data.get("redirectUrl") or ""
        refreshed = bool(data.get("refreshed"))
        popup_seen = data.get("popupSeen") or ""
        browser_ver = data.get("browserVersion") or ""

        # Build wide cookie comparison (targets + dynamic changed cookies)
        before_targets = _snapshot_targets(before)
        after_targets  = _snapshot_targets(after)

        ext_ordinal = job.get("extension_ordinal", 0)
        prefix = f"{ext_ordinal}." if ext_ordinal else ""

        def val_before(name): 
            return (prefix + (before_targets.get(name,{}).get("value","") or "")) if name in before_targets else ""
        def val_after(name):
            return (prefix + (after_targets.get(name,{}).get("value","") or "")) if name in after_targets else ""

        domain = urlparse(job.get("affiliate_link","")).netloc

        wide = {
            "Plugin": job.get("extension_name",""),
            "Browser": job.get("browser","Chromium"),
            "Browser Privacy Level": job.get("privacy_name",""),
            "Browser Version": browser_ver,
            "Website": domain,
            "Affiliate Link": job.get("affiliate_link",""),
        }

        for ck in sorted(set(before_targets.keys()) | set(after_targets.keys()), key=lambda x: x.lower()):
            wide[f"{ck} (Before)"] = val_before(ck)
            wide[f"{ck} (After)"]  = val_after(ck)

        # add dynamic changed cookies (non-targets)
        changed_names, bmap, amap = _union_changed_names(before, after)
        for name in sorted(changed_names, key=lambda x: (x or "").lower()):
            if _is_target_name(name):  # skip targets; already above
                continue
            bvals = [c["value"] for c in before if c["name"] == name]
            avals = [c["value"] for c in after  if c["name"] == name]
            wide[_before_key(name)] = (prefix + bvals[0]) if bvals else ""
            wide[_after_key(name)]  = (prefix + avals[0]) if avals else ""

        # Diagnostics/counts
        added = [amap[k] for k in amap.keys() - bmap.keys()]
        changed = []
        for k in amap.keys() & bmap.keys():
            if amap[k].get("value_hash") != bmap[k].get("value_hash"):
                changed.append({"before": bmap[k], "after": amap[k]})

        new_tab_urls = "; ".join([t.get("url","") for t in new_tabs if t.get("url")])
        new_tab_titles = "; ".join([t.get("title","") for t in new_tabs if t.get("title")])

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
            "Redirect URL": redirect_url,
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

        # Diagnostics rows (only targets + new_tab markers)
        diag_rows = []
        for ck in sorted(set(before_targets.keys()) | set(after_targets.keys()), key=lambda x: x.lower()):
            b = next((c for c in before if c["name"] == ck), None)
            a = next((c for c in after  if c["name"] == ck), None)
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
                "Affiliate Link": job.get("affiliate_link",""),
                "Cookie Name": "(new_tab)",
                "Change": tab.get("title",""),
                "Before Hash": "",
                "After Hash": tab.get("url",""),
                "Observed At": ts
            })
        append_diagnostics(out_workbook, diag_rows)

        print("✔ Wrote: Clean_Data + Diagnostics + Cookie Field Comparison (Puppeteer Chromium).")

    finally:
        try:
            shutil.rmtree(tmp_root, ignore_errors=True)
        except Exception:
            pass
