# excel_writer.py — dynamic-column Excel writer using openpyxl only
# Works even when new fields appear later (adds headers on the fly).

from pathlib import Path
from typing import Dict, List, Iterable
from openpyxl import Workbook, load_workbook
from openpyxl.worksheet.worksheet import Worksheet

# --------- config: sheet names ----------
SHEET_COOKIE_COMPARISON = "Cookie Field Comparison"
SHEET_CLEAN_DATA = "Clean_Data"
SHEET_DIAGNOSTICS = "Diagnostics"

# --------- helpers ----------

def _open_or_create(path: Path):
    path = Path(path)
    if path.exists():
        wb = load_workbook(path)
    else:
        wb = Workbook()
        # openpyxl starts with a default sheet; we’ll reuse/rename on first use
    return wb

def _ensure_sheet(wb, sheet_name: str) -> Worksheet:
    if sheet_name in wb.sheetnames:
        return wb[sheet_name]
    # If workbook still has the default "Sheet" and it's empty, rename it
    if len(wb.sheetnames) == 1 and wb.active.max_row == 1 and wb.active.max_column == 1 and wb.active["A1"].value is None:
        ws = wb.active
        ws.title = sheet_name
        return ws
    return wb.create_sheet(title=sheet_name)

def _header_map(ws: Worksheet) -> Dict[str, int]:
    """
    Return a mapping of header -> column index (1-based).
    If no header row yet, returns {}.
    """
    if ws.max_row < 1:
        return {}
    headers = {}
    row = ws[1]
    for idx, cell in enumerate(row, start=1):
        val = cell.value
        if isinstance(val, str) and val.strip() != "":
            headers[val] = idx
    return headers

def _ensure_headers(ws: Worksheet, needed_headers: Iterable[str]) -> Dict[str, int]:
    """
    Make sure every header in needed_headers exists. Any missing ones are appended to row 1.
    Return the up-to-date header->col map.
    """
    hdrs = _header_map(ws)
    if ws.max_row < 1 or not hdrs:
        # initialize with needed headers, in order
        for col_idx, key in enumerate(needed_headers, start=1):
            ws.cell(row=1, column=col_idx, value=key)
        return _header_map(ws)

    # append missing
    next_col = ws.max_column + 1
    added = False
    for key in needed_headers:
        if key not in hdrs:
            ws.cell(row=1, column=next_col, value=key)
            hdrs[key] = next_col
            next_col += 1
            added = True
    # if we appended, rebuild map to be safe
    return _header_map(ws) if added else hdrs

def _append_row(ws: Worksheet, row_dict: Dict[str, object]):
    """
    Append a row using keys from row_dict; headers added dynamically as needed.
    """
    # We keep current headers in the sheet and only add what's missing from this row
    headers_needed = list(row_dict.keys())
    hdr_map = _ensure_headers(ws, headers_needed)

    # Next row index
    r = ws.max_row + 1 if ws.max_row >= 1 else 1
    # Fill by header order
    for key, val in row_dict.items():
        c = hdr_map[key]
        ws.cell(row=r, column=c, value=val)

# --------- public API ----------

def append_cookie_comparison(out_workbook: Path, wide_row: Dict[str, object]):
    """
    Write a single 'wide' comparison row to 'Cookie Field Comparison'.
    Adds any missing headers automatically.
    """
    wb = _open_or_create(out_workbook)
    ws = _ensure_sheet(wb, SHEET_COOKIE_COMPARISON)
    _append_row(ws, wide_row)
    wb.save(out_workbook)

def append_clean_data_row(_master_workbook: Path, out_workbook: Path, clean_row: Dict[str, object]):
    """
    Append a row to 'Clean_Data'. We do not force a master schema; instead we add headers as they appear.
    (The master workbook is accepted for backward compatibility but not required.)
    """
    wb = _open_or_create(out_workbook)
    ws = _ensure_sheet(wb, SHEET_CLEAN_DATA)
    _append_row(ws, clean_row)
    wb.save(out_workbook)

def append_diagnostics(out_workbook: Path, rows: List[Dict[str, object]]):
    """
    Append multiple rows to 'Diagnostics'. Dynamically adds headers based on union of keys across rows.
    """
    if not rows:
        return
    wb = _open_or_create(out_workbook)
    ws = _ensure_sheet(wb, SHEET_DIAGNOSTICS)

    # Ensure union headers exist first (better column stability)
    union_keys = []
    seen = set()
    for r in rows:
        for k in r.keys():
            if k not in seen:
                seen.add(k)
                union_keys.append(k)
    _ensure_headers(ws, union_keys)

    for r in rows:
        _append_row(ws, r)

    wb.save(out_workbook)
