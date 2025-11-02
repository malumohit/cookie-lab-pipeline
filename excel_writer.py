# excel_writer.py — openpyxl-only writer (no pandas)
from pathlib import Path
from datetime import datetime
from typing import Dict, List

from openpyxl import Workbook, load_workbook
from openpyxl.utils import get_column_letter

# --- Helpers ---------------------------------------------------------------

def _safe_load(path: Path):
    if path.exists():
        return load_workbook(filename=str(path))
    wb = Workbook()
    # openpyxl creates a default sheet — remove it so we control all sheets
    wb.remove(wb.active)
    return wb

def _ensure_sheet_with_headers(wb, sheet_name: str, headers: List[str]):
    if sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        # If sheet exists but empty, write headers
        if ws.max_row == 0 or (ws.max_row == 1 and ws.max_column == 1 and (ws["A1"].value is None)):
            ws.append(headers)
        return ws
    ws = wb.create_sheet(title=sheet_name)
    ws.append(headers)
    return ws

def _read_headers_from_master(master_xlsx: Path, desired_sheet: str) -> List[str]:
    """
    Read headers (first row) from 'desired_sheet' in the master workbook.
    If not found, fallback to the first sheet's first row.
    """
    m = load_workbook(filename=str(master_xlsx), read_only=True, data_only=True)
    try:
        ws = m[desired_sheet] if desired_sheet in m.sheetnames else m[m.sheetnames[0]]
        first_row = next(ws.iter_rows(min_row=1, max_row=1, values_only=True), None)
        if first_row and any(v is not None for v in first_row):
            return [str(v) if v is not None else "" for v in first_row]
    finally:
        m.close()
    # Last resort: return an empty list (caller must handle)
    return []

def _append_dict_row(ws, headers: List[str], row_dict: Dict[str, str]):
    """
    Append a row to ws following headers order. Unknown keys are appended as new columns at the end (once).
    """
    # Add any new keys not in headers
    new_keys = [k for k in row_dict.keys() if k not in headers]
    if new_keys:
        headers.extend(new_keys)
        # grow header row in the sheet
        if ws.max_row >= 1:
            for idx, h in enumerate(headers, start=1):
                ws.cell(row=1, column=idx, value=h)

    # Build row in header order
    row_vals = [row_dict.get(h, "") for h in headers]
    ws.append(row_vals)

# --- Public API ------------------------------------------------------------

def append_clean_data_row(master_xlsx: Path, out_xlsx: Path, row: Dict[str, str]):
    """
    Appends one row into sheet 'Clean_Data' of out_xlsx, using headers
    copied from master_xlsx's 'Clean_Data' (first row).
    """
    headers = _read_headers_from_master(master_xlsx, "Clean_Data")
    # If the master is missing headers for Clean_Data, fall back to the keys of row
    if not headers:
        headers = list(row.keys())

    wb = _safe_load(out_xlsx)
    ws = _ensure_sheet_with_headers(wb, "Clean_Data", headers)
    _append_dict_row(ws, headers, row)
    wb.save(str(out_xlsx))
    wb.close()

def append_diagnostics(out_xlsx: Path, rows: List[Dict[str, str]]):
    """
    Appends multiple rows into sheet 'Diagnostics'.
    On first write, headers are taken from the keys of the first row.
    """
    if not rows:
        return
    headers = list(rows[0].keys())
    wb = _safe_load(out_xlsx)
    ws = _ensure_sheet_with_headers(wb, "Diagnostics", headers)
    for r in rows:
        _append_dict_row(ws, headers, r)
    wb.save(str(out_xlsx))
    wb.close()

def append_cookie_comparison(out_xlsx: Path, wide_row: Dict[str, str]):
    """
    Appends one wide row into sheet 'Cookie Field Comparison'.
    On first write, headers are the keys of wide_row (in current order).
    """
    headers = list(wide_row.keys())
    wb = _safe_load(out_xlsx)
    ws = _ensure_sheet_with_headers(wb, "Cookie Field Comparison", headers)
    _append_dict_row(ws, headers, wide_row)
    wb.save(str(out_xlsx))
    wb.close()
