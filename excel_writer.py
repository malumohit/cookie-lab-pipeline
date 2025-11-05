# excel_writer.py  — openpyxl-only, dynamic headers, no pandas
from pathlib import Path
from typing import List, Dict, Any
from openpyxl import Workbook, load_workbook
from openpyxl.worksheet.worksheet import Worksheet

def _ensure_parent(p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)

def _load_or_create_book(path: Path):
    if path.exists():
        try:
            return load_workbook(path)
        except Exception:
            # corrupted or unreadable → start fresh
            wb = Workbook()
            # remove default sheet to avoid surprises; we'll add as needed
            if "Sheet" in wb.sheetnames:
                ws = wb["Sheet"]
                wb.remove(ws)
            return wb
    else:
        wb = Workbook()
        if "Sheet" in wb.sheetnames:
            ws = wb["Sheet"]
            wb.remove(ws)
        return wb

def _get_or_create_sheet(wb, name: str) -> Worksheet:
    if name in wb.sheetnames:
        return wb[name]
    ws = wb.create_sheet(title=name)
    # initialize empty header row
    ws.append([])  # row 1 reserved for headers
    return ws

def _read_header(ws: Worksheet) -> List[str]:
    if ws.max_row >= 1:
        return [(c.value if c.value is not None else "") for c in ws[1]]
    return []

def _write_header(ws: Worksheet, headers: List[str]):
    # rewrite entire header row (row 1)
    if ws.max_row < 1:
        ws.append([])
    # clear existing header row cells (optional)
    for i in range(1, len(headers) + 1):
        ws.cell(row=1, column=i, value=headers[i-1])

def _ensure_headers(ws: Worksheet, keys: List[str]) -> List[str]:
    """Merge existing headers with new keys (append-only). Return final header list."""
    header = _read_header(ws)
    existing = set(h for h in header if h)
    # initialize header if none
    if not header:
        header = []

    added = []
    for k in keys:
        if k not in existing:
            header.append(k)
            existing.add(k)
            added.append(k)

    if added:
        _write_header(ws, header)

    return header

def _append_row(ws: Worksheet, header: List[str], row: Dict[str, Any]):
    # build list in header order
    values = [row.get(h, "") for h in header]
    ws.append(values)

def _append_rows(ws: Worksheet, header: List[str], rows: List[Dict[str, Any]]):
    for r in rows:
        values = [r.get(h, "") for h in header]
        ws.append(values)

def _collect_all_keys(rows: List[Dict[str, Any]]) -> List[str]:
    out: List[str] = []
    seen = set()
    for r in rows:
        for k in r.keys():
            if k not in seen:
                seen.add(k)
                out.append(k)
    return out

# Public functions used by the runners

def append_cookie_comparison(out_workbook: Path, row: Dict[str, Any]):
    """Append one row to 'Cookie Field Comparison', expanding headers as needed."""
    _ensure_parent(out_workbook)
    wb = _load_or_create_book(out_workbook)
    ws = _get_or_create_sheet(wb, "Cookie Field Comparison")
    header = _ensure_headers(ws, list(row.keys()))
    _append_row(ws, header, row)
    wb.save(out_workbook)

def append_clean_data_row(src_workbook: Path, out_workbook: Path, row: Dict[str, Any]):
    """Append one row to 'Clean_Data', expanding headers as needed."""
    _ensure_parent(out_workbook)
    wb = _load_or_create_book(out_workbook)
    ws = _get_or_create_sheet(wb, "Clean_Data")
    header = _ensure_headers(ws, list(row.keys()))
    _append_row(ws, header, row)
    wb.save(out_workbook)

def append_diagnostics(out_workbook: Path, rows: List[Dict[str, Any]]):
    """Append multiple rows to 'Diagnostics', expanding headers as needed."""
    if not rows:
        return
    _ensure_parent(out_workbook)
    wb = _load_or_create_book(out_workbook)
    ws = _get_or_create_sheet(wb, "Diagnostics")
    keys_union = _collect_all_keys(rows)
    header = _ensure_headers(ws, keys_union)
    _append_rows(ws, header, rows)
    wb.save(out_workbook)
