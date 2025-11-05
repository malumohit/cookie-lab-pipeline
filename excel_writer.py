# excel_writer.py
from pathlib import Path
import pandas as pd

def _ensure_parent(p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)

def _read_sheet(path: Path, sheet: str) -> pd.DataFrame:
    if path.exists():
        try:
            return pd.read_excel(path, sheet_name=sheet, engine="openpyxl")
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()

def _write_sheet(path: Path, sheet: str, df: pd.DataFrame):
    mode = "a" if path.exists() else "w"
    with pd.ExcelWriter(path, mode=mode, engine="openpyxl") as xw:
        df.to_excel(xw, index=False, sheet_name=sheet)

def _append_row_dynamic(out_xlsx: Path, sheet_name: str, row: dict):
    _ensure_parent(out_xlsx)
    existing = _read_sheet(out_xlsx, sheet_name)
    existing_cols = list(existing.columns) if not existing.empty else []
    new_cols = [c for c in row.keys() if c not in existing_cols]
    cols = existing_cols + new_cols

    new_df = pd.DataFrame([{c: row.get(c, "") for c in cols}], columns=cols)
    if existing.empty:
        out = new_df
    else:
        out = pd.concat([existing.reindex(columns=cols, fill_value=""), new_df], ignore_index=True)
    _write_sheet(out_xlsx, sheet_name, out)

def _append_rows_dynamic(out_xlsx: Path, sheet_name: str, rows: list[dict]):
    if not rows:
        return
    _ensure_parent(out_xlsx)
    existing = _read_sheet(out_xlsx, sheet_name)
    existing_cols = list(existing.columns) if not existing.empty else []

    all_new_cols = []
    seen = set(existing_cols)
    for r in rows:
        for k in r.keys():
            if k not in seen:
                seen.add(k); all_new_cols.append(k)
    cols = existing_cols + all_new_cols

    norm = [{c: r.get(c, "") for c in cols} for r in rows]
    new_df = pd.DataFrame(norm, columns=cols)
    if existing.empty:
        out = new_df
    else:
        out = pd.concat([existing.reindex(columns=cols, fill_value=""), new_df], ignore_index=True)
    _write_sheet(out_xlsx, sheet_name, out)

# Public API used by runners
def append_cookie_comparison(out_workbook: Path, row: dict):
    _append_row_dynamic(out_workbook, "Cookie Field Comparison", row)

def append_clean_data_row(src_workbook: Path, out_workbook: Path, row: dict):
    _append_row_dynamic(out_workbook, "Clean_Data", row)

def append_diagnostics(out_workbook: Path, rows: list[dict]):
    _append_rows_dynamic(out_workbook, "Diagnostics", rows)
