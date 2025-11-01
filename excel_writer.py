# excel_writer.py (adds Cookie Field Comparison wide-sheet writer)
from pathlib import Path
import pandas as pd
import time, os, shutil
from tempfile import NamedTemporaryFile
from openpyxl import load_workbook

MAX_RETRIES = 8
SLEEP_SECONDS = 0.75

def _atomic_write(df_map: dict[str, pd.DataFrame], out_xlsx: Path):
    tmp = NamedTemporaryFile(delete=False, suffix=".xlsx", dir=str(out_xlsx.parent))
    tmp_path = Path(tmp.name); tmp.close()
    try:
        with pd.ExcelWriter(tmp_path, mode="w", engine="openpyxl") as xw:
            for sheet, df in df_map.items():
                df.to_excel(xw, sheet_name=sheet, index=False)
        if out_xlsx.exists():
            os.replace(tmp_path, out_xlsx)
        else:
            shutil.move(tmp_path, out_xlsx)
    finally:
        try:
            if tmp_path.exists(): tmp_path.unlink()
        except Exception:
            pass

def _retry_write(write_fn):
    delay = SLEEP_SECONDS
    for i in range(1, MAX_RETRIES + 1):
        try:
            return write_fn()
        except PermissionError:
            if i == MAX_RETRIES: raise
            time.sleep(delay); delay *= 1.5

def append_clean_data_row(src_xlsx: Path, out_xlsx: Path, row: dict):
    import pandas as pd
    cols = list(pd.read_excel(src_xlsx, sheet_name="Clean_Data", nrows=0).columns)
    ordered = {c: ("" if row.get(c) is None else row.get(c)) for c in cols}
    new_row_df = pd.DataFrame([ordered], columns=cols).astype("object")

    def write():
        sheets = {}
        if out_xlsx.exists():
            try:
                existing = pd.read_excel(out_xlsx, sheet_name="Clean_Data", dtype="object")
            except Exception:
                existing = pd.DataFrame(columns=cols)
            df = pd.concat([existing, new_row_df], ignore_index=True)
        else:
            df = new_row_df
        sheets["Clean_Data"] = df
        # keep Diagnostics if present
        if out_xlsx.exists():
            try:
                diag = pd.read_excel(out_xlsx, sheet_name="Diagnostics", dtype="object")
                sheets["Diagnostics"] = diag
            except Exception:
                pass
        # keep Cookie Field Comparison if present
        if out_xlsx.exists():
            try:
                cmpdf = pd.read_excel(out_xlsx, sheet_name="Cookie Field Comparison", dtype="object")
                sheets["Cookie Field Comparison"] = cmpdf
            except Exception:
                pass
        _atomic_write(sheets, out_xlsx)

    _retry_write(write)

def append_diagnostics(out_xlsx: Path, rows: list):
    import pandas as pd
    df_new = pd.DataFrame(rows)
    def write():
        sheets = {}
        # keep Clean_Data if present
        if out_xlsx.exists():
            try:
                clean = pd.read_excel(out_xlsx, sheet_name="Clean_Data", dtype="object")
                sheets["Clean_Data"] = clean
            except Exception:
                pass
            try:
                existing = pd.read_excel(out_xlsx, sheet_name="Diagnostics", dtype="object")
                df = pd.concat([existing, df_new], ignore_index=True)
            except Exception:
                df = df_new
        else:
            df = df_new
        sheets["Diagnostics"] = df
        # keep Cookie Field Comparison if present
        if out_xlsx.exists():
            try:
                cmpdf = pd.read_excel(out_xlsx, sheet_name="Cookie Field Comparison", dtype="object")
                sheets["Cookie Field Comparison"] = cmpdf
            except Exception:
                pass
        _atomic_write(sheets, out_xlsx)
    _retry_write(write)

def append_cookie_comparison(out_xlsx: Path, comparison_row: dict):
    """
    comparison_row must contain keys matching the wide layout. We will
    append to 'Cookie Field Comparison' sheet (create if absent).
    """
    import pandas as pd
    df_new = pd.DataFrame([comparison_row])

    def write():
        sheets = {}
        # keep Clean_Data if present
        if out_xlsx.exists():
            for keep in ("Clean_Data", "Diagnostics"):
                try:
                    dfk = pd.read_excel(out_xlsx, sheet_name=keep, dtype="object")
                    sheets[keep] = dfk
                except Exception:
                    pass
            try:
                existing = pd.read_excel(out_xlsx, sheet_name="Cookie Field Comparison", dtype="object")
                # align columns: union, then reindex
                all_cols = list(dict.fromkeys(list(existing.columns) + list(df_new.columns)))
                existing = existing.reindex(columns=all_cols)
                to_add = df_new.reindex(columns=all_cols)
                df = pd.concat([existing, to_add], ignore_index=True)
            except Exception:
                df = df_new
        else:
            df = df_new
        sheets["Cookie Field Comparison"] = df
        _atomic_write(sheets, out_xlsx)
    _retry_write(write)
