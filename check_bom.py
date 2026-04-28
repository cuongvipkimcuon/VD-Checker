import hashlib
import logging
import os
import pickle
import sqlite3
import threading
import tkinter as tk
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from tkinter import filedialog, messagebox, simpledialog, ttk

import pandas as pd


DB_FILE = "check_bom.db"
LOG_FILE = "check_bom.log"


logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


def normalize_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() == "nan":
        return ""
    return text


def normalize_key(value: object) -> str:
    return normalize_text(value).lower()


def normalize_dg_case(value: object) -> str:
    text = normalize_text(value).upper().replace(" ", "")
    if not text:
        return ""
    if text.startswith("0-"):
        text = "O-" + text[2:]
    return text


def safe_float(value: object) -> float | None:
    if value is None:
        return None
    text = normalize_text(value).replace(",", ".")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def compute_file_md5(file_path: str) -> str:
    md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            md5.update(chunk)
    return md5.hexdigest()


def extract_customer_code_from_product_code(product_code: object) -> str:
    text = normalize_text(product_code)
    if not text:
        return ""
    parts = [p.strip() for p in text.split(".") if p.strip()]
    if len(parts) >= 2:
        return parts[1]
    return ""


@dataclass
class CompareResult:
    ma_npl: str
    ten_npl: str
    mo_ta: str
    dvt: str
    sldm1_ke: float | None
    so_luong_ke: float | None
    sldm1_bom: float | None
    so_luong_bom: float | None
    khac: str
    chi_tiet: str
    trang_thai: str
    dg_case: str = ""


class DatabaseManager:
    def __init__(self, db_file: str = DB_FILE):
        self.db_file = db_file
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_file)
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _init_db(self) -> None:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS setup (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                key TEXT UNIQUE NOT NULL,
                value TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS customers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_name TEXT NOT NULL,
                code TEXT,
                folder_link TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS mapping (
                dg_case TEXT PRIMARY KEY,
                file_path TEXT NOT NULL,
                sheet_name TEXT NOT NULL,
                cell TEXT NOT NULL,
                file_hash TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS hash_cache (
                file_path TEXT NOT NULL,
                sheet_name TEXT NOT NULL,
                hash_value TEXT NOT NULL,
                data BLOB NOT NULL,
                last_used TEXT NOT NULL,
                PRIMARY KEY (file_path, sheet_name)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS bom_ke (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dg_case TEXT,
                row_index INTEGER,
                order_date TEXT,
                ma_npl TEXT,
                ten_npl TEXT,
                mo_ta TEXT,
                don_vi_tinh TEXT,
                so_luong_dm_1 REAL,
                so_luong REAL,
                hash_bom_line TEXT
            )
            """
        )
        conn.commit()
        conn.close()

    def get_setup_value(self, key: str) -> str:
        conn = self._connect()
        cur = conn.cursor()
        row = cur.execute("SELECT value FROM setup WHERE key = ?", (key,)).fetchone()
        conn.close()
        return str(row[0]) if row and row[0] is not None else ""

    def set_setup_value(self, key: str, value: str) -> None:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO setup(key, value)
            VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, value),
        )
        conn.commit()
        conn.close()

    def get_customers(self) -> list[tuple]:
        conn = self._connect()
        cur = conn.cursor()
        rows = cur.execute(
            "SELECT id, customer_name, code, folder_link FROM customers ORDER BY id ASC"
        ).fetchall()
        conn.close()
        return rows

    def add_customer(self, customer_name: str, code: str, folder_link: str) -> None:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO customers(customer_name, code, folder_link) VALUES (?, ?, ?)",
            (customer_name, code, folder_link),
        )
        conn.commit()
        conn.close()

    def update_customer(self, row_id: int, customer_name: str, code: str, folder_link: str) -> None:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE customers
            SET customer_name = ?, code = ?, folder_link = ?
            WHERE id = ?
            """,
            (customer_name, code, folder_link, row_id),
        )
        conn.commit()
        conn.close()

    def delete_customer(self, row_id: int) -> None:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("DELETE FROM customers WHERE id = ?", (row_id,))
        conn.commit()
        conn.close()

    def get_mappings(self) -> list[tuple]:
        conn = self._connect()
        cur = conn.cursor()
        rows = cur.execute(
            """
            SELECT dg_case, file_path, sheet_name, cell, file_hash
            FROM mapping
            ORDER BY dg_case
            """
        ).fetchall()
        conn.close()
        return rows

    def get_mapping(self, dg_case: str) -> tuple | None:
        conn = self._connect()
        cur = conn.cursor()
        row = cur.execute(
            """
            SELECT dg_case, file_path, sheet_name, cell, file_hash
            FROM mapping
            WHERE dg_case = ?
            """,
            (dg_case,),
        ).fetchone()
        conn.close()
        return row

    def upsert_mapping(self, dg_case: str, file_path: str, sheet_name: str, cell: str, file_hash: str) -> None:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO mapping(dg_case, file_path, sheet_name, cell, file_hash)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(dg_case) DO UPDATE SET
                file_path = excluded.file_path,
                sheet_name = excluded.sheet_name,
                cell = excluded.cell,
                file_hash = excluded.file_hash
            """,
            (dg_case, file_path, sheet_name, cell, file_hash),
        )
        conn.commit()
        conn.close()

    def delete_mapping(self, dg_case: str) -> None:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("DELETE FROM mapping WHERE dg_case = ?", (dg_case,))
        conn.commit()
        conn.close()

    def get_cache(self, file_path: str, sheet_name: str) -> tuple | None:
        conn = self._connect()
        cur = conn.cursor()
        row = cur.execute(
            """
            SELECT hash_value, data
            FROM hash_cache
            WHERE file_path = ? AND sheet_name = ?
            """,
            (file_path, sheet_name),
        ).fetchone()
        conn.close()
        return row

    def upsert_cache(self, file_path: str, sheet_name: str, hash_value: str, data_blob: bytes) -> None:
        now = datetime.now().isoformat(timespec="seconds")
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO hash_cache(file_path, sheet_name, hash_value, data, last_used)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(file_path, sheet_name) DO UPDATE SET
                hash_value = excluded.hash_value,
                data = excluded.data,
                last_used = excluded.last_used
            """,
            (file_path, sheet_name, hash_value, data_blob, now),
        )
        conn.commit()
        conn.close()

    def touch_cache(self, file_path: str, sheet_name: str) -> None:
        now = datetime.now().isoformat(timespec="seconds")
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE hash_cache
            SET last_used = ?
            WHERE file_path = ? AND sheet_name = ?
            """,
            (now, file_path, sheet_name),
        )
        conn.commit()
        conn.close()

    def get_all_cache_rows(self) -> list[tuple]:
        conn = self._connect()
        cur = conn.cursor()
        rows = cur.execute(
            """
            SELECT file_path, sheet_name, hash_value, last_used
            FROM hash_cache
            ORDER BY last_used DESC
            """
        ).fetchall()
        conn.close()
        return rows

    def delete_cache_older_than(self, days: int) -> int:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat(timespec="seconds")
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("DELETE FROM hash_cache WHERE last_used < ?", (cutoff,))
        affected = cur.rowcount if cur.rowcount is not None else 0
        conn.commit()
        conn.close()
        return int(affected)

    def clear_cache(self) -> None:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("DELETE FROM hash_cache")
        conn.commit()
        conn.close()


class ExcelParser:
    def __init__(self, db: DatabaseManager):
        self.db = db

    def load_bom_ke(self, file_path: str) -> pd.DataFrame:
        if not Path(file_path).exists():
            raise FileNotFoundError(f"Khong tim thay bang ke: {file_path}")
        file_hash = compute_file_md5(file_path)
        # Bump cache key version when parse schema changes to avoid stale cached data.
        cache_key = "BOM_KE_V2"
        cached = self.db.get_cache(file_path, cache_key)
        if cached and cached[0] == file_hash:
            self.db.touch_cache(file_path, cache_key)
            logging.info("Load bang ke tu cache: %s", file_path)
            return pickle.loads(cached[1])

        df_raw = pd.read_excel(file_path, sheet_name=0, header=None)
        if df_raw.empty:
            raise ValueError("Bang ke rong.")
        if df_raw.shape[1] < 16:
            raise ValueError("Bang ke khong du cot (can toi thieu cot P).")

        out = pd.DataFrame(
            {
                "row_index": df_raw.index + 1,
                "dg_case": df_raw.iloc[:, 0].map(normalize_text),
                "order_date": pd.to_datetime(df_raw.iloc[:, 1], errors="coerce"),
                "product_code": df_raw.iloc[:, 3].map(normalize_text),
                "ma_npl": df_raw.iloc[:, 9].map(normalize_text),
                "ten_npl": df_raw.iloc[:, 10].map(normalize_text),
                "mo_ta": df_raw.iloc[:, 11].map(normalize_text),
                "don_vi_tinh": df_raw.iloc[:, 13].map(normalize_text),
                "so_luong_dm_1": pd.to_numeric(df_raw.iloc[:, 14], errors="coerce"),
                "so_luong": pd.to_numeric(df_raw.iloc[:, 15], errors="coerce"),
            }
        )
        out["customer_code"] = out["product_code"].map(extract_customer_code_from_product_code)
        out = out[(out["dg_case"] != "") & (out["product_code"] != "")].copy()
        blob = pickle.dumps(out)
        self.db.upsert_cache(file_path, cache_key, file_hash, blob)
        logging.info("Parse bang ke moi va luu cache: %s", file_path)
        return out

    def load_bom_sheet(self, file_path: str, sheet_name: str) -> pd.DataFrame:
        if not Path(file_path).exists():
            raise FileNotFoundError(f"Khong tim thay file BOM: {file_path}")
        file_hash = compute_file_md5(file_path)
        cache_key = sheet_name
        cached = self.db.get_cache(file_path, cache_key)
        if cached and cached[0] == file_hash:
            self.db.touch_cache(file_path, cache_key)
            logging.info("Load BOM sheet tu cache: %s | %s", file_path, sheet_name)
            return pickle.loads(cached[1])

        df_raw = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
        if df_raw.empty:
            raise ValueError(f"Sheet {sheet_name} rong.")

        if df_raw.shape[1] < 11:
            raise ValueError(f"Sheet {sheet_name} khong du cot (can toi thieu cot L).")

        df = df_raw.iloc[1:].copy()
        col_a = df.iloc[:, 0].map(normalize_text)
        df = df[col_a != ""].copy()
        out = pd.DataFrame(
            {
                "ma_npl": df.iloc[:, 9].map(normalize_text),
                "ten_npl": df.iloc[:, 10].map(normalize_text),
                "mo_ta": df.iloc[:, 11].map(normalize_text) if df.shape[1] > 11 else "",
                "sldm1_h": pd.to_numeric(df.iloc[:, 7], errors="coerce"),
                "so_luong_i": pd.to_numeric(df.iloc[:, 8], errors="coerce"),
                "so_luong_k": pd.to_numeric(df.iloc[:, 10], errors="coerce"),
            }
        )
        out = out[out["ma_npl"] != ""].copy()
        blob = pickle.dumps(out)
        self.db.upsert_cache(file_path, cache_key, file_hash, blob)
        logging.info("Parse BOM sheet moi va luu cache: %s | %s", file_path, sheet_name)
        return out


class BomSearcher:
    def __init__(self, db: DatabaseManager):
        self.db = db

    def search_in_file(self, file_path: str, dg_case: str) -> tuple[str, str] | None:
        xls = pd.ExcelFile(file_path)
        target = normalize_key(dg_case)
        for sheet in xls.sheet_names:
            df = pd.read_excel(file_path, sheet_name=sheet, header=None)
            max_row = min(9, len(df))
            max_col = min(2, df.shape[1])
            for r in range(max_row):
                for c in range(max_col):
                    val = normalize_key(df.iat[r, c])
                    if val == target:
                        cell = f"{chr(ord('A') + c)}{r + 1}"
                        return sheet, cell
        return None

    def resolve_mapping(self, dg_case: str, customer_folder: str) -> tuple[str, str, str]:
        mapped = self.db.get_mapping(dg_case)
        if mapped:
            _, file_path, sheet_name, cell, file_hash = mapped
            if Path(file_path).exists():
                current_hash = compute_file_md5(file_path)
                if current_hash == file_hash:
                    return file_path, sheet_name, cell

        folder = Path(customer_folder)
        if not folder.exists():
            raise FileNotFoundError(f"Khong ton tai thu muc khach hang: {customer_folder}")
        excel_files = [p for p in folder.glob("*") if p.suffix.lower() in [".xlsx", ".xls"]]
        excel_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        for fp in excel_files:
            try:
                found = self.search_in_file(str(fp), dg_case)
                if found:
                    sheet, cell = found
                    f_hash = compute_file_md5(str(fp))
                    self.db.upsert_mapping(dg_case, str(fp), sheet, cell, f_hash)
                    return str(fp), sheet, cell
            except Exception as exc:
                logging.warning("Khong scan duoc file %s: %s", fp, exc)
                continue
        raise ValueError(f"Khong tim thay DG Case {dg_case} trong thu muc {customer_folder}")


class BomComparator:
    @staticmethod
    def is_quantity_match(dvt: str, value_ke: float | None, value_bom: float | None) -> bool:
        if value_ke is None or value_bom is None:
            return False
        unit = normalize_key(dvt)
        if unit == "m":
            return abs(value_ke - value_bom) <= 0.05 * abs(value_ke)
        return abs(value_ke - value_bom) <= 1

    @staticmethod
    def is_sldm1_match(value_ke: float | None, value_bom: float | None) -> bool:
        if value_ke is None or value_bom is None:
            return False
        return abs(value_ke - value_bom) <= 1

    def compare_pm_only(self, selected_rows: pd.DataFrame) -> list[CompareResult]:
        if "time_key" not in selected_rows.columns:
            raise ValueError("Du lieu PM thieu thong tin moc thoi gian.")
        time_keys = selected_rows["time_key"].dropna().unique().tolist()
        if len(time_keys) < 2:
            raise ValueError("Check PM can toi thieu 2 moc thoi gian.")

        grouped = {}
        time_order = []
        for tk, gdf in selected_rows.groupby("time_key", sort=False):
            g_sorted = gdf.sort_values(by=["row_index"], ascending=True)
            grouped[str(tk)] = g_sorted
            sample = g_sorted.iloc[0]
            time_order.append(
                (
                    str(tk),
                    sample["order_date"] if pd.notna(sample["order_date"]) else pd.Timestamp.max,
                )
            )
        time_order.sort(key=lambda x: x[1])
        ordered_keys = [x[0] for x in time_order]

        def to_map(df: pd.DataFrame) -> dict[str, pd.Series]:
            out: dict[str, pd.Series] = {}
            for _, row in df.iterrows():
                out[normalize_key(row["ma_npl"])] = row
            return out

        results: list[CompareResult] = []
        for idx in range(1, len(ordered_keys)):
            prev_key = ordered_keys[idx - 1]
            curr_key = ordered_keys[idx]
            prev_df = grouped[prev_key]
            curr_df = grouped[curr_key]
            prev_map = to_map(prev_df)
            curr_map = to_map(curr_df)
            union_keys = sorted({*prev_map.keys(), *curr_map.keys()})
            prev_label = normalize_text(prev_df.iloc[0]["time_label"])
            curr_label = normalize_text(curr_df.iloc[0]["time_label"])
            for key in union_keys:
                p_row = prev_map.get(key)
                c_row = curr_map.get(key)
                if p_row is None:
                    results.append(
                        CompareResult(
                            ma_npl=normalize_text(c_row["ma_npl"]),
                            ten_npl=normalize_text(c_row["ten_npl"]),
                            mo_ta=normalize_text(c_row["mo_ta"]),
                            dvt=normalize_text(c_row["don_vi_tinh"]),
                            sldm1_ke=None,
                            so_luong_ke=None,
                            sldm1_bom=safe_float(c_row["so_luong_dm_1"]),
                            so_luong_bom=safe_float(c_row["so_luong"]),
                            khac=f"{prev_label} -> {curr_label}",
                            chi_tiet="Phat sinh moi theo thoi gian",
                            trang_thai="❌",
                            dg_case=normalize_text(c_row.get("dg_case", "")),
                        )
                    )
                    continue
                if c_row is None:
                    results.append(
                        CompareResult(
                            ma_npl=normalize_text(p_row["ma_npl"]),
                            ten_npl=normalize_text(p_row["ten_npl"]),
                            mo_ta=normalize_text(p_row["mo_ta"]),
                            dvt=normalize_text(p_row["don_vi_tinh"]),
                            sldm1_ke=safe_float(p_row["so_luong_dm_1"]),
                            so_luong_ke=safe_float(p_row["so_luong"]),
                            sldm1_bom=None,
                            so_luong_bom=None,
                            khac=f"{prev_label} -> {curr_label}",
                            chi_tiet="Bi mat trong moc thoi gian sau",
                            trang_thai="❌",
                            dg_case=normalize_text(p_row.get("dg_case", "")),
                        )
                    )
                    continue

                dvt = normalize_text(c_row["don_vi_tinh"] or p_row["don_vi_tinh"])
                prev_sldm = safe_float(p_row["so_luong_dm_1"])
                curr_sldm = safe_float(c_row["so_luong_dm_1"])
                prev_qty = safe_float(p_row["so_luong"])
                curr_qty = safe_float(c_row["so_luong"])
                sldm_ok = self.is_sldm1_match(curr_sldm, prev_sldm)
                qty_ok = self.is_quantity_match(dvt, curr_qty, prev_qty)
                ok = sldm_ok and qty_ok
                reasons = []
                if not sldm_ok:
                    reasons.append("SLDM1 thay doi")
                if not qty_ok:
                    reasons.append("So luong thay doi")
                detail = "On dinh qua 2 moc thoi gian" if ok else ", ".join(reasons)
                results.append(
                    CompareResult(
                        ma_npl=normalize_text(c_row["ma_npl"]),
                        ten_npl=normalize_text(c_row["ten_npl"]),
                        mo_ta=normalize_text(c_row["mo_ta"]),
                        dvt=dvt,
                        sldm1_ke=curr_sldm,
                        so_luong_ke=curr_qty,
                        sldm1_bom=prev_sldm,
                        so_luong_bom=prev_qty,
                        khac=f"{prev_label} -> {curr_label}",
                        chi_tiet=detail,
                        trang_thai="✔️" if ok else "❌",
                        dg_case=normalize_text(c_row.get("dg_case", "")),
                    )
                )
        return results

    def compare_pm_excel(self, ke_rows: pd.DataFrame, bom_rows: pd.DataFrame, dg_case: str) -> list[CompareResult]:
        bom_by_ma = {}
        for _, row in bom_rows.iterrows():
            bom_by_ma[normalize_key(row["ma_npl"])] = row

        results: list[CompareResult] = []
        for _, row in ke_rows.iterrows():
            ma_key = normalize_key(row["ma_npl"])
            bom_row = bom_by_ma.get(ma_key)
            if bom_row is None:
                results.append(
                    CompareResult(
                        ma_npl=normalize_text(row["ma_npl"]),
                        ten_npl=normalize_text(row["ten_npl"]),
                        mo_ta=normalize_text(row["mo_ta"]),
                        dvt=normalize_text(row["don_vi_tinh"]),
                        sldm1_ke=safe_float(row["so_luong_dm_1"]),
                        so_luong_ke=safe_float(row["so_luong"]),
                        sldm1_bom=None,
                        so_luong_bom=None,
                        khac=dg_case,
                        chi_tiet="Thieu dong trong file BOM",
                        trang_thai="❌",
                        dg_case=normalize_text(row.get("dg_case", dg_case)),
                    )
                )
                continue

            dvt = normalize_text(row["don_vi_tinh"])
            ke_sldm1 = safe_float(row["so_luong_dm_1"])
            ke_qty = safe_float(row["so_luong"])
            bom_h = safe_float(bom_row["sldm1_h"])
            bom_i = safe_float(bom_row["so_luong_i"])
            bom_k = safe_float(bom_row["so_luong_k"])
            qty_target = bom_i if self.is_quantity_match(dvt, ke_qty, bom_i) else bom_k
            sldm_ok = self.is_sldm1_match(ke_sldm1, bom_h)
            qty_ok = self.is_quantity_match(dvt, ke_qty, qty_target)
            ok = sldm_ok and qty_ok
            detail = "Khop PM & Excel"
            if not ok:
                reasons = []
                if not sldm_ok:
                    reasons.append("Sai SLDM1 (O vs H)")
                if not qty_ok:
                    reasons.append("Sai so luong (P vs I/K)")
                detail = ", ".join(reasons)
            results.append(
                CompareResult(
                    ma_npl=normalize_text(row["ma_npl"]),
                    ten_npl=normalize_text(row["ten_npl"]),
                    mo_ta=normalize_text(row["mo_ta"]),
                    dvt=dvt,
                    sldm1_ke=ke_sldm1,
                    so_luong_ke=ke_qty,
                    sldm1_bom=bom_h,
                    so_luong_bom=qty_target,
                    khac=dg_case,
                    chi_tiet=detail,
                    trang_thai="✔️" if ok else "❌",
                    dg_case=normalize_text(row.get("dg_case", dg_case)),
                )
            )

        ke_keys = {normalize_key(v) for v in ke_rows["ma_npl"].tolist()}
        for _, bom_row in bom_rows.iterrows():
            key = normalize_key(bom_row["ma_npl"])
            if key in ke_keys:
                continue
            results.append(
                CompareResult(
                    ma_npl=normalize_text(bom_row["ma_npl"]),
                    ten_npl=normalize_text(bom_row["ten_npl"]),
                    mo_ta=normalize_text(bom_row["mo_ta"]),
                    dvt="",
                    sldm1_ke=None,
                    so_luong_ke=None,
                    sldm1_bom=safe_float(bom_row["sldm1_h"]),
                    so_luong_bom=safe_float(bom_row["so_luong_i"]),
                    khac=dg_case,
                    chi_tiet="Thua dong trong file BOM",
                    trang_thai="❌",
                    dg_case=dg_case,
                )
            )
        return results


class CheckBomApp:
    def __init__(self, root: tk.Tk, back_to_launcher: callable | None = None):
        self.root = root
        self.back_to_launcher = back_to_launcher
        self.root.title("Check BOM")
        self.root.geometry("1400x820")

        self.db = DatabaseManager()
        self.parser = ExcelParser(self.db)
        self.searcher = BomSearcher(self.db)
        self.comparator = BomComparator()

        self.bom_link_var = tk.StringVar()
        self.dg_case_pm_var = tk.StringVar()
        self.dg_case_excel_var = tk.StringVar()
        self.selected_customer_var = tk.StringVar()
        self.status_pm_var = tk.StringVar(value="San sang.")
        self.status_excel_var = tk.StringVar(value="San sang.")

        self.loaded_bom_ke_df: pd.DataFrame | None = None
        self.last_pm_result_df: pd.DataFrame | None = None
        self.last_excel_result_df: pd.DataFrame | None = None
        self.pm_current_subset: pd.DataFrame | None = None
        self.excel_current_subset: pd.DataFrame | None = None
        self.pm_display_time_keys: list[str] = []
        self.excel_display_time_keys: list[str] = []
        self.excel_display_row_indexes: list[int] = []

        self._build_ui()
        self._load_setup_data()

    def _subset_by_dg_case(self, df: pd.DataFrame, dg_case: str) -> pd.DataFrame:
        key = normalize_dg_case(dg_case)
        if not key:
            return df.iloc[0:0].copy()
        direct = df[
            df["dg_case"].map(
                lambda x: key == normalize_dg_case(x) or key in normalize_dg_case(x)
            )
        ].copy()
        if direct.empty:
            return direct
        base_product_code = normalize_text(direct.iloc[0]["product_code"])
        subset = df[df["product_code"].map(normalize_key) == normalize_key(base_product_code)].copy()
        subset = subset.sort_values(
            by=["order_date", "row_index"],
            ascending=[True, True],
            na_position="last",
        ).reset_index(drop=True)
        subset["time_key"] = subset["order_date"].map(
            lambda d: d.strftime("%Y-%m-%d") if pd.notna(d) else "N/A"
        )
        subset["time_label"] = subset["order_date"].map(
            lambda d: d.strftime("%d/%m/%Y") if pd.notna(d) else "N/A"
        )
        return subset

    def _auto_pick_customer_from_subset(self, subset: pd.DataFrame) -> str:
        if subset.empty:
            return ""
        customers = self.db.get_customers()
        customer_by_code = {normalize_key(row[2]): row for row in customers if normalize_text(row[2])}
        for code in subset["customer_code"].tolist():
            mapped = customer_by_code.get(normalize_key(code))
            if mapped:
                combo_text = f"{mapped[0]} | {mapped[1]} | {mapped[2]}"
                self.selected_customer_var.set(combo_text)
                return normalize_text(mapped[2])
        return ""

    def _build_ui(self) -> None:
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill="both", expand=True)

        self.tab_setup = ttk.Frame(self.notebook, padding=10)
        self.tab_mapping = ttk.Frame(self.notebook, padding=10)
        self.tab_hash = ttk.Frame(self.notebook, padding=10)
        self.tab_check = ttk.Frame(self.notebook, padding=10)
        self.notebook.add(self.tab_setup, text="Setup")
        self.notebook.add(self.tab_mapping, text="Mapping")
        self.notebook.add(self.tab_hash, text="Hash")
        self.notebook.add(self.tab_check, text="Check")

        self._build_tab_setup()
        self._build_tab_mapping()
        self._build_tab_hash()
        self._build_tab_check()

    def _build_tab_setup(self) -> None:
        frame_link = ttk.LabelFrame(self.tab_setup, text="Bang ke")
        frame_link.pack(fill="x", pady=(0, 10))
        ttk.Label(frame_link, text="Duong dan bang ke:").grid(row=0, column=0, sticky="w", padx=8, pady=8)
        ttk.Entry(frame_link, textvariable=self.bom_link_var, width=110).grid(
            row=0, column=1, sticky="ew", padx=8, pady=8
        )
        ttk.Button(frame_link, text="Chon file", command=self._choose_bom_link).grid(row=0, column=2, padx=8, pady=8)
        ttk.Button(frame_link, text="Luu", command=self._save_bom_link).grid(row=0, column=3, padx=8, pady=8)
        frame_link.columnconfigure(1, weight=1)

        frame_customer = ttk.LabelFrame(self.tab_setup, text="Danh sach khach hang")
        frame_customer.pack(fill="both", expand=True)
        cols = ("id", "customer_name", "code", "folder_link")
        customer_wrap = ttk.Frame(frame_customer)
        customer_wrap.pack(fill="both", expand=True, padx=8, pady=8)
        self.customer_tree = ttk.Treeview(customer_wrap, columns=cols, show="headings")
        for c, w in [("id", 60), ("customer_name", 220), ("code", 120), ("folder_link", 740)]:
            self.customer_tree.heading(c, text=c)
            self.customer_tree.column(c, width=w, anchor="w")
        y_scroll = ttk.Scrollbar(customer_wrap, orient="vertical", command=self.customer_tree.yview)
        x_scroll = ttk.Scrollbar(customer_wrap, orient="horizontal", command=self.customer_tree.xview)
        self.customer_tree.configure(yscrollcommand=y_scroll.set, xscrollcommand=x_scroll.set)
        self.customer_tree.grid(row=0, column=0, sticky="nsew")
        y_scroll.grid(row=0, column=1, sticky="ns")
        x_scroll.grid(row=1, column=0, sticky="ew")
        customer_wrap.rowconfigure(0, weight=1)
        customer_wrap.columnconfigure(0, weight=1)

        btn_wrap = ttk.Frame(frame_customer)
        btn_wrap.pack(fill="x", padx=8, pady=(0, 8))
        ttk.Button(btn_wrap, text="Them", command=self._add_customer_dialog).pack(side="left")
        ttk.Button(btn_wrap, text="Sua", command=self._edit_customer_dialog).pack(side="left", padx=6)
        ttk.Button(btn_wrap, text="Xoa", command=self._delete_customer).pack(side="left")

    def _build_tab_mapping(self) -> None:
        cols = ("dg_case", "file_path", "sheet_name", "cell", "file_hash")
        wrap = ttk.Frame(self.tab_mapping)
        wrap.pack(fill="both", expand=True)
        self.mapping_tree = ttk.Treeview(wrap, columns=cols, show="headings")
        for c, w in [
            ("dg_case", 150),
            ("file_path", 620),
            ("sheet_name", 180),
            ("cell", 90),
            ("file_hash", 280),
        ]:
            self.mapping_tree.heading(c, text=c)
            self.mapping_tree.column(c, width=w, anchor="w")
        y_scroll = ttk.Scrollbar(wrap, orient="vertical", command=self.mapping_tree.yview)
        x_scroll = ttk.Scrollbar(wrap, orient="horizontal", command=self.mapping_tree.xview)
        self.mapping_tree.configure(yscrollcommand=y_scroll.set, xscrollcommand=x_scroll.set)
        self.mapping_tree.grid(row=0, column=0, sticky="nsew")
        y_scroll.grid(row=0, column=1, sticky="ns")
        x_scroll.grid(row=1, column=0, sticky="ew")
        wrap.rowconfigure(0, weight=1)
        wrap.columnconfigure(0, weight=1)
        btn_wrap = ttk.Frame(self.tab_mapping)
        btn_wrap.pack(fill="x", pady=8)
        ttk.Button(btn_wrap, text="Them moi", command=self._add_mapping_dialog).pack(side="left")
        ttk.Button(btn_wrap, text="Xoa", command=self._delete_mapping).pack(side="left", padx=6)
        ttk.Button(btn_wrap, text="Tai lai", command=self._reload_mapping_tree).pack(side="left")

    def _build_tab_hash(self) -> None:
        cols = ("file_path", "sheet_name", "hash_value", "last_used")
        wrap = ttk.Frame(self.tab_hash)
        wrap.pack(fill="both", expand=True)
        self.cache_tree = ttk.Treeview(wrap, columns=cols, show="headings")
        for c, w in [("file_path", 630), ("sheet_name", 220), ("hash_value", 280), ("last_used", 200)]:
            self.cache_tree.heading(c, text=c)
            self.cache_tree.column(c, width=w, anchor="w")
        y_scroll = ttk.Scrollbar(wrap, orient="vertical", command=self.cache_tree.yview)
        x_scroll = ttk.Scrollbar(wrap, orient="horizontal", command=self.cache_tree.xview)
        self.cache_tree.configure(yscrollcommand=y_scroll.set, xscrollcommand=x_scroll.set)
        self.cache_tree.grid(row=0, column=0, sticky="nsew")
        y_scroll.grid(row=0, column=1, sticky="ns")
        x_scroll.grid(row=1, column=0, sticky="ew")
        wrap.rowconfigure(0, weight=1)
        wrap.columnconfigure(0, weight=1)

        btn_wrap = ttk.Frame(self.tab_hash)
        btn_wrap.pack(fill="x", pady=8)
        ttk.Button(btn_wrap, text="Tai lai", command=self._reload_cache_tree).pack(side="left")
        ttk.Button(btn_wrap, text="Xoa cache cu", command=self._delete_old_cache).pack(side="left", padx=6)
        ttk.Button(btn_wrap, text="Lam moi toan bo", command=self._clear_all_cache).pack(side="left")

    def _build_tab_check(self) -> None:
        self.check_notebook = ttk.Notebook(self.tab_check)
        self.check_notebook.pack(fill="both", expand=True)
        self.tab_check_pm = ttk.Frame(self.check_notebook, padding=8)
        self.tab_check_excel = ttk.Frame(self.check_notebook, padding=8)
        self.check_notebook.add(self.tab_check_pm, text="Check PM")
        self.check_notebook.add(self.tab_check_excel, text="Check Excel")
        self._build_pm_panel()
        self._build_excel_panel()

    def _build_result_tree(self, parent: ttk.Frame) -> ttk.Treeview:
        cols = (
            "dg_case",
            "ma_npl",
            "ten_npl",
            "mo_ta",
            "dvt",
            "sldm1",
            "so_luong",
            "khac",
            "chi_tiet",
            "trang_thai",
        )
        tree = ttk.Treeview(parent, columns=cols, show="headings")
        for c, w in [
            ("dg_case", 120),
            ("ma_npl", 140),
            ("ten_npl", 220),
            ("mo_ta", 220),
            ("dvt", 70),
            ("sldm1", 180),
            ("so_luong", 180),
            ("khac", 180),
            ("chi_tiet", 280),
            ("trang_thai", 80),
        ]:
            tree.heading(c, text=c)
            tree.column(c, width=w, anchor="w")
        tree.tag_configure("ok", background="#e8f7e8")
        tree.tag_configure("fail", background="#ffdede")
        return tree

    def _build_pm_panel(self) -> None:
        top = ttk.Frame(self.tab_check_pm)
        top.pack(fill="x")
        ttk.Label(top, text="So O / DG Case:").pack(side="left", padx=(0, 6))
        ttk.Entry(top, textvariable=self.dg_case_pm_var, width=24).pack(side="left")
        ttk.Button(top, text="Tim", command=lambda: self._search_dg_rows("pm")).pack(side="left", padx=8)
        ttk.Button(top, text="So sanh", command=lambda: self._start_compare_thread("pm")).pack(side="left")
        ttk.Button(top, text="Export", command=lambda: self._export_result("pm")).pack(side="left", padx=6)
        if self.back_to_launcher is not None:
            ttk.Button(top, text="Back ve Launcher", command=self._back_to_launcher).pack(side="left", padx=(8, 0))

        ttk.Label(
            self.tab_check_pm,
            textvariable=self.status_pm_var,
            padding=(0, 8),
            foreground="#1f4e79",
        ).pack(fill="x")
        self.progress_pm = ttk.Progressbar(self.tab_check_pm, mode="indeterminate")
        self.progress_pm.pack(fill="x", pady=(0, 8))

        split = ttk.PanedWindow(self.tab_check_pm, orient="vertical")
        split.pack(fill="both", expand=True)
        top_list = ttk.LabelFrame(split, text="Cung ma san pham (cot D), sap xep theo thoi gian")
        split.add(top_list, weight=1)
        self.pm_row_listbox = tk.Listbox(top_list, selectmode="extended", exportselection=False)
        self.pm_row_listbox.pack(side="left", fill="both", expand=True, padx=6, pady=6)
        pm_scroll = ttk.Scrollbar(top_list, orient="vertical", command=self.pm_row_listbox.yview)
        self.pm_row_listbox.configure(yscrollcommand=pm_scroll.set)
        pm_scroll.pack(side="right", fill="y", pady=6)

        bottom_list = ttk.LabelFrame(split, text="Ket qua Check PM")
        split.add(bottom_list, weight=2)
        wrap = ttk.Frame(bottom_list)
        wrap.pack(fill="both", expand=True, padx=6, pady=6)
        self.pm_result_tree = self._build_result_tree(wrap)
        y_scroll = ttk.Scrollbar(wrap, orient="vertical", command=self.pm_result_tree.yview)
        x_scroll = ttk.Scrollbar(wrap, orient="horizontal", command=self.pm_result_tree.xview)
        self.pm_result_tree.configure(yscrollcommand=y_scroll.set, xscrollcommand=x_scroll.set)
        self.pm_result_tree.grid(row=0, column=0, sticky="nsew")
        y_scroll.grid(row=0, column=1, sticky="ns")
        x_scroll.grid(row=1, column=0, sticky="ew")
        wrap.rowconfigure(0, weight=1)
        wrap.columnconfigure(0, weight=1)

    def _build_excel_panel(self) -> None:
        top = ttk.Frame(self.tab_check_excel)
        top.pack(fill="x")
        ttk.Label(top, text="So O / DG Case:").pack(side="left", padx=(0, 6))
        ttk.Entry(top, textvariable=self.dg_case_excel_var, width=24).pack(side="left")
        ttk.Label(top, text="Khach hang:").pack(side="left", padx=(16, 6))
        self.customer_combo_excel = ttk.Combobox(
            top,
            textvariable=self.selected_customer_var,
            state="readonly",
            width=40,
        )
        self.customer_combo_excel.pack(side="left")
        ttk.Button(top, text="Tim", command=lambda: self._search_dg_rows("excel")).pack(side="left", padx=8)
        ttk.Button(top, text="So sanh", command=lambda: self._start_compare_thread("excel")).pack(side="left")
        ttk.Button(top, text="Export", command=lambda: self._export_result("excel")).pack(side="left", padx=6)

        ttk.Label(
            self.tab_check_excel,
            textvariable=self.status_excel_var,
            padding=(0, 8),
            foreground="#1f4e79",
        ).pack(fill="x")
        self.progress_excel = ttk.Progressbar(self.tab_check_excel, mode="indeterminate")
        self.progress_excel.pack(fill="x", pady=(0, 8))

        split = ttk.PanedWindow(self.tab_check_excel, orient="vertical")
        split.pack(fill="both", expand=True)
        top_list = ttk.LabelFrame(split, text="Cung ma san pham (cot D), sap xep theo thoi gian")
        split.add(top_list, weight=1)
        self.excel_row_listbox = tk.Listbox(top_list, selectmode="extended", exportselection=False)
        self.excel_row_listbox.pack(side="left", fill="both", expand=True, padx=6, pady=6)
        excel_scroll = ttk.Scrollbar(top_list, orient="vertical", command=self.excel_row_listbox.yview)
        self.excel_row_listbox.configure(yscrollcommand=excel_scroll.set)
        excel_scroll.pack(side="right", fill="y", pady=6)

        bottom_list = ttk.LabelFrame(split, text="Ket qua Check Excel")
        split.add(bottom_list, weight=2)
        wrap = ttk.Frame(bottom_list)
        wrap.pack(fill="both", expand=True, padx=6, pady=6)
        self.excel_result_tree = self._build_result_tree(wrap)
        y_scroll = ttk.Scrollbar(wrap, orient="vertical", command=self.excel_result_tree.yview)
        x_scroll = ttk.Scrollbar(wrap, orient="horizontal", command=self.excel_result_tree.xview)
        self.excel_result_tree.configure(yscrollcommand=y_scroll.set, xscrollcommand=x_scroll.set)
        self.excel_result_tree.grid(row=0, column=0, sticky="nsew")
        y_scroll.grid(row=0, column=1, sticky="ns")
        x_scroll.grid(row=1, column=0, sticky="ew")
        wrap.rowconfigure(0, weight=1)
        wrap.columnconfigure(0, weight=1)

    def _load_setup_data(self) -> None:
        self.bom_link_var.set(self.db.get_setup_value("bom_link"))
        self._reload_customer_tree()
        self._reload_mapping_tree()
        self._reload_cache_tree()

    def _choose_bom_link(self) -> None:
        path = filedialog.askopenfilename(
            title="Chon bang ke",
            filetypes=[("Excel files", "*.xlsx *.xls"), ("All files", "*.*")],
        )
        if path:
            self.bom_link_var.set(path)

    def _save_bom_link(self) -> None:
        path = self.bom_link_var.get().strip()
        if not path:
            messagebox.showwarning("Setup", "Hay nhap duong dan bang ke.")
            return
        self.db.set_setup_value("bom_link", path)
        messagebox.showinfo("Setup", "Da luu duong dan bang ke.")

    def _open_customer_dialog(
        self, title: str, customer_name: str = "", code: str = "", folder_link: str = ""
    ) -> tuple[str, str, str] | None:
        dialog = tk.Toplevel(self.root)
        dialog.title(title)
        dialog.geometry("620x220")
        dialog.transient(self.root)
        dialog.grab_set()
        name_var = tk.StringVar(value=customer_name)
        code_var = tk.StringVar(value=code)
        folder_var = tk.StringVar(value=folder_link)

        ttk.Label(dialog, text="Khach hang").grid(row=0, column=0, sticky="w", padx=10, pady=8)
        ttk.Entry(dialog, textvariable=name_var, width=55).grid(row=0, column=1, sticky="ew", padx=10, pady=8)
        ttk.Label(dialog, text="Ma").grid(row=1, column=0, sticky="w", padx=10, pady=8)
        ttk.Entry(dialog, textvariable=code_var, width=55).grid(row=1, column=1, sticky="ew", padx=10, pady=8)
        ttk.Label(dialog, text="Link thu muc").grid(row=2, column=0, sticky="w", padx=10, pady=8)
        ttk.Entry(dialog, textvariable=folder_var, width=55).grid(row=2, column=1, sticky="ew", padx=10, pady=8)
        ttk.Button(
            dialog,
            text="Chon thu muc",
            command=lambda: folder_var.set(filedialog.askdirectory(title="Chon thu muc BOM") or folder_var.get()),
        ).grid(row=2, column=2, padx=10, pady=8)
        result: dict[str, tuple[str, str, str] | None] = {"value": None}

        def submit() -> None:
            result["value"] = (name_var.get().strip(), code_var.get().strip(), folder_var.get().strip())
            dialog.destroy()

        ttk.Button(dialog, text="Luu", command=submit).grid(row=3, column=1, sticky="e", padx=10, pady=12)
        ttk.Button(dialog, text="Huy", command=dialog.destroy).grid(row=3, column=2, sticky="w", padx=10, pady=12)
        dialog.columnconfigure(1, weight=1)
        self.root.wait_window(dialog)
        return result["value"]

    def _selected_customer_id(self) -> int | None:
        sel = self.customer_tree.selection()
        if not sel:
            return None
        vals = self.customer_tree.item(sel[0], "values")
        if not vals:
            return None
        return int(vals[0])

    def _reload_customer_tree(self) -> None:
        for item in self.customer_tree.get_children():
            self.customer_tree.delete(item)
        rows = self.db.get_customers()
        combo_values = []
        for row in rows:
            self.customer_tree.insert("", "end", values=row)
            combo_values.append(f"{row[0]} | {row[1]} | {row[2]}")
        if hasattr(self, "customer_combo_excel"):
            self.customer_combo_excel["values"] = combo_values
        if combo_values and not self.selected_customer_var.get():
            self.selected_customer_var.set(combo_values[0])

    def _add_customer_dialog(self) -> None:
        result = self._open_customer_dialog("Them khach hang")
        if not result:
            return
        name, code, link = result
        if not name or not link:
            messagebox.showwarning("Khach hang", "Ten va link thu muc la bat buoc.")
            return
        self.db.add_customer(name, code, link)
        self._reload_customer_tree()

    def _edit_customer_dialog(self) -> None:
        row_id = self._selected_customer_id()
        if row_id is None:
            messagebox.showwarning("Khach hang", "Hay chon dong can sua.")
            return
        values = self.customer_tree.item(self.customer_tree.selection()[0], "values")
        result = self._open_customer_dialog(
            "Sua khach hang",
            customer_name=str(values[1]),
            code=str(values[2]),
            folder_link=str(values[3]),
        )
        if not result:
            return
        name, code, link = result
        if not name or not link:
            messagebox.showwarning("Khach hang", "Ten va link thu muc la bat buoc.")
            return
        self.db.update_customer(row_id, name, code, link)
        self._reload_customer_tree()

    def _delete_customer(self) -> None:
        row_id = self._selected_customer_id()
        if row_id is None:
            messagebox.showwarning("Khach hang", "Hay chon dong can xoa.")
            return
        if not messagebox.askyesno("Khach hang", "Xoa khach hang da chon?"):
            return
        self.db.delete_customer(row_id)
        self._reload_customer_tree()

    def _reload_mapping_tree(self) -> None:
        for item in self.mapping_tree.get_children():
            self.mapping_tree.delete(item)
        for row in self.db.get_mappings():
            self.mapping_tree.insert("", "end", values=row)

    def _add_mapping_dialog(self) -> None:
        dg_case = simpledialog.askstring("Mapping", "Nhap DG Case:")
        if not dg_case:
            return
        file_path = filedialog.askopenfilename(
            title="Chon file BOM",
            filetypes=[("Excel files", "*.xlsx *.xls"), ("All files", "*.*")],
        )
        if not file_path:
            return
        try:
            found = self.searcher.search_in_file(file_path, dg_case.strip())
            if not found:
                messagebox.showwarning("Mapping", "Khong tim thay DG Case trong file nay.")
                return
            sheet_name, cell = found
            file_hash = compute_file_md5(file_path)
            self.db.upsert_mapping(dg_case.strip(), file_path, sheet_name, cell, file_hash)
            self._reload_mapping_tree()
            messagebox.showinfo("Mapping", f"Da luu: {sheet_name} - {cell}")
        except Exception as exc:
            logging.exception("Loi add mapping")
            messagebox.showerror("Mapping", str(exc))

    def _delete_mapping(self) -> None:
        sel = self.mapping_tree.selection()
        if not sel:
            messagebox.showwarning("Mapping", "Hay chon dong mapping can xoa.")
            return
        dg_case = str(self.mapping_tree.item(sel[0], "values")[0])
        if not messagebox.askyesno("Mapping", f"Xoa mapping {dg_case}?"):
            return
        self.db.delete_mapping(dg_case)
        self._reload_mapping_tree()

    def _reload_cache_tree(self) -> None:
        for item in self.cache_tree.get_children():
            self.cache_tree.delete(item)
        for row in self.db.get_all_cache_rows():
            self.cache_tree.insert("", "end", values=row)

    def _delete_old_cache(self) -> None:
        days = simpledialog.askinteger("Cache", "Xoa cache cu hon bao nhieu ngay?", initialvalue=7, minvalue=1)
        if not days:
            return
        deleted = self.db.delete_cache_older_than(days)
        self._reload_cache_tree()
        messagebox.showinfo("Cache", f"Da xoa {deleted} dong cache.")

    def _clear_all_cache(self) -> None:
        if not messagebox.askyesno("Cache", "Xoa toan bo cache?"):
            return
        self.db.clear_cache()
        self._reload_cache_tree()

    def _search_dg_rows(self, mode: str) -> None:
        try:
            dg_case = self.dg_case_pm_var.get().strip() if mode == "pm" else self.dg_case_excel_var.get().strip()
            if not dg_case:
                messagebox.showwarning("Check", "Hay nhap DG Case.")
                return
            bom_link = self.db.get_setup_value("bom_link").strip() or self.bom_link_var.get().strip()
            if not bom_link:
                messagebox.showwarning("Check", "Chua setup duong dan bang ke.")
                return
            self.loaded_bom_ke_df = self.parser.load_bom_ke(bom_link)
            subset = self._subset_by_dg_case(self.loaded_bom_ke_df, dg_case)
            target_listbox = self.pm_row_listbox if mode == "pm" else self.excel_row_listbox
            target_listbox.delete(0, tk.END)
            if subset.empty:
                if mode == "pm":
                    self.status_pm_var.set("Khong tim thay dong nao theo DG Case.")
                else:
                    self.status_excel_var.set("Khong tim thay dong nao theo DG Case.")
                return

            if mode == "pm":
                self.pm_current_subset = subset.copy()
                self.pm_display_time_keys = []
            else:
                self.excel_current_subset = subset.copy()
                self.excel_display_time_keys = []
                self.excel_display_row_indexes = []

            customer_folder = self._auto_pick_customer_from_subset(subset) if mode == "excel" else ""
            now = datetime.now()
            if mode == "pm":
                time_groups = subset.groupby("time_key", sort=False)
                for time_key, gdf in time_groups:
                    sample = gdf.iloc[0]
                    date_value = sample["order_date"]
                    date_label = ""
                    if pd.notna(date_value):
                        if date_value.date() < now.date():
                            date_label = "qua khu"
                        elif date_value.date() > now.date():
                            date_label = "tuong lai"
                        else:
                            date_label = "hien tai"
                        date_text = date_value.strftime("%d/%m/%Y")
                    else:
                        date_text = "N/A"
                    line = (
                        f"Ngay {date_text} ({date_label}) | DG={sample['dg_case']} | "
                        f"So dong NPL={len(gdf)} | Ma SP={sample['product_code']}"
                    )
                    target_listbox.insert(tk.END, line)
                    self.pm_display_time_keys.append(str(time_key))
            else:
                dg_rows = subset[
                    subset["dg_case"].map(lambda x: normalize_dg_case(dg_case) in normalize_dg_case(x))
                ].copy()
                if dg_rows.empty:
                    dg_rows = subset.copy()
                self.excel_current_subset = dg_rows
                for _, row in dg_rows.iterrows():
                    date_value = row["order_date"]
                    date_text = date_value.strftime("%d/%m/%Y") if pd.notna(date_value) else "N/A"
                    line = (
                        f"Dong {int(row['row_index'])} | DG={row['dg_case']} | Ngay {date_text} | "
                        f"Ma SP={row['product_code']} | Ma NPL={row['ma_npl']} | "
                        f"SLDM1={row['so_luong_dm_1']} | SL={row['so_luong']}"
                    )
                    target_listbox.insert(tk.END, line)
                    self.excel_display_row_indexes.append(int(row["row_index"]))
            base_code = normalize_text(subset.iloc[0]["product_code"])
            if mode == "excel" and customer_folder:
                self.status_excel_var.set(
                    f"Da tai {len(self.excel_current_subset)} dong theo DG Case {dg_case}. Da auto map khach hang."
                )
            else:
                target_status = self.status_pm_var if mode == "pm" else self.status_excel_var
                tail = " Vui long chon customer tay." if mode == "excel" else ""
                total = len(subset) if mode == "pm" else len(self.excel_current_subset)
                target_status.set(
                    f"Da tai {total} dong {'cung ma ' + base_code if mode == 'pm' else 'theo DG Case ' + dg_case}, sap xep theo ngay.{tail}"
                )
        except Exception as exc:
            logging.exception("Loi search DG rows")
            messagebox.showerror("Check", str(exc))

    def _back_to_launcher(self) -> None:
        if self.back_to_launcher is None:
            return
        self.root.destroy()
        self.back_to_launcher()

    def _start_compare_thread(self, mode: str) -> None:
        progress = self.progress_pm if mode == "pm" else self.progress_excel
        status_var = self.status_pm_var if mode == "pm" else self.status_excel_var
        progress.start(12)
        status_var.set("Dang xu ly...")
        thread = threading.Thread(target=lambda: self._run_compare(mode), daemon=True)
        thread.start()

    def _current_customer_folder(self) -> str:
        selected = self.selected_customer_var.get().strip()
        if not selected:
            raise ValueError("Hay chon khach hang.")
        customer_id = int(selected.split("|", 1)[0].strip())
        for row in self.db.get_customers():
            if int(row[0]) == customer_id:
                return str(row[3])
        raise ValueError("Khong tim thay thong tin khach hang.")

    def _selected_ke_rows(self, mode: str) -> pd.DataFrame:
        subset = self.pm_current_subset if mode == "pm" else self.excel_current_subset
        if subset is None or subset.empty:
            raise ValueError("Chua tai bang ke theo DG Case.")
        if mode == "pm":
            listbox = self.pm_row_listbox
            picked = listbox.curselection()
            if not picked or len(picked) < 2:
                return subset.copy()
            picked_keys = [self.pm_display_time_keys[idx] for idx in picked if 0 <= idx < len(self.pm_display_time_keys)]
            if not picked_keys:
                return subset.copy()
            return subset[subset["time_key"].isin(picked_keys)].copy()

        listbox = self.excel_row_listbox
        picked = listbox.curselection()
        if not picked:
            return subset.copy()
        row_indexes = [self.excel_display_row_indexes[idx] for idx in picked if 0 <= idx < len(self.excel_display_row_indexes)]
        if not row_indexes:
            return subset.copy()
        return subset[subset["row_index"].isin(row_indexes)].copy()

    def _run_compare(self, mode: str) -> None:
        try:
            dg_case = self.dg_case_pm_var.get().strip() if mode == "pm" else self.dg_case_excel_var.get().strip()
            if not dg_case:
                raise ValueError("Hay nhap DG Case.")
            rows = self._selected_ke_rows(mode)
            if rows.empty:
                raise ValueError("Khong co dong du lieu de so sanh.")

            if mode == "pm":
                results = self.comparator.compare_pm_only(rows)
            else:
                folder = self._current_customer_folder()
                file_path, sheet_name, _ = self.searcher.resolve_mapping(dg_case, folder)
                bom_rows = self.parser.load_bom_sheet(file_path, sheet_name)
                results = self.comparator.compare_pm_excel(rows, bom_rows, dg_case)

            out = pd.DataFrame([r.__dict__ for r in results])
            if mode == "pm":
                self.last_pm_result_df = out
                self.root.after(0, lambda: self._render_results(out, self.pm_result_tree))
                self.root.after(0, lambda: self.status_pm_var.set(f"Hoan tat: {len(out)} dong ket qua PM."))
            else:
                self.last_excel_result_df = out
                self.root.after(0, lambda: self._render_results(out, self.excel_result_tree))
                self.root.after(0, lambda: self.status_excel_var.set(f"Hoan tat: {len(out)} dong ket qua Excel."))
        except Exception as exc:
            logging.exception("Loi compare")
            self.root.after(0, lambda: messagebox.showerror("Check", str(exc)))
            target_status = self.status_pm_var if mode == "pm" else self.status_excel_var
            self.root.after(0, lambda: target_status.set("Co loi khi so sanh."))
        finally:
            target_progress = self.progress_pm if mode == "pm" else self.progress_excel
            self.root.after(0, target_progress.stop)
            self.root.after(0, self._reload_mapping_tree)
            self.root.after(0, self._reload_cache_tree)

    def _render_results(self, df: pd.DataFrame, tree: ttk.Treeview) -> None:
        for item in tree.get_children():
            tree.delete(item)
        for _, row in df.iterrows():
            sldm_text = f"moi:{row['sldm1_ke']} | cu:{row['sldm1_bom']}"
            qty_text = f"moi:{row['so_luong_ke']} | cu:{row['so_luong_bom']}"
            tag = "ok" if row["trang_thai"] == "✔️" else "fail"
            tree.insert(
                "",
                "end",
                values=(
                    row.get("dg_case", ""),
                    row["ma_npl"],
                    row["ten_npl"],
                    row["mo_ta"],
                    row["dvt"],
                    sldm_text,
                    qty_text,
                    row["khac"],
                    row["chi_tiet"],
                    row["trang_thai"],
                ),
                tags=(tag,),
            )

    def _export_result(self, mode: str) -> None:
        result_df = self.last_pm_result_df if mode == "pm" else self.last_excel_result_df
        dg_case = self.dg_case_pm_var.get().strip() if mode == "pm" else self.dg_case_excel_var.get().strip()
        if result_df is None or result_df.empty:
            messagebox.showwarning("Export", "Chua co ket qua de export.")
            return
        path = filedialog.asksaveasfilename(
            title="Luu ket qua Check BOM",
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx")],
            initialfile=f"check_bom_{mode}_{dg_case or 'result'}.xlsx",
        )
        if not path:
            return
        export_df = result_df.copy()
        export_df.to_excel(path, index=False)
        messagebox.showinfo("Export", f"Da xuat file:\n{path}")


def main(back_to_launcher: callable | None = None) -> None:
    root = tk.Tk()
    CheckBomApp(root, back_to_launcher=back_to_launcher)
    root.mainloop()


if __name__ == "__main__":
    main()
