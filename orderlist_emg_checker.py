import json
import re
import sqlite3
import tkinter as tk
from collections import Counter
from datetime import datetime
from pathlib import Path
from tkinter import filedialog, messagebox, simpledialog, ttk
import unicodedata

import pandas as pd


DB_FILE = "orderlist_emg_checker.db"
CONFIG_FILE = "orderlist_emg_checker_config.json"


def clean_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def clean_key(value: object) -> str:
    base = clean_text(value).lower()
    base = unicodedata.normalize("NFKD", base)
    base = "".join(ch for ch in base if not unicodedata.combining(ch))
    base = base.replace("đ", "d")
    return re.sub(r"[^a-z0-9]", "", base)


def to_number(value: object) -> float | None:
    if pd.isna(value):
        return None
    text = clean_text(value).replace(",", "")
    if text == "":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def mode_value(series: pd.Series) -> str:
    values = [clean_text(v) for v in series if clean_text(v)]
    if not values:
        return ""
    counter = Counter(values)
    return counter.most_common(1)[0][0]


def almost_equal(left: object, right: object, tol: float = 1e-6) -> bool:
    l_num = to_number(left)
    r_num = to_number(right)
    if l_num is not None and r_num is not None:
        return abs(l_num - r_num) <= tol
    return clean_key(left) == clean_key(right)


def normalize_logo(value: object) -> str:
    key = clean_key(value)
    if key in {"nologo", "nologo.", "nologo-"}:
        return "NL"
    return clean_text(value)


def qty_status(order_qty: object, bang_ke_qty: object) -> str:
    order_num = to_number(order_qty)
    bk_num = to_number(bang_ke_qty)
    if order_num is None or bk_num is None:
        return "Đúng" if almost_equal(order_qty, bang_ke_qty) else "Lệch"
    if abs(order_num - bk_num) <= 1e-6:
        return "Đúng"
    # Rule nghiệp vụ: Bảng kê lớn hơn Order List đúng 1 vẫn chấp nhận.
    if 0 < (bk_num - order_num) <= 1:
        return "Đúng"
    return "Lệch"


def parse_npl_950_code(code: object) -> tuple[str, str, str]:
    text = clean_text(code)
    if not text:
        return ("", "", "")
    parts = [p.strip() for p in text.split(".")]
    if len(parts) < 4:
        return ("", "", "")
    return (parts[1], parts[2], parts[3])


def parse_npl_color(code: object) -> str:
    text = clean_text(code)
    if not text:
        return ""
    parts = [p.strip() for p in text.split(".")]
    if len(parts) < 2:
        return ""
    return parts[1]


COLOR_CODE_MAP = {
    "100": "Black",
    "507": "Cam",
    "204": "Marine Blue",
    "503": "Red",
    "800": "White",
    "209": "Blue",
    "305": "Green",
    "702": "Yellow",
}


def color_name_from_code(color_code: str) -> str:
    key = clean_text(color_code)
    if not key:
        return ""
    return COLOR_CODE_MAP.get(key, key)


def normalize_color_name(value: object) -> str:
    key = clean_key(value)
    alias = {
        "black": "black",
        "cam": "cam",
        "orange": "cam",
        "marineblue": "marineblue",
        "red": "red",
        "white": "white",
        "blue": "blue",
        "green": "green",
        "yellow": "yellow",
    }
    return alias.get(key, key)


def load_config() -> dict:
    path = Path(CONFIG_FILE)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_config(data: dict) -> None:
    Path(CONFIG_FILE).write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def init_db() -> None:
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_type TEXT NOT NULL,
            target_dg TEXT,
            order_file TEXT NOT NULL,
            bang_ke_file TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS run_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            dg_case_no TEXT NOT NULL,
            field_name TEXT NOT NULL,
            order_value TEXT,
            bang_ke_value TEXT,
            status TEXT NOT NULL,
            FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
        )
        """
    )
    existing_cols = {row[1] for row in cur.execute("PRAGMA table_info(run_items)").fetchall()}
    if "auto_status" not in existing_cols:
        cur.execute("ALTER TABLE run_items ADD COLUMN auto_status TEXT")
    if "is_adjusted" not in existing_cols:
        cur.execute("ALTER TABLE run_items ADD COLUMN is_adjusted INTEGER DEFAULT 0")
    if "adjusted_at" not in existing_cols:
        cur.execute("ALTER TABLE run_items ADD COLUMN adjusted_at TEXT")
    if "adjust_reason" not in existing_cols:
        cur.execute("ALTER TABLE run_items ADD COLUMN adjust_reason TEXT")
    conn.commit()
    conn.close()


def find_bang_ke_header_row(file_path: str) -> int:
    preview = pd.read_excel(file_path, sheet_name=0, header=None, nrows=40)
    for idx, row in preview.iterrows():
        row_values = [clean_key(v) for v in row.tolist()]
        line = "|".join(v for v in row_values if v)
        if "soso" in line and "masanpham" in line and "ghichu" in line:
            return int(idx)
    raise ValueError("Không tìm thấy dòng header trong file Bảng Kê.")


def format_number(value: float | None) -> str:
    if value is None:
        return ""
    if float(value).is_integer():
        return str(int(value))
    return f"{value:.4f}".rstrip("0").rstrip(".")


def format_status_display(status_core: str, reason: str) -> str:
    core = clean_text(status_core)
    r = clean_text(reason)
    if r:
        return f"{core} — Lý do: {r}"
    return core


def parse_status_core_from_display(display: str) -> str:
    s = clean_text(display)
    if not s:
        return ""
    if s.startswith("Đúng"):
        return "Đúng"
    if s.startswith("Lệch"):
        return "Lệch"
    return s


def group_has_future_ship_date(group: pd.DataFrame, ship_col: int = 12) -> bool:
    now = pd.Timestamp.now()
    for v in group.iloc[:, ship_col]:
        ts = pd.to_datetime(v, errors="coerce")
        if pd.notna(ts) and ts > now:
            return True
    return False


class OrderlistCheckerApp:
    def __init__(self, root: tk.Tk):
        init_db()
        self.root = root
        self.root.title("ORDERLIST EMG Checker")
        self.root.geometry("1300x760")

        self.order_file_var = tk.StringVar()
        self.bang_ke_file_var = tk.StringVar()
        self.dg_case_var = tk.StringVar()
        self.status_var = tk.StringVar(value="Sẵn sàng.")
        self.clock_var = tk.StringVar(value="")
        self.date_from_var = tk.StringVar(value="")
        self.date_to_var = tk.StringVar(value="")

        self.last_result_df: pd.DataFrame | None = None
        self.current_run_id: int | None = None
        self.current_view_mode = "detail"
        self.history_current_run_type: str = ""
        self.history_current_run_id: int | None = None
        self.history_last_df: pd.DataFrame | None = None
        self.detail_win: tk.Toplevel | None = None
        self.detail_tree: ttk.Treeview | None = None
        self.detail_dg_case: str | None = None

        self.check_filter_o_var = tk.StringVar()
        self.check_filter_status_var = tk.StringVar(value="Tất cả")
        self.hist_filter_o_var = tk.StringVar()
        self.hist_filter_status_var = tk.StringVar(value="Tất cả")

        self.config = load_config()

        self._build_ui()
        self._load_last_paths()
        self._set_default_dates()
        self.refresh_history_runs()
        self._start_clock()

    def _build_ui(self) -> None:
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill="both", expand=True)

        self.tab_check = ttk.Frame(self.notebook, padding=0)
        self.tab_history = ttk.Frame(self.notebook, padding=0)
        self.notebook.add(self.tab_check, text="Check")
        self.notebook.add(self.tab_history, text="Lịch sử")

        top = ttk.Frame(self.tab_check, padding=10)
        top.pack(fill="x")

        ttk.Label(top, text="File ORDER LIST (sheet 1):").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=4)
        ttk.Entry(top, textvariable=self.order_file_var, width=100).grid(row=0, column=1, sticky="ew", padx=(0, 8), pady=4)
        ttk.Button(top, text="Chọn file", command=self.choose_order_file).grid(row=0, column=2, pady=4)

        ttk.Label(top, text="File Bảng Kê định mức:").grid(row=1, column=0, sticky="w", padx=(0, 8), pady=4)
        ttk.Entry(top, textvariable=self.bang_ke_file_var, width=100).grid(row=1, column=1, sticky="ew", padx=(0, 8), pady=4)
        ttk.Button(top, text="Chọn file", command=self.choose_bang_ke_file).grid(row=1, column=2, pady=4)

        ttk.Label(top, text="Run đơn theo DG Case No:").grid(row=2, column=0, sticky="w", padx=(0, 8), pady=(10, 4))
        ttk.Entry(top, textvariable=self.dg_case_var, width=32).grid(row=2, column=1, sticky="w", pady=(10, 4))
        action = ttk.Frame(top)
        action.grid(row=2, column=2, sticky="e", pady=(10, 4))
        ttk.Button(action, text="Run All", command=self.run_all).pack(side="left")
        ttk.Button(action, text="Run One", command=self.run_one).pack(side="left", padx=(6, 0))

        top.columnconfigure(1, weight=1)

        status_wrap = ttk.Frame(self.tab_check, padding=(10, 0, 10, 8))
        status_wrap.pack(fill="x")
        ttk.Label(status_wrap, textvariable=self.status_var, foreground="#1f4e79").pack(side="left")
        ttk.Label(status_wrap, textvariable=self.clock_var, foreground="#666666").pack(side="right")

        filter_check = ttk.Frame(self.tab_check, padding=(10, 0, 10, 4))
        filter_check.pack(fill="x")
        ttk.Label(filter_check, text="Lọc số O (DG):").pack(side="left")
        ttk.Entry(filter_check, textvariable=self.check_filter_o_var, width=22).pack(side="left", padx=(6, 14))
        ttk.Label(filter_check, text="Trạng thái:").pack(side="left")
        ttk.Combobox(
            filter_check,
            textvariable=self.check_filter_status_var,
            values=("Tất cả", "Chỉ lệch", "Không lệch"),
            width=12,
            state="readonly",
        ).pack(side="left", padx=(6, 10))
        ttk.Button(filter_check, text="Áp dụng lọc", command=self.apply_check_filters).pack(side="left")

        columns = ("dg_case_no", "field_name", "order_value", "bang_ke_value", "status")
        frame = ttk.Frame(self.tab_check, padding=(10, 0, 10, 10))
        frame.pack(fill="both", expand=True)
        self.tree = ttk.Treeview(frame, columns=columns, show="headings")
        for col, width in [
            ("dg_case_no", 160),
            ("field_name", 160),
            ("order_value", 260),
            ("bang_ke_value", 260),
            ("status", 360),
        ]:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=width, anchor="center")
        self.tree.pack(side="left", fill="both", expand=True)
        scrollbar = ttk.Scrollbar(frame, orient="vertical", command=self.tree.yview)
        scrollbar.pack(side="right", fill="y")
        self.tree.configure(yscrollcommand=scrollbar.set)
        self.tree.tag_configure("ok", background="#d7f5dd")
        self.tree.tag_configure("bad", background="#f9d8d8")
        self.tree.bind("<Double-1>", self.on_tree_double_click)

        action_bottom = ttk.Frame(self.tab_check, padding=(10, 0, 10, 10))
        action_bottom.pack(fill="x")
        ttk.Button(
            action_bottom,
            text="Toggle mục chọn Đúng/Lệch",
            command=self.toggle_selected_status_main,
        ).pack(side="left")

        self._build_history_tab()

    def apply_check_filters(self) -> None:
        if self.last_result_df is None or self.last_result_df.empty:
            messagebox.showinfo("Thông báo", "Chưa có dữ liệu. Hãy Run trước.")
            return
        if self.current_view_mode == "summary":
            self.render_summary()
        else:
            self.render_result()

    def _filter_check_detail_df(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df
        o = self.check_filter_o_var.get().strip()
        if o:
            key = clean_key(o)
            out = out[
                out["dg_case_no"].astype(str).apply(
                    lambda dg: key == clean_key(dg) or key in clean_key(dg)
                )
            ]
        st = self.check_filter_status_var.get()
        if st == "Chỉ lệch":
            out = out[out["status_core"] == "Lệch"]
        elif st == "Không lệch":
            out = out[out["status_core"] == "Đúng"]
        return out

    def _filter_check_summary_df(self, df: pd.DataFrame) -> pd.DataFrame:
        o = self.check_filter_o_var.get().strip()
        st = self.check_filter_status_var.get()
        mask = pd.Series(True, index=df.index)
        if o:
            key = clean_key(o)
            mask &= df["dg_case_no"].astype(str).apply(lambda dg: key == clean_key(dg) or key in clean_key(dg))
        if st == "Chỉ lệch":
            bad_dg = df.loc[df["status_core"] == "Lệch", "dg_case_no"].unique()
            mask &= df["dg_case_no"].isin(bad_dg)
        elif st == "Không lệch":
            ok_dgs: list[str] = []
            for dg, g in df.groupby("dg_case_no"):
                if (g["status_core"] == "Lệch").sum() == 0:
                    ok_dgs.append(dg)
            mask &= df["dg_case_no"].isin(ok_dgs)
        return df[mask]

    def apply_history_item_filters(self) -> None:
        if self.history_last_df is None or self.history_last_df.empty:
            messagebox.showinfo("Thông báo", "Chọn một run và tải chi tiết trước.")
            return
        self._render_history_items_from_df(self.history_last_df)

    def choose_order_file(self) -> None:
        path = filedialog.askopenfilename(
            title="Chọn file ORDER LIST",
            filetypes=[("Excel files", "*.xlsx *.xls")],
        )
        if path:
            self.order_file_var.set(path)
            self._save_last_paths()

    def _build_history_tab(self) -> None:
        filter_wrap = ttk.Frame(self.tab_history, padding=10)
        filter_wrap.pack(fill="x")
        ttk.Label(filter_wrap, text="Từ ngày (YYYY-MM-DD):").pack(side="left")
        ttk.Entry(filter_wrap, textvariable=self.date_from_var, width=14).pack(side="left", padx=(6, 10))
        ttk.Label(filter_wrap, text="Đến ngày (YYYY-MM-DD):").pack(side="left")
        ttk.Entry(filter_wrap, textvariable=self.date_to_var, width=14).pack(side="left", padx=(6, 10))
        ttk.Button(filter_wrap, text="Lọc", command=self.refresh_history_runs).pack(side="left")
        ttk.Button(filter_wrap, text="Hôm nay", command=self._set_default_dates).pack(side="left", padx=(6, 0))

        hist_filter_row = ttk.Frame(self.tab_history, padding=(10, 0, 10, 4))
        hist_filter_row.pack(fill="x")
        ttk.Label(hist_filter_row, text="Lọc số O (DG):").pack(side="left")
        ttk.Entry(hist_filter_row, textvariable=self.hist_filter_o_var, width=22).pack(side="left", padx=(6, 14))
        ttk.Label(hist_filter_row, text="Trạng thái:").pack(side="left")
        ttk.Combobox(
            hist_filter_row,
            textvariable=self.hist_filter_status_var,
            values=("Tất cả", "Chỉ lệch", "Không lệch"),
            width=12,
            state="readonly",
        ).pack(side="left", padx=(6, 10))
        ttk.Button(hist_filter_row, text="Áp dụng lọc chi tiết", command=self.apply_history_item_filters).pack(side="left")

        run_cols = ("run_id", "created_at", "run_type", "target_dg", "total", "lech", "adjusted")
        run_wrap = ttk.Frame(self.tab_history, padding=(10, 0, 10, 6))
        run_wrap.pack(fill="both", expand=True)
        self.history_runs_tree = ttk.Treeview(run_wrap, columns=run_cols, show="headings", height=10)
        for col, width in [
            ("run_id", 70),
            ("created_at", 160),
            ("run_type", 90),
            ("target_dg", 150),
            ("total", 80),
            ("lech", 80),
            ("adjusted", 90),
        ]:
            self.history_runs_tree.heading(col, text=col)
            self.history_runs_tree.column(col, width=width, anchor="center")
        self.history_runs_tree.pack(side="left", fill="both", expand=True)
        run_scroll = ttk.Scrollbar(run_wrap, orient="vertical", command=self.history_runs_tree.yview)
        run_scroll.pack(side="right", fill="y")
        self.history_runs_tree.configure(yscrollcommand=run_scroll.set)
        self.history_runs_tree.bind("<Double-1>", self.load_history_run_items)

        detail_cols = ("dg_case_no", "field_name", "order_value", "bang_ke_value", "status")
        detail_wrap = ttk.Frame(self.tab_history, padding=(10, 0, 10, 10))
        detail_wrap.pack(fill="both", expand=True)
        self.history_items_tree = ttk.Treeview(detail_wrap, columns=detail_cols, show="headings", height=12)
        for col, width in [
            ("dg_case_no", 160),
            ("field_name", 160),
            ("order_value", 260),
            ("bang_ke_value", 260),
            ("status", 360),
        ]:
            self.history_items_tree.heading(col, text=col)
            self.history_items_tree.column(col, width=width, anchor="center")
        self.history_items_tree.pack(side="left", fill="both", expand=True)
        detail_scroll = ttk.Scrollbar(detail_wrap, orient="vertical", command=self.history_items_tree.yview)
        detail_scroll.pack(side="right", fill="y")
        self.history_items_tree.configure(yscrollcommand=detail_scroll.set)
        self.history_items_tree.tag_configure("ok", background="#d7f5dd")
        self.history_items_tree.tag_configure("bad", background="#f9d8d8")
        self.history_items_tree.bind("<Double-1>", self.on_history_items_double_click)

    def _set_default_dates(self) -> None:
        today = datetime.now().strftime("%Y-%m-%d")
        self.date_from_var.set(today)
        self.date_to_var.set(today)
        if hasattr(self, "history_runs_tree"):
            self.refresh_history_runs()

    def _start_clock(self) -> None:
        self.clock_var.set(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        self.root.after(1000, self._start_clock)

    def refresh_history_runs(self) -> None:
        if not hasattr(self, "history_runs_tree"):
            return
        for item in self.history_runs_tree.get_children():
            self.history_runs_tree.delete(item)
        for item in self.history_items_tree.get_children():
            self.history_items_tree.delete(item)

        date_from = self.date_from_var.get().strip()
        date_to = self.date_to_var.get().strip()
        where = []
        params: list[str] = []
        if date_from:
            where.append("date(r.created_at) >= date(?)")
            params.append(date_from)
        if date_to:
            where.append("date(r.created_at) <= date(?)")
            params.append(date_to)
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""

        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()
        rows = cur.execute(
            f"""
            SELECT
                r.id,
                r.created_at,
                r.run_type,
                COALESCE(r.target_dg, ''),
                COUNT(i.id) AS total_checks,
                SUM(CASE WHEN i.status = 'Lệch' THEN 1 ELSE 0 END) AS bad_checks,
                SUM(CASE WHEN COALESCE(i.is_adjusted, 0) = 1 THEN 1 ELSE 0 END) AS adjusted_checks
            FROM runs r
            LEFT JOIN run_items i ON i.run_id = r.id
            {where_sql}
            GROUP BY r.id
            ORDER BY r.id DESC
            """,
            params,
        ).fetchall()
        conn.close()

        for row in rows:
            self.history_runs_tree.insert("", "end", values=row)

    def load_history_run_items(self, _event: tk.Event | None = None) -> None:
        selected = self.history_runs_tree.selection()
        if not selected:
            return
        run_vals = self.history_runs_tree.item(selected[0], "values")
        if not run_vals:
            return
        run_id = int(run_vals[0])
        run_type = str(run_vals[2])
        self.history_current_run_id = run_id
        self.history_current_run_type = run_type
        for item in self.history_items_tree.get_children():
            self.history_items_tree.delete(item)

        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()
        rows = cur.execute(
            """
            SELECT dg_case_no, field_name, order_value, bang_ke_value, status,
                   COALESCE(adjust_reason, '')
            FROM run_items
            WHERE run_id = ?
            ORDER BY dg_case_no, field_name
            """,
            (run_id,),
        ).fetchall()
        conn.close()
        self.history_last_df = pd.DataFrame(
            rows,
            columns=[
                "dg_case_no",
                "field_name",
                "order_value",
                "bang_ke_value",
                "status_core",
                "adjust_reason",
            ],
        )
        if self.history_last_df.empty:
            return
        self._render_history_items_from_df(self.history_last_df, run_type)

    def _filter_history_items_df(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        o = self.hist_filter_o_var.get().strip()
        if o:
            key = clean_key(o)
            out = out[out["dg_case_no"].astype(str).apply(lambda dg: key == clean_key(dg) or key in clean_key(dg))]
        st = self.hist_filter_status_var.get()
        if st == "Chỉ lệch":
            out = out[out["status_core"] == "Lệch"]
        elif st == "Không lệch":
            out = out[out["status_core"] == "Đúng"]
        return out

    def _filter_history_summary_df(self, df: pd.DataFrame) -> pd.DataFrame:
        return self._filter_check_summary_df(df)

    def _render_history_items_from_df(self, df: pd.DataFrame, run_type: str) -> None:
        for item in self.history_items_tree.get_children():
            self.history_items_tree.delete(item)
        if df.empty:
            return
        if run_type == "all":
            view = self._filter_history_summary_df(df)
            if view.empty:
                return
            summary = (
                view.groupby("dg_case_no", as_index=False)
                .agg(
                    total_checks=("status_core", "size"),
                    bad_checks=("status_core", lambda s: int((s == "Lệch").sum())),
                )
                .sort_values("dg_case_no", kind="stable")
            )
            for rec in summary.to_dict("records"):
                status_core = "Đúng" if rec["bad_checks"] == 0 else "Lệch"
                tag = "ok" if status_core == "Đúng" else "bad"
                self.history_items_tree.insert(
                    "",
                    "end",
                    values=(
                        rec["dg_case_no"],
                        "Tổng quan",
                        f"Tổng check: {rec['total_checks']}",
                        f"Số lệch: {rec['bad_checks']}",
                        status_core,
                    ),
                    tags=(tag,),
                )
        else:
            view = self._filter_history_items_df(df)
            for rec in view.to_dict("records"):
                disp = format_status_display(str(rec["status_core"]), str(rec.get("adjust_reason", "")))
                tag = "ok" if rec["status_core"] == "Đúng" else "bad"
                self.history_items_tree.insert(
                    "",
                    "end",
                    values=(
                        rec["dg_case_no"],
                        rec["field_name"],
                        rec["order_value"],
                        rec["bang_ke_value"],
                        disp,
                    ),
                    tags=(tag,),
                )

    def on_history_items_double_click(self, _event: tk.Event | None = None) -> None:
        if self.history_current_run_type != "all" or self.history_last_df is None or self.history_last_df.empty:
            return
        selected = self.history_items_tree.selection()
        if not selected:
            return
        vals = self.history_items_tree.item(selected[0], "values")
        if not vals:
            return
        dg_case = str(vals[0]).strip()
        if not dg_case:
            return
        detail_df = self.history_last_df[self.history_last_df["dg_case_no"] == dg_case]
        if detail_df.empty:
            return
        self.open_detail_window(dg_case, detail_df)

    def choose_bang_ke_file(self) -> None:
        path = filedialog.askopenfilename(
            title="Chọn file Bảng Kê định mức",
            filetypes=[("Excel files", "*.xlsx *.xls")],
        )
        if path:
            self.bang_ke_file_var.set(path)
            self._save_last_paths()

    def _load_last_paths(self) -> None:
        self.order_file_var.set(self.config.get("order_file", ""))
        self.bang_ke_file_var.set(self.config.get("bang_ke_file", ""))

    def _save_last_paths(self) -> None:
        self.config["order_file"] = self.order_file_var.get().strip()
        self.config["bang_ke_file"] = self.bang_ke_file_var.get().strip()
        save_config(self.config)

    def run_all(self) -> None:
        self._run(run_type="all", target_dg=None)

    def run_one(self) -> None:
        dg_case = self.dg_case_var.get().strip()
        if not dg_case:
            messagebox.showwarning("Thiếu DG Case", "Nhập DG Case No để chạy Run One.")
            return
        self._run(run_type="one", target_dg=dg_case)

    def _run(self, run_type: str, target_dg: str | None) -> None:
        order_file = self.order_file_var.get().strip()
        bang_ke_file = self.bang_ke_file_var.get().strip()
        if not order_file or not bang_ke_file:
            messagebox.showwarning("Thiếu file", "Hãy chọn đủ file ORDER LIST và Bảng Kê.")
            return
        if not Path(order_file).exists() or not Path(bang_ke_file).exists():
            messagebox.showerror("Sai đường dẫn", "Một trong hai file không tồn tại.")
            return

        self.status_var.set("Đang xử lý dữ liệu...")
        self.root.update_idletasks()
        try:
            result_df = self.compare_files(order_file, bang_ke_file, target_dg)
            if result_df.empty:
                self.render_result(result_df)
                self.status_var.set(
                    "Không có dữ liệu khớp điều kiện (EMG + DG + Ship date tương lai + Bảng kê)."
                )
                return
            run_id = self.save_run(run_type, target_dg, order_file, bang_ke_file, result_df)
            self.current_run_id = run_id
            self.last_result_df = result_df
            if run_type == "all":
                self.render_summary(result_df)
            else:
                self.render_result(result_df)
            ok_count = int((result_df["status_core"] == "Đúng").sum())
            bad_count = int((result_df["status_core"] == "Lệch").sum())
            msg = f"Hoàn tất. Run ID {run_id} | Tổng check: {len(result_df)} | Đúng: {ok_count} | Lệch: {bad_count}"
            if run_type == "all":
                msg += " | Double-click DG Case No để xem chi tiết."
            self.status_var.set(msg)
            self.refresh_history_runs()
        except Exception as exc:
            messagebox.showerror("Lỗi", str(exc))
            self.status_var.set("Có lỗi khi xử lý.")

    def compare_files(self, order_file: str, bang_ke_file: str, target_dg: str | None) -> pd.DataFrame:
        order_df = pd.read_excel(order_file, sheet_name=0, header=0)
        header_row = find_bang_ke_header_row(bang_ke_file)
        bang_ke_df = pd.read_excel(bang_ke_file, sheet_name=0, header=header_row)

        # ORDER LIST: lọc dòng của KH EMG và có DG Case No.
        order_df = order_df.copy()
        order_df["customer_key"] = order_df.iloc[:, 5].apply(clean_key)
        order_df["dg_case"] = order_df.iloc[:, 2].apply(clean_text)
        order_df = order_df[(order_df["customer_key"] == "emg") & (order_df["dg_case"] != "")]
        if target_dg:
            target_key = clean_key(target_dg)
            order_df = order_df[order_df["dg_case"].apply(clean_key) == target_key]

        if order_df.empty:
            return pd.DataFrame(
                columns=[
                    "dg_case_no",
                    "field_name",
                    "order_value",
                    "bang_ke_value",
                    "auto_status",
                    "status_core",
                    "adjust_reason",
                ]
            )

        records: list[dict] = []
        for dg_case, group in order_df.groupby("dg_case", dropna=False):
            if not group_has_future_ship_date(group, ship_col=12):
                continue
            order_order_no = mode_value(group.iloc[:, 1])
            order_qty_total = sum(v for v in group.iloc[:, 6].apply(to_number) if v is not None)
            order_logo = normalize_logo(mode_value(group.iloc[:, 9]))
            order_ma_sp = mode_value(group.iloc[:, 7])
            order_ten_sp = mode_value(group.iloc[:, 8])
            order_color_k = mode_value(group.iloc[:, 10])
            order_color_s = mode_value(group.iloc[:, 18])
            order_size_t = mode_value(group.iloc[:, 19])
            order_size_u = mode_value(group.iloc[:, 20])
            order_carton_qty = sum(v for v in group.iloc[:, 22].apply(to_number) if v is not None)

            bang_ke_match = bang_ke_df[bang_ke_df.iloc[:, 0].apply(clean_key) == clean_key(dg_case)]
            bk_note = mode_value(bang_ke_match.iloc[:, 8]) if not bang_ke_match.empty else ""
            bk_logo = normalize_logo(mode_value(bang_ke_match.iloc[:, 7])) if not bang_ke_match.empty else ""
            bk_ma_sp = mode_value(bang_ke_match.iloc[:, 3]) if not bang_ke_match.empty else ""
            bk_ten_sp = mode_value(bang_ke_match.iloc[:, 4]) if not bang_ke_match.empty else ""

            # Carton (thùng) và Pallet: tách 2 luật, luôn có 2 nhóm dòng so riêng.
            pallet_rows = pd.DataFrame()
            carton_only_rows = pd.DataFrame()
            if not bang_ke_match.empty:
                pallet_mask = bang_ke_match.iloc[:, 9].apply(lambda v: "948pallet" in clean_key(v))
                pallet_rows = bang_ke_match[pallet_mask]
                carton_mask = bang_ke_match.apply(
                    lambda row: (
                        ("cartonbox" in clean_key(row.iloc[10]))
                        or ("cartonbox" in clean_key(row.iloc[11]))
                        or ("carton" in clean_key(row.iloc[10]))
                        or ("carton" in clean_key(row.iloc[11]))
                    ),
                    axis=1,
                )
                carton_only_rows = bang_ke_match[carton_mask & ~pallet_mask]

            bk_qty_carton = mode_value(carton_only_rows.iloc[:, 6]) if not carton_only_rows.empty else ""
            bk_qty_pallet = mode_value(pallet_rows.iloc[:, 6]) if not pallet_rows.empty else ""

            carton_qty_bk = mode_value(carton_only_rows.iloc[:, 15]) if not carton_only_rows.empty else ""
            carton_npl_bk = mode_value(carton_only_rows.iloc[:, 9]) if not carton_only_rows.empty else ""
            npl_x, npl_y, npl_z = parse_npl_950_code(carton_npl_bk)
            bang_ke_size_xyz_carton = ".".join(v for v in [npl_x, npl_y, npl_z] if clean_text(v))

            pallet_nums = [v for v in pallet_rows.iloc[:, 15].apply(to_number) if v is not None]
            pallet_qty_bk = format_number(sum(pallet_nums)) if pallet_nums else ""
            has_pallet = not pallet_rows.empty
            fabric_rows = pd.DataFrame()
            if not bang_ke_match.empty:
                fabric_rows = bang_ke_match[
                    bang_ke_match.iloc[:, 11].apply(lambda v: "vai" in clean_key(v))
                ].copy()
            fabric_color_code = ""
            fabric_color_name = ""
            fabric_npl_full = ""
            fabric_npl_desc = ""
            if not fabric_rows.empty:
                fabric_rows["qty_p"] = fabric_rows.iloc[:, 15].apply(to_number).fillna(0)
                fabric_top = fabric_rows.sort_values("qty_p", ascending=False).iloc[0]
                fabric_color_code = parse_npl_color(fabric_top.iloc[9])
                fabric_color_name = color_name_from_code(fabric_color_code)
                fabric_npl_full = clean_text(fabric_top.iloc[9])
                fabric_npl_desc = clean_text(fabric_top.iloc[10])

            order_size_xyz = ".".join(v for v in [order_color_s, order_size_t, order_size_u] if clean_text(v))
            color_display = f"{fabric_npl_full} | {fabric_npl_desc} ({fabric_color_name})".strip(" |")
            if not fabric_npl_full and not fabric_npl_desc:
                color_display = ""

            checks = [
                ("Đơn hàng", order_order_no, bk_note),
                ("Số lượng (carton)", format_number(order_qty_total), bk_qty_carton),
                ("Số lượng (pallet)", format_number(order_qty_total), bk_qty_pallet),
                ("Logo", order_logo, bk_logo),
                ("Mã sản phẩm", order_ma_sp, bk_ma_sp),
                ("Tên sản phẩm", order_ten_sp, bk_ten_sp),
                ("Màu sắc", order_color_k, color_display),
                ("Số thùng (carton)", format_number(order_carton_qty), carton_qty_bk),
                ("Size thùng", order_size_xyz, bang_ke_size_xyz_carton),
                ("Số thùng (pallet)", format_number(order_carton_qty), pallet_qty_bk),
                ("Size pallet", order_size_xyz, "120.100.h" if has_pallet else "(không có dòng pallet)"),
            ]
            for field_name, left, right in checks:
                if field_name.startswith("Số lượng"):
                    status = qty_status(left, right)
                elif field_name == "Màu sắc":
                    status = (
                        "Đúng"
                        if normalize_color_name(left) == normalize_color_name(fabric_color_name)
                        else "Lệch"
                    )
                elif field_name == "Size pallet":
                    if has_pallet:
                        status = "Lệch"
                    else:
                        status = "Đúng"
                else:
                    status = "Đúng" if almost_equal(left, right) else "Lệch"
                records.append(
                    {
                        "dg_case_no": dg_case,
                        "field_name": field_name,
                        "order_value": clean_text(left),
                        "bang_ke_value": clean_text(right),
                        "auto_status": status,
                        "status_core": status,
                        "adjust_reason": "",
                    }
                )
        result = pd.DataFrame(records)
        if result.empty:
            return pd.DataFrame(
                columns=[
                    "dg_case_no",
                    "field_name",
                    "order_value",
                    "bang_ke_value",
                    "auto_status",
                    "status_core",
                    "adjust_reason",
                ]
            )
        return result.sort_values(["dg_case_no", "field_name"], kind="stable").reset_index(drop=True)

    def save_run(
        self,
        run_type: str,
        target_dg: str | None,
        order_file: str,
        bang_ke_file: str,
        result_df: pd.DataFrame,
    ) -> int:
        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO runs (run_type, target_dg, order_file, bang_ke_file)
            VALUES (?, ?, ?, ?)
            """,
            (run_type, target_dg or "", order_file, bang_ke_file),
        )
        run_id = int(cur.lastrowid)
        rows = [
            (
                run_id,
                str(rec["dg_case_no"]),
                str(rec["field_name"]),
                str(rec["order_value"]),
                str(rec["bang_ke_value"]),
                str(rec.get("auto_status", rec["status_core"])),
                str(rec["status_core"]),
                0,
                str(rec.get("adjust_reason", "")),
            )
            for rec in result_df.to_dict("records")
        ]
        cur.executemany(
            """
            INSERT INTO run_items (
                run_id, dg_case_no, field_name, order_value, bang_ke_value,
                auto_status, status, is_adjusted, adjust_reason
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
        conn.close()
        self.refresh_history_runs()
        return run_id

    def render_result(self, result_df: pd.DataFrame | None = None) -> None:
        self.current_view_mode = "detail"
        base = result_df if result_df is not None else self.last_result_df
        if base is None:
            base = pd.DataFrame()
        view = self._filter_check_detail_df(base) if not base.empty else base
        for item in self.tree.get_children():
            self.tree.delete(item)
        for rec in view.to_dict("records"):
            disp = format_status_display(str(rec["status_core"]), str(rec.get("adjust_reason", "")))
            tag = "ok" if rec["status_core"] == "Đúng" else "bad"
            self.tree.insert(
                "",
                "end",
                values=(
                    rec["dg_case_no"],
                    rec["field_name"],
                    rec["order_value"],
                    rec["bang_ke_value"],
                    disp,
                ),
                tags=(tag,),
            )

    def render_summary(self, result_df: pd.DataFrame | None = None) -> None:
        self.current_view_mode = "summary"
        base = result_df if result_df is not None else self.last_result_df
        if base is None:
            base = pd.DataFrame()
        view = self._filter_check_summary_df(base) if not base.empty else base
        for item in self.tree.get_children():
            self.tree.delete(item)
        if view.empty:
            return
        summary = (
            view.groupby("dg_case_no", as_index=False)
            .agg(
                total_checks=("status_core", "size"),
                bad_checks=("status_core", lambda s: int((s == "Lệch").sum())),
            )
            .sort_values("dg_case_no", kind="stable")
        )
        for rec in summary.to_dict("records"):
            status_core = "Đúng" if rec["bad_checks"] == 0 else "Lệch"
            detail_left = f"Tổng check: {rec['total_checks']}"
            detail_right = f"Số lệch: {rec['bad_checks']}"
            tag = "ok" if status_core == "Đúng" else "bad"
            self.tree.insert(
                "",
                "end",
                values=(rec["dg_case_no"], "Tổng quan", detail_left, detail_right, status_core),
                tags=(tag,),
            )

    def on_tree_double_click(self, _event: tk.Event) -> None:
        if self.current_view_mode != "summary" or self.last_result_df is None or self.last_result_df.empty:
            return
        selected = self.tree.selection()
        if not selected:
            return
        values = self.tree.item(selected[0], "values")
        if not values:
            return
        dg_case = str(values[0]).strip()
        if not dg_case:
            return
        detail_df = self.last_result_df[self.last_result_df["dg_case_no"] == dg_case]
        if detail_df.empty:
            return
        self.open_detail_window(dg_case, detail_df)

    def open_detail_window(self, dg_case: str, detail_df: pd.DataFrame) -> None:
        win = tk.Toplevel(self.root)
        win.title(f"Chi tiết {dg_case}")
        win.geometry("1120x560")
        self.detail_win = win
        self.detail_dg_case = dg_case

        def _on_detail_close() -> None:
            self.detail_win = None
            self.detail_tree = None
            self.detail_dg_case = None
            win.destroy()

        win.protocol("WM_DELETE_WINDOW", _on_detail_close)

        cols = ("field_name", "order_value", "bang_ke_value", "status")
        tree = ttk.Treeview(win, columns=cols, show="headings")
        self.detail_tree = tree
        for col, width in [
            ("field_name", 180),
            ("order_value", 300),
            ("bang_ke_value", 300),
            ("status", 420),
        ]:
            tree.heading(col, text=col)
            tree.column(col, width=width, anchor="center")
        tree.pack(side="left", fill="both", expand=True, padx=(10, 0), pady=10)
        scrollbar = ttk.Scrollbar(win, orient="vertical", command=tree.yview)
        scrollbar.pack(side="right", fill="y", padx=(0, 10), pady=10)
        tree.configure(yscrollcommand=scrollbar.set)
        tree.tag_configure("ok", background="#d7f5dd")
        tree.tag_configure("bad", background="#f9d8d8")
        ttk.Button(
            win,
            text="Toggle mục chọn Đúng/Lệch",
            command=lambda: self.toggle_selected_status_detail(tree, dg_case),
        ).pack(anchor="w", padx=10, pady=(0, 8))

        for rec in detail_df.to_dict("records"):
            disp = format_status_display(str(rec["status_core"]), str(rec.get("adjust_reason", "")))
            tag = "ok" if rec["status_core"] == "Đúng" else "bad"
            tree.insert(
                "",
                "end",
                values=(rec["field_name"], rec["order_value"], rec["bang_ke_value"], disp),
                tags=(tag,),
            )

    def toggle_selected_status_main(self) -> None:
        if self.current_view_mode != "detail":
            messagebox.showinfo("Thông báo", "Hãy mở chi tiết trước khi chỉnh.")
            return
        selected = self.tree.selection()
        if not selected:
            messagebox.showinfo("Thông báo", "Chọn dòng cần chỉnh.")
            return
        vals = self.tree.item(selected[0], "values")
        if not vals:
            return
        dg_case, field_name, order_value, bang_ke_value, current_display = [str(v) for v in vals]
        if self.toggle_item_status(dg_case, field_name, order_value, bang_ke_value, current_display) is None:
            return

    def toggle_selected_status_detail(self, tree: ttk.Treeview, dg_case: str) -> None:
        selected = tree.selection()
        if not selected:
            messagebox.showinfo("Thông báo", "Chọn dòng cần chỉnh.")
            return
        vals = tree.item(selected[0], "values")
        if not vals:
            return
        field_name, order_value, bang_ke_value, current_display = [str(v) for v in vals]
        if self.toggle_item_status(dg_case, field_name, order_value, bang_ke_value, current_display) is None:
            return

    def _refresh_detail_popup(self, dg_case: str) -> None:
        if self.detail_tree is None or self.detail_dg_case != dg_case:
            return
        if self.last_result_df is None or self.last_result_df.empty:
            return
        sub = self.last_result_df[self.last_result_df["dg_case_no"] == dg_case]
        for item in self.detail_tree.get_children():
            self.detail_tree.delete(item)
        for rec in sub.to_dict("records"):
            disp = format_status_display(str(rec["status_core"]), str(rec.get("adjust_reason", "")))
            tag = "ok" if rec["status_core"] == "Đúng" else "bad"
            self.detail_tree.insert(
                "",
                "end",
                values=(rec["field_name"], rec["order_value"], rec["bang_ke_value"], disp),
                tags=(tag,),
            )

    def _sync_views_after_toggle(self, dg_case: str) -> None:
        if self.last_result_df is None:
            return
        if self.current_view_mode == "summary":
            self.render_summary()
        else:
            self.render_result()
        self._refresh_detail_popup(dg_case)
        self.refresh_history_runs()
        if (
            self.history_current_run_id is not None
            and self.history_current_run_id == self.current_run_id
            and self.last_result_df is not None
            and not self.last_result_df.empty
        ):
            self.history_last_df = self.last_result_df[
                [
                    "dg_case_no",
                    "field_name",
                    "order_value",
                    "bang_ke_value",
                    "status_core",
                    "adjust_reason",
                ]
            ].copy()
            self._render_history_items_from_df(self.history_last_df, self.history_current_run_type)

    def toggle_item_status(
        self, dg_case: str, field_name: str, order_value: str, bang_ke_value: str, current_display: str
    ) -> str | None:
        current_core = parse_status_core_from_display(current_display)
        raw = simpledialog.askstring(
            "Lý do điều chỉnh",
            "Nhập lý do (bắt buộc) trước khi đổi trạng thái:",
            parent=self.root,
        )
        if raw is None:
            return None
        reason = raw.strip()
        if not reason:
            messagebox.showwarning("Thiếu lý do", "Vui lòng nhập lý do điều chỉnh.")
            return None

        auto_status = ""
        if self.last_result_df is not None and not self.last_result_df.empty:
            mask = (
                (self.last_result_df["dg_case_no"] == dg_case)
                & (self.last_result_df["field_name"] == field_name)
                & (self.last_result_df["order_value"] == order_value)
                & (self.last_result_df["bang_ke_value"] == bang_ke_value)
            )
            if mask.any() and "auto_status" in self.last_result_df.columns:
                auto_status = clean_text(self.last_result_df.loc[mask, "auto_status"].iloc[0])

        if self.current_run_id is not None and not auto_status:
            conn = sqlite3.connect(DB_FILE)
            cur = conn.cursor()
            row = cur.execute(
                """
                SELECT auto_status
                FROM run_items
                WHERE run_id = ?
                  AND dg_case_no = ?
                  AND field_name = ?
                  AND order_value = ?
                  AND bang_ke_value = ?
                LIMIT 1
                """,
                (self.current_run_id, dg_case, field_name, order_value, bang_ke_value),
            ).fetchone()
            conn.close()
            auto_status = clean_text(row[0]) if row else ""

        if current_core == "Đúng":
            new_core = auto_status if auto_status else "Lệch"
        else:
            new_core = "Đúng"

        if new_core == auto_status:
            new_adjust_reason = ""
            is_adjusted = 0
            adjusted_sql = "NULL"
        else:
            new_adjust_reason = reason
            is_adjusted = 1
            adjusted_sql = "CURRENT_TIMESTAMP"

        if self.last_result_df is not None and not self.last_result_df.empty:
            mask = (
                (self.last_result_df["dg_case_no"] == dg_case)
                & (self.last_result_df["field_name"] == field_name)
                & (self.last_result_df["order_value"] == order_value)
                & (self.last_result_df["bang_ke_value"] == bang_ke_value)
            )
            self.last_result_df.loc[mask, "status_core"] = new_core
            self.last_result_df.loc[mask, "adjust_reason"] = new_adjust_reason

        if self.current_run_id is not None:
            conn = sqlite3.connect(DB_FILE)
            cur = conn.cursor()
            cur.execute(
                f"""
                UPDATE run_items
                SET status = ?, adjust_reason = ?, is_adjusted = ?, adjusted_at = {adjusted_sql}
                WHERE run_id = ?
                  AND dg_case_no = ?
                  AND field_name = ?
                  AND order_value = ?
                  AND bang_ke_value = ?
                """,
                (
                    new_core,
                    new_adjust_reason,
                    is_adjusted,
                    self.current_run_id,
                    dg_case,
                    field_name,
                    order_value,
                    bang_ke_value,
                ),
            )
            conn.commit()
            conn.close()

        self._sync_views_after_toggle(dg_case)
        return format_status_display(new_core, new_adjust_reason)


def main() -> None:
    root = tk.Tk()
    style = ttk.Style(root)
    try:
        style.theme_use("clam")
    except tk.TclError:
        pass
    app = OrderlistCheckerApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
