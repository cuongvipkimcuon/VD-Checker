import json
import math
import sqlite3
import tkinter as tk
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from tkinter import filedialog, messagebox, simpledialog, ttk

import pandas as pd


CONFIG_FILE = "quotation_support_config.json"
DB_FILE = "quotation_support.db"


def norm_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() == "nan":
        return ""
    return text


def to_float(value: object, default: float = 0.0) -> float:
    text = norm_text(value).replace(",", ".")
    if not text:
        return default
    try:
        return float(text)
    except ValueError:
        return default


def normalize_code(value: object) -> str:
    text = norm_text(value)
    return text.replace(" ", ".") if text else ""


def format_money(value: float) -> str:
    return f"{value:,.0f}"


def compute_file_hash(file_path: str) -> str:
    p = Path(file_path)
    st = p.stat()
    return f"stat:{st.st_size}:{st.st_mtime_ns}"


def default_carton_price(length: float, width: float, height: float, base_price: float) -> float:
    perimeter = (length + width) * 2
    if perimeter < 190:
        raw = ((perimeter + 5) * (width + height + 5) / 10000) * base_price
    else:
        raw = ((perimeter + 10) * (width + height + 5) / 10000) * base_price
    return round(raw, -2) * 1.1


@dataclass
class PricingSettings:
    inbound_file: str = ""
    stock_file: str = ""
    fx_usd_vnd: float = 26000.0
    processing_cost_vnd: float = 0.0
    profit_rate: float = 0.2
    carton_length: float = 0.0
    carton_width: float = 0.0
    carton_height: float = 0.0
    bags_per_carton: float = 0.0
    carton_base_price: float = 0.0
    carton_formula: str = ""


class QuotationSupportApp:
    def __init__(self, root: tk.Tk, back_to_launcher: callable | None = None):
        self.root = root
        self.back_to_launcher = back_to_launcher
        self.root.title("Quotation Support")
        self.root.geometry("1500x860")

        self.settings = self._load_settings()
        self.bom_path_var = tk.StringVar()
        self.status_var = tk.StringVar(value="San sang.")
        self.summary_var = tk.StringVar(value="")
        self.kpi_cost_var = tk.StringVar(value="Cost Price: -")
        self.kpi_selling_var = tk.StringVar(value="Selling Price: -")
        self.kpi_margin_var = tk.StringVar(value="Margin: -")
        self.kpi_warning_var = tk.StringVar(value="")

        self.df_bom_raw: pd.DataFrame | None = None
        self.df_step1: pd.DataFrame = pd.DataFrame()
        self.df_step2: pd.DataFrame = pd.DataFrame()
        self.df_inbound: pd.DataFrame | None = None
        self.df_stock: pd.DataFrame | None = None

        self._build_ui()
        self._init_db()
        self._refresh_settings_labels()

    def _init_db(self) -> None:
        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS fixed_file_cache (
                file_kind TEXT PRIMARY KEY,
                file_path TEXT NOT NULL,
                file_hash TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.commit()
        conn.close()

    def _load_fixed_file_cached(self, file_kind: str, file_path: str) -> tuple[pd.DataFrame, bool]:
        file_hash = compute_file_hash(file_path)
        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()
        row = cur.execute(
            "SELECT file_hash, payload_json FROM fixed_file_cache WHERE file_kind = ? AND file_path = ?",
            (file_kind, file_path),
        ).fetchone()
        if row and str(row[0]) == file_hash:
            conn.close()
            return pd.read_json(row[1], orient="split"), True
        df = pd.read_excel(file_path, sheet_name=0, header=None)
        payload = df.to_json(orient="split", force_ascii=False)
        now = datetime.now().isoformat(timespec="seconds")
        cur.execute(
            """
            INSERT INTO fixed_file_cache(file_kind, file_path, file_hash, payload_json, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(file_kind) DO UPDATE SET
                file_path = excluded.file_path,
                file_hash = excluded.file_hash,
                payload_json = excluded.payload_json,
                updated_at = excluded.updated_at
            """,
            (file_kind, file_path, file_hash, payload, now),
        )
        conn.commit()
        conn.close()
        return df, False

    def _load_settings(self) -> PricingSettings:
        cfg = Path(CONFIG_FILE)
        if not cfg.exists():
            return PricingSettings()
        try:
            data = json.loads(cfg.read_text(encoding="utf-8"))
            return PricingSettings(**{k: data.get(k, v) for k, v in asdict(PricingSettings()).items()})
        except Exception:
            return PricingSettings()

    def _save_settings(self) -> None:
        Path(CONFIG_FILE).write_text(json.dumps(asdict(self.settings), ensure_ascii=False, indent=2), encoding="utf-8")

    def _build_ui(self) -> None:
        top = ttk.Frame(self.root, padding=10)
        top.pack(fill="x")
        ttk.Label(top, text="File BOM quotation:").pack(side="left")
        ttk.Entry(top, textvariable=self.bom_path_var, width=70).pack(side="left", padx=6)
        ttk.Button(top, text="Chon BOM", command=self._choose_bom).pack(side="left")
        ttk.Button(top, text="Setup du lieu co dinh", command=self._setup_fixed_files).pack(side="left", padx=8)
        ttk.Button(top, text="Setup carton + chi phi", command=self._open_cost_setup).pack(side="left")
        if self.back_to_launcher is not None:
            ttk.Button(top, text="Back ve Launcher", command=self._go_back).pack(side="right")

        step_bar = ttk.Frame(self.root, padding=(10, 0, 10, 6))
        step_bar.pack(fill="x")
        self.btn_step1 = tk.Button(
            step_bar,
            text="1) Doc Dinh Muc",
            command=self._run_step1,
            bg="#6c5ce7",
            fg="white",
            activebackground="#5a4bcf",
            activeforeground="white",
            relief="raised",
            bd=1,
            padx=14,
            pady=6,
        )
        self.btn_step1.pack(side="left", padx=(0, 8))
        self.btn_step2 = tk.Button(
            step_bar,
            text="2) Tinh Don Gia",
            command=self._run_step2,
            bg="#00b894",
            fg="white",
            activebackground="#019875",
            activeforeground="white",
            relief="raised",
            bd=1,
            padx=14,
            pady=6,
        )
        self.btn_step2.pack(side="left", padx=(0, 8))
        self.btn_step3 = tk.Button(
            step_bar,
            text="3) Tinh Bao Gia",
            command=self._run_step3,
            bg="#fd9644",
            fg="white",
            activebackground="#e67e22",
            activeforeground="white",
            relief="raised",
            bd=1,
            padx=14,
            pady=6,
        )
        self.btn_step3.pack(side="left")

        ttk.Label(self.root, textvariable=self.status_var, foreground="#1f4e79", padding=(10, 0, 10, 6)).pack(fill="x")

        self.tabs = ttk.Notebook(self.root)
        self.tabs.pack(fill="both", expand=True)
        self.tab_step1 = ttk.Frame(self.tabs, padding=8)
        self.tab_step2 = ttk.Frame(self.tabs, padding=8)
        self.tab_step3 = ttk.Frame(self.tabs, padding=8)
        self.tabs.add(self.tab_step1, text="Buoc 1 - Chuan hoa BOM")
        self.tabs.add(self.tab_step2, text="Buoc 2 - Tinh don gia")
        self.tabs.add(self.tab_step3, text="Buoc 3 - Tong hop bao gia")

        self._build_step1_tab()
        self._build_step2_tab()
        self._build_step3_tab()
        self._highlight_step_button(1)

    def _highlight_step_button(self, step: int) -> None:
        buttons = {
            1: self.btn_step1,
            2: self.btn_step2,
            3: self.btn_step3,
        }
        for idx, btn in buttons.items():
            if idx == step:
                btn.configure(relief="sunken", bd=3)
            else:
                btn.configure(relief="raised", bd=1)

    def _build_step1_tab(self) -> None:
        actions = ttk.Frame(self.tab_step1)
        actions.pack(fill="x", pady=(0, 6))
        ttk.Label(actions, text="Double-click de sua dong | Insert: them dong | Delete: xoa dong chon.").pack(side="left")

        cols = ("ma_npl", "ten_npl", "sldm", "qty", "dvt")
        self.tree_step1 = ttk.Treeview(self.tab_step1, columns=cols, show="headings", selectmode="browse")
        for c, w in [("ma_npl", 180), ("ten_npl", 460), ("sldm", 120), ("qty", 110), ("dvt", 120)]:
            self.tree_step1.heading(c, text=c)
            self.tree_step1.column(c, width=w, anchor="center")
        self.tree_step1.tag_configure("row_missing", background="#ffcdd2", foreground="#b71c1c")
        self.tree_step1.pack(fill="both", expand=True)
        self.tree_step1.bind("<Double-1>", self._on_step1_double_click)
        self.tree_step1.bind("<Insert>", lambda _e: self._step1_add_row())
        self.tree_step1.bind("<Delete>", lambda _e: self._step1_delete_row())

    def _on_step1_double_click(self, event: tk.Event) -> None:
        row_id = self.tree_step1.identify_row(event.y)
        if not row_id:
            return
        self.tree_step1.selection_set(row_id)
        self._step1_edit_row()

    def _build_step2_tab(self) -> None:
        actions = ttk.Frame(self.tab_step2)
        actions.pack(fill="x", pady=(0, 6))
        ttk.Label(actions, text="Double-click vao dong de nhap tay don gia (USD/VND).").pack(side="left")
        cols = (
            "ma_npl",
            "ten_npl",
            "sldm",
            "qty",
            "dvt",
            "gia_nguon_don",
            "gia_nguon_ton",
            "don_gia_don",
            "don_gia_ton",
        )
        self.tree_step2 = ttk.Treeview(self.tab_step2, columns=cols, show="headings", selectmode="browse")
        widths = [140, 280, 80, 70, 70, 140, 140, 150, 150]
        for c, w in zip(cols, widths):
            self.tree_step2.heading(c, text=c)
            self.tree_step2.column(c, width=w, anchor="center")
        self.tree_step2.tag_configure("row_missing", background="#ffcdd2", foreground="#b71c1c")
        self.tree_step2.pack(fill="both", expand=True)
        self.tree_step2.bind("<Double-1>", self._on_step2_double_click)

    def _on_step2_double_click(self, event: tk.Event) -> None:
        row_id = self.tree_step2.identify_row(event.y)
        if not row_id:
            return
        self.tree_step2.selection_set(row_id)
        self._manual_price_popup()

    def _build_step3_tab(self) -> None:
        kpi_wrap = ttk.LabelFrame(self.tab_step3, text="Tong quan bao gia")
        kpi_wrap.pack(fill="x", pady=(0, 8))
        ttk.Label(kpi_wrap, textvariable=self.kpi_cost_var, font=("Segoe UI", 11, "bold")).grid(
            row=0, column=0, sticky="w", padx=10, pady=(8, 4)
        )
        ttk.Label(kpi_wrap, textvariable=self.kpi_selling_var, font=("Segoe UI", 11, "bold")).grid(
            row=0, column=1, sticky="w", padx=10, pady=(8, 4)
        )
        ttk.Label(kpi_wrap, textvariable=self.kpi_margin_var).grid(
            row=1, column=0, sticky="w", padx=10, pady=(0, 8)
        )
        ttk.Label(kpi_wrap, textvariable=self.kpi_warning_var, foreground="#b71c1c").grid(
            row=1, column=1, sticky="w", padx=10, pady=(0, 8)
        )
        kpi_wrap.columnconfigure(0, weight=1)
        kpi_wrap.columnconfigure(1, weight=1)

        box = ttk.LabelFrame(self.tab_step3, text="Chi tiet thanh phan gia")
        box.pack(fill="x", pady=(0, 8))
        ttk.Label(box, textvariable=self.summary_var, justify="left").pack(anchor="w", padx=10, pady=8)

        cols = ("hang_muc", "gia_tri_vnd", "gia_tri_usd", "ty_trong", "ghi_chu")
        self.tree_summary = ttk.Treeview(self.tab_step3, columns=cols, show="headings")
        for c, w in [
            ("hang_muc", 260),
            ("gia_tri_vnd", 200),
            ("gia_tri_usd", 170),
            ("ty_trong", 100),
            ("ghi_chu", 600),
        ]:
            self.tree_summary.heading(c, text=c)
            anchor = "e" if c in {"gia_tri_vnd", "gia_tri_usd", "ty_trong"} else "w"
            self.tree_summary.column(c, width=w, anchor=anchor)
        self.tree_summary.tag_configure("row_missing", background="#ffcdd2", foreground="#b71c1c")
        self.tree_summary.tag_configure("row_total", background="#e3f2fd", foreground="#0d47a1")
        self.tree_summary.tag_configure("row_profit", background="#e8f5e9", foreground="#1b5e20")
        self.tree_summary.pack(fill="both", expand=True)

    def _refresh_settings_labels(self) -> None:
        self.status_var.set(
            "File co dinh: "
            f"Nhap kho={Path(self.settings.inbound_file).name if self.settings.inbound_file else '(chua setup)'} | "
            f"Ton kho={Path(self.settings.stock_file).name if self.settings.stock_file else '(chua setup)'}"
        )

    def _choose_bom(self) -> None:
        path = filedialog.askopenfilename(
            title="Chon file quotation BOM",
            filetypes=[("Excel files", "*.xlsx *.xls"), ("All files", "*.*")],
        )
        if path:
            self.bom_path_var.set(path)

    def _setup_fixed_files(self) -> None:
        inbound = filedialog.askopenfilename(
            title="Chon file BANG KE NHAP KHO THEO DON HANG",
            filetypes=[("Excel files", "*.xlsx *.xls"), ("All files", "*.*")],
        )
        if not inbound:
            return
        stock = filedialog.askopenfilename(
            title="Chon file TONG HOP NHAP XUAT TON",
            filetypes=[("Excel files", "*.xlsx *.xls"), ("All files", "*.*")],
        )
        if not stock:
            return
        self.settings.inbound_file = inbound
        self.settings.stock_file = stock
        self._save_settings()
        self._refresh_settings_labels()

    def _run_step1(self) -> None:
        bom_path = self.bom_path_var.get().strip()
        if not bom_path:
            messagebox.showwarning("Quotation", "Hay chon file BOM quotation.")
            return
        if not Path(bom_path).exists():
            messagebox.showerror("Quotation", "Khong tim thay file BOM.")
            return
        if not self.settings.stock_file or not Path(self.settings.stock_file).exists():
            messagebox.showwarning("Quotation", "Can setup file Tong Hop ton kho truoc.")
            return
        try:
            self.df_bom_raw = pd.read_excel(bom_path, sheet_name=0, header=None)
            stock_df, stock_cached = self._load_fixed_file_cached("stock", self.settings.stock_file)
            self.df_stock = stock_df

            b = self.df_bom_raw.iloc[:, 1].map(norm_text) if self.df_bom_raw.shape[1] > 1 else pd.Series([""] * len(self.df_bom_raw))
            i = self.df_bom_raw.iloc[:, 8].map(norm_text) if self.df_bom_raw.shape[1] > 8 else pd.Series([""] * len(self.df_bom_raw))
            j = self.df_bom_raw.iloc[:, 9].map(norm_text) if self.df_bom_raw.shape[1] > 9 else pd.Series([""] * len(self.df_bom_raw))
            valid_rows = self.df_bom_raw[(b != "") & (i != "") & (j != "")]

            stock_map_name = {}
            for _, row in stock_df.iterrows():
                code = normalize_code(row.iloc[0] if len(row) > 0 else "")
                name = norm_text(row.iloc[1] if len(row) > 1 else "")
                if code and name and code not in stock_map_name:
                    stock_map_name[code] = name

            rows = []
            for idx, row in valid_rows.iterrows():
                code = normalize_code(row.iloc[0] if len(row) > 0 else "")
                ma_npl = code if code else "MISSING"
                ten_npl = ""
                col_b_text = norm_text(row.iloc[1] if len(row) > 1 else "")
                if code and code in stock_map_name:
                    ten_npl = stock_map_name[code]
                if not ten_npl:
                    if "consumption" in col_b_text.lower():
                        ten_npl = self._find_nearest_name_above(idx)
                    else:
                        ten_npl = col_b_text
                sldm = to_float(row.iloc[8], 0.0)
                rows.append(
                    {
                        "ma_npl": ma_npl,
                        "ten_npl": ten_npl,
                        "sldm": sldm,
                        "qty": self._derive_order_qty(row),
                        "dvt": norm_text(row.iloc[9]),
                    }
                )

            self.df_step1 = pd.DataFrame(rows)
            self._render_step1()
            self._auto_detect_carton()
            source_note = "cache" if stock_cached else "read file"
            self.status_var.set(f"Buoc 1 xong: doc {len(self.df_step1)} dong hop le. Ton kho: {source_note}.")
            self.tabs.select(self.tab_step1)
            self._highlight_step_button(1)
        except Exception as exc:
            messagebox.showerror("Quotation", f"Loi doc Step 1: {exc}")

    def _derive_order_qty(self, row: pd.Series) -> float:
        i_val = to_float(row.iloc[8] if len(row) > 8 else 0, 0)
        h_val = to_float(row.iloc[7] if len(row) > 7 else 0, 0)
        if i_val > 0 and h_val > 0:
            return float(int(round(i_val / h_val)))
        return i_val

    def _find_nearest_name_above(self, idx: int) -> str:
        if self.df_bom_raw is None or self.df_bom_raw.shape[1] <= 7:
            return ""
        for r in range(idx, -1, -1):
            val = norm_text(self.df_bom_raw.iat[r, 7])
            if val and not val.replace(".", "").isdigit():
                return val
        return ""

    def _auto_detect_carton(self) -> None:
        if self.df_bom_raw is None:
            return
        for _, row in self.df_bom_raw.iterrows():
            text = norm_text(row.iloc[0] if len(row) > 0 else "").lower()
            if "carton size" in text:
                self.settings.carton_length = to_float(row.iloc[2] if len(row) > 2 else 0)
                self.settings.carton_width = to_float(row.iloc[3] if len(row) > 3 else 0)
                self.settings.carton_height = to_float(row.iloc[4] if len(row) > 4 else 0)
                g = to_float(row.iloc[6] if len(row) > 6 else 0)
                self.settings.bags_per_carton = (1 / g) if g else 0.0
                self.settings.carton_base_price = to_float(row.iloc[14] if len(row) > 14 else 0)
                self._save_settings()
                return

    def _render_step1(self) -> None:
        for item in self.tree_step1.get_children():
            self.tree_step1.delete(item)
        for idx, row in self.df_step1.iterrows():
            missing = (
                norm_text(row.get("ma_npl", "")) in {"", "MISSING"}
                or norm_text(row.get("ten_npl", "")) == ""
                or to_float(row.get("sldm", 0), 0) <= 0
                or to_float(row.get("qty", 0), 0) <= 0
                or norm_text(row.get("dvt", "")) == ""
            )
            self.tree_step1.insert(
                "",
                "end",
                iid=str(idx),
                values=(row["ma_npl"], row["ten_npl"], row["sldm"], row.get("qty", 0), row["dvt"]),
                tags=("row_missing",) if missing else (),
            )

    def _step1_add_row(self) -> None:
        data = self._step1_row_dialog()
        if not data:
            return
        self.df_step1 = pd.concat([self.df_step1, pd.DataFrame([data])], ignore_index=True)
        self._render_step1()

    def _step1_edit_row(self) -> None:
        sel = self.tree_step1.selection()
        if not sel:
            messagebox.showwarning("Step 1", "Chon dong can sua.")
            return
        idx = int(sel[0])
        cur = self.df_step1.loc[idx].to_dict()
        data = self._step1_row_dialog(cur)
        if not data:
            return
        for k, v in data.items():
            self.df_step1.at[idx, k] = v
        self._render_step1()

    def _step1_delete_row(self) -> None:
        sel = self.tree_step1.selection()
        if not sel:
            return
        idx = int(sel[0])
        self.df_step1 = self.df_step1.drop(index=idx).reset_index(drop=True)
        self._render_step1()

    def _step1_row_dialog(self, initial: dict | None = None) -> dict | None:
        dlg = tk.Toplevel(self.root)
        dlg.title("Dong BOM")
        dlg.geometry("520x240")
        dlg.transient(self.root)
        dlg.grab_set()

        ma_var = tk.StringVar(value=(initial or {}).get("ma_npl", ""))
        ten_var = tk.StringVar(value=(initial or {}).get("ten_npl", ""))
        sldm_var = tk.StringVar(value=str((initial or {}).get("sldm", "")))
        qty_var = tk.StringVar(value=str((initial or {}).get("qty", "")))
        dvt_var = tk.StringVar(value=(initial or {}).get("dvt", ""))
        ttk.Label(dlg, text="Ma NPL").grid(row=0, column=0, sticky="w", padx=10, pady=8)
        ttk.Entry(dlg, textvariable=ma_var, width=44).grid(row=0, column=1, padx=10, pady=8)
        ttk.Label(dlg, text="Ten NPL").grid(row=1, column=0, sticky="w", padx=10, pady=8)
        ttk.Entry(dlg, textvariable=ten_var, width=44).grid(row=1, column=1, padx=10, pady=8)
        ttk.Label(dlg, text="SLDM").grid(row=2, column=0, sticky="w", padx=10, pady=8)
        ttk.Entry(dlg, textvariable=sldm_var, width=44).grid(row=2, column=1, padx=10, pady=8)
        ttk.Label(dlg, text="Qty").grid(row=3, column=0, sticky="w", padx=10, pady=8)
        ttk.Entry(dlg, textvariable=qty_var, width=44).grid(row=3, column=1, padx=10, pady=8)
        ttk.Label(dlg, text="DVT").grid(row=4, column=0, sticky="w", padx=10, pady=8)
        ttk.Entry(dlg, textvariable=dvt_var, width=44).grid(row=4, column=1, padx=10, pady=8)

        result = {"value": None}

        def save() -> None:
            result["value"] = {
                "ma_npl": norm_text(ma_var.get()) or "MISSING",
                "ten_npl": norm_text(ten_var.get()),
                "sldm": to_float(sldm_var.get(), 0.0),
                "qty": max(1.0, float(int(round(to_float(qty_var.get(), 0.0))))) if to_float(qty_var.get(), 0.0) > 0 else 0.0,
                "dvt": norm_text(dvt_var.get()),
            }
            dlg.destroy()

        ttk.Button(dlg, text="Luu", command=save).grid(row=5, column=1, sticky="e", padx=10, pady=12)
        ttk.Button(dlg, text="Huy", command=dlg.destroy).grid(row=5, column=1, sticky="w", padx=10, pady=12)
        self.root.wait_window(dlg)
        return result["value"]

    def _run_step2(self) -> None:
        if self.df_step1.empty:
            messagebox.showwarning("Step 2", "Chua co du lieu step 1.")
            return
        if not self.settings.inbound_file or not Path(self.settings.inbound_file).exists():
            messagebox.showwarning("Step 2", "Can setup file Bang ke nhap kho.")
            return
        if not self.settings.stock_file or not Path(self.settings.stock_file).exists():
            messagebox.showwarning("Step 2", "Can setup file Tong hop ton kho.")
            return
        try:
            self.df_inbound, inbound_cached = self._load_fixed_file_cached("inbound", self.settings.inbound_file)
            self.df_stock, stock_cached = self._load_fixed_file_cached("stock", self.settings.stock_file)
            out = self.df_step1.copy()
            out["don_gia_don"] = 0.0
            out["don_gia_don_note"] = ""
            out["don_gia_ton"] = 0.0
            out["gia_nguon_don"] = 0.0
            out["gia_nguon_ton"] = 0.0
            out["is_new_npl"] = False

            stock_price_map = {}
            for _, row in self.df_stock.iterrows():
                code = normalize_code(row.iloc[0] if len(row) > 0 else "")
                price = to_float(row.iloc[8] if len(row) > 8 else 0)
                if code and code not in stock_price_map:
                    stock_price_map[code] = price

            now = pd.Timestamp(datetime.now())
            for idx, row in out.iterrows():
                code = normalize_code(row["ma_npl"])
                dvt = norm_text(row["dvt"]).lower()
                qty = to_float(row.get("qty", 0), 0)
                qty_pricing = qty if qty > 0 else 1.0

                inbound_matches = self._find_inbound_matches(code, dvt)
                selected_price_total = 0.0
                selected_price_per_unit = 0.0
                note = ""
                if inbound_matches:
                    chosen = min(inbound_matches, key=lambda x: abs((x["date"] - now).days) if pd.notna(x["date"]) else 999999)
                    selected_price_total = chosen["price"]
                    selected_price_per_unit = selected_price_total / qty_pricing
                    all_prices = [m["price"] for m in inbound_matches if m["price"] > 0]
                    if len(all_prices) > 1 and selected_price_per_unit > 0:
                        avg_per_unit = (sum(all_prices) / len(all_prices)) / qty_pricing
                        diff_pct = ((selected_price_per_unit / avg_per_unit) - 1) * 100 if avg_per_unit else 0
                        sign = "+" if diff_pct >= 0 else ""
                        note = f"{selected_price_per_unit:,.0f} ({sign}{diff_pct:.1f}%)"
                    else:
                        note = f"{selected_price_per_unit:,.0f}"
                stock_total = stock_price_map.get(code, 0.0)
                stock_price = stock_total / qty_pricing
                is_new_npl = (selected_price_per_unit <= 0) and (stock_price <= 0)
                if is_new_npl:
                    # NPL moi: don gia don/ton luon dong bo.
                    stock_price = selected_price_per_unit
                out.at[idx, "gia_nguon_don"] = selected_price_total
                out.at[idx, "gia_nguon_ton"] = stock_total
                out.at[idx, "don_gia_don"] = selected_price_per_unit
                out.at[idx, "don_gia_don_note"] = note
                out.at[idx, "don_gia_ton"] = stock_price
                out.at[idx, "is_new_npl"] = is_new_npl

            self.df_step2 = out
            self._render_step2()
            self.status_var.set(
                "Buoc 2 xong: da tinh gia nguon va don gia / cai. "
                f"Nhap kho={'cache' if inbound_cached else 'read file'} | Ton kho={'cache' if stock_cached else 'read file'}."
            )
            self.tabs.select(self.tab_step2)
            self._highlight_step_button(2)
        except Exception as exc:
            messagebox.showerror("Step 2", f"Loi tinh toan: {exc}")

    def _find_inbound_matches(self, code: str, dvt_lower: str) -> list[dict]:
        if self.df_inbound is None:
            return []
        matches: list[dict] = []
        for _, row in self.df_inbound.iterrows():
            row_code = normalize_code(row.iloc[0] if len(row) > 0 else "")
            if row_code != code:
                continue
            row_dvt = norm_text(row.iloc[6] if len(row) > 6 else "").lower()
            if dvt_lower and row_dvt and dvt_lower != row_dvt:
                continue
            price = to_float(row.iloc[9] if len(row) > 9 else 0)
            if price <= 0:
                continue
            date = pd.to_datetime(row.iloc[1] if len(row) > 1 else None, errors="coerce", dayfirst=True)
            matches.append({"price": price, "date": date})
        return matches

    def _render_step2(self) -> None:
        for item in self.tree_step2.get_children():
            self.tree_step2.delete(item)
        for idx, row in self.df_step2.iterrows():
            don_text = row["don_gia_don_note"] if norm_text(row["don_gia_don_note"]) else format_money(to_float(row["don_gia_don"]))
            missing = (
                norm_text(row.get("ma_npl", "")) in {"", "MISSING"}
                or norm_text(row.get("ten_npl", "")) == ""
                or to_float(row.get("sldm", 0), 0) <= 0
                or to_float(row.get("qty", 0), 0) <= 0
                or norm_text(row.get("dvt", "")) == ""
                or (to_float(row.get("don_gia_don", 0), 0) <= 0 and to_float(row.get("don_gia_ton", 0), 0) <= 0)
            )
            self.tree_step2.insert(
                "",
                "end",
                iid=str(idx),
                values=(
                    row["ma_npl"],
                    row["ten_npl"],
                    f"{to_float(row['sldm']):,.4f}",
                    f"{to_float(row.get('qty', 0)):,.0f}",
                    row["dvt"],
                    format_money(to_float(row.get("gia_nguon_don", 0))),
                    format_money(to_float(row.get("gia_nguon_ton", 0))),
                    don_text,
                    format_money(to_float(row["don_gia_ton"])),
                ),
                tags=("row_missing",) if missing else (),
            )

    def _manual_price_popup(self) -> None:
        if self.df_step2.empty:
            messagebox.showwarning("Step 2", "Chua co bang step 2.")
            return
        sel = self.tree_step2.selection()
        if not sel:
            messagebox.showwarning("Step 2", "Chon 1 dong de nhap tay don gia.")
            return
        idx = int(sel[0])
        dlg = tk.Toplevel(self.root)
        dlg.title("Nhap don gia thu cong")
        dlg.geometry("420x170")
        dlg.transient(self.root)
        dlg.grab_set()

        price_var = tk.StringVar(value="")
        currency_var = tk.StringVar(value="VND")
        ttk.Label(dlg, text="Don gia:").grid(row=0, column=0, sticky="w", padx=10, pady=10)
        ttk.Entry(dlg, textvariable=price_var, width=26).grid(row=0, column=1, sticky="w", padx=10, pady=10)
        ttk.Label(dlg, text="Tien te:").grid(row=1, column=0, sticky="w", padx=10, pady=8)
        ccy_combo = ttk.Combobox(dlg, textvariable=currency_var, state="readonly", values=("USD", "VND"), width=23)
        ccy_combo.grid(row=1, column=1, sticky="w", padx=10, pady=8)
        ccy_combo.current(1)

        result = {"ok": False}

        def confirm() -> None:
            val = to_float(price_var.get(), -1)
            if val < 0:
                messagebox.showwarning("Step 2", "Don gia khong hop le.")
                return
            result["ok"] = True
            dlg.destroy()

        ttk.Button(dlg, text="Luu", command=confirm).grid(row=2, column=1, sticky="e", padx=10, pady=10)
        ttk.Button(dlg, text="Huy", command=dlg.destroy).grid(row=2, column=1, sticky="w", padx=10, pady=10)
        self.root.wait_window(dlg)
        if not result["ok"]:
            return

        val = to_float(price_var.get(), 0.0)
        ccy = currency_var.get().strip().upper()
        value_vnd = val * self.settings.fx_usd_vnd if ccy == "USD" else val
        # Nhap tay 1 lan: dong bo gia don/ton cho dong duoc chon.
        self.df_step2.at[idx, "don_gia_don"] = value_vnd
        self.df_step2.at[idx, "don_gia_ton"] = value_vnd
        self.df_step2.at[idx, "is_new_npl"] = True
        self.df_step2.at[idx, "gia_nguon_don"] = value_vnd
        self.df_step2.at[idx, "gia_nguon_ton"] = value_vnd
        self.df_step2.at[idx, "don_gia_don_note"] = f"{value_vnd:,.0f} (manual {ccy})"
        self._render_step2()

    def _open_cost_setup(self) -> None:
        dlg = tk.Toplevel(self.root)
        dlg.title("Setup Carton + Processing + Ty gia")
        dlg.geometry("620x470")
        dlg.transient(self.root)
        dlg.grab_set()

        vars_map = {
            "fx_usd_vnd": tk.StringVar(value=str(self.settings.fx_usd_vnd)),
            "processing_cost_vnd": tk.StringVar(value=str(self.settings.processing_cost_vnd)),
            "profit_rate": tk.StringVar(value=str(self.settings.profit_rate)),
            "carton_length": tk.StringVar(value=str(self.settings.carton_length)),
            "carton_width": tk.StringVar(value=str(self.settings.carton_width)),
            "carton_height": tk.StringVar(value=str(self.settings.carton_height)),
            "bags_per_carton": tk.StringVar(value=str(self.settings.bags_per_carton)),
            "carton_base_price": tk.StringVar(value=str(self.settings.carton_base_price)),
            "carton_formula": tk.StringVar(value=self.settings.carton_formula),
        }
        labels = [
            ("Ty gia USD/VND", "fx_usd_vnd"),
            ("Gia processing (VND)", "processing_cost_vnd"),
            ("Loi nhuan (0.2 = 20%)", "profit_rate"),
            ("Dai carton", "carton_length"),
            ("Rong carton", "carton_width"),
            ("Cao carton", "carton_height"),
            ("So tui / thung", "bags_per_carton"),
            ("Gia co so carton", "carton_base_price"),
            ("Cong thuc carton (optional)", "carton_formula"),
        ]
        for i, (lb, key) in enumerate(labels):
            ttk.Label(dlg, text=lb).grid(row=i, column=0, sticky="w", padx=10, pady=6)
            ttk.Entry(dlg, textvariable=vars_map[key], width=58).grid(row=i, column=1, sticky="ew", padx=10, pady=6)
        ttk.Label(
            dlg,
            text="Cong thuc tuy chinh dung bien L, W, H, BASE. Bo trong de dung cong thuc mac dinh.",
            foreground="#555555",
        ).grid(row=len(labels), column=0, columnspan=2, sticky="w", padx=10, pady=(2, 8))

        def save() -> None:
            self.settings.fx_usd_vnd = to_float(vars_map["fx_usd_vnd"].get(), self.settings.fx_usd_vnd)
            self.settings.processing_cost_vnd = to_float(vars_map["processing_cost_vnd"].get(), 0)
            self.settings.profit_rate = to_float(vars_map["profit_rate"].get(), 0.2)
            self.settings.carton_length = to_float(vars_map["carton_length"].get(), 0)
            self.settings.carton_width = to_float(vars_map["carton_width"].get(), 0)
            self.settings.carton_height = to_float(vars_map["carton_height"].get(), 0)
            self.settings.bags_per_carton = to_float(vars_map["bags_per_carton"].get(), 0)
            self.settings.carton_base_price = to_float(vars_map["carton_base_price"].get(), 0)
            self.settings.carton_formula = vars_map["carton_formula"].get().strip()
            self._save_settings()
            dlg.destroy()

        ttk.Button(dlg, text="Luu", command=save).grid(row=len(labels) + 1, column=1, sticky="e", padx=10, pady=10)
        ttk.Button(dlg, text="Dong", command=dlg.destroy).grid(row=len(labels) + 1, column=1, sticky="w", padx=10, pady=10)
        dlg.columnconfigure(1, weight=1)

    def _calc_carton_cost(self) -> float:
        l = self.settings.carton_length
        w = self.settings.carton_width
        h = self.settings.carton_height
        base = self.settings.carton_base_price
        if self.settings.carton_formula:
            safe_vars = {"L": l, "W": w, "H": h, "BASE": base, "ROUND": round, "IF": lambda c, a, b: a if c else b}
            try:
                val = eval(self.settings.carton_formula, {"__builtins__": {}}, safe_vars)
                return float(val)
            except Exception:
                return default_carton_price(l, w, h, base)
        return default_carton_price(l, w, h, base)

    def _run_step3(self) -> None:
        if self.df_step2.empty:
            messagebox.showwarning("Step 3", "Can tinh step 2 truoc.")
            return
        material_cost_order = float(self.df_step2["don_gia_don"].sum())
        material_cost_stock = float(self.df_step2["don_gia_ton"].sum())
        processing = self.settings.processing_cost_vnd
        carton = self._calc_carton_cost()
        cost_order = material_cost_order + processing + carton
        cost_stock = material_cost_stock + processing + carton

        margin = self.settings.profit_rate
        if margin >= 1:
            messagebox.showwarning("Step 3", "Loi nhuan phai < 1.")
            return
        selling_order = cost_order / (1 - margin) if margin < 1 else 0
        delta_pct = ((selling_order - cost_order) / cost_order * 100) if cost_order else 0
        warn = "CANH BAO: chenh lech cost/selling lon." if delta_pct > 300 else ""
        fx = self.settings.fx_usd_vnd or 1

        for item in self.tree_summary.get_children():
            self.tree_summary.delete(item)

        rows = [
            ("Gia NPL / cai (Don)", material_cost_order, material_cost_order / fx, "Tong don gia 1 cai theo nguon don"),
            ("Gia NPL / cai (Ton)", material_cost_stock, material_cost_stock / fx, "Tong don gia 1 cai theo nguon ton"),
            ("Processing / cai", processing, processing / fx, "Gia processing setup"),
            ("Carton / cai", carton, carton / fx, "Tinh theo carton setup"),
            ("Cost Price / cai (Don)", cost_order, cost_order / fx, "NPL/cai + processing/cai + carton/cai"),
            ("Cost Price / cai (Ton)", cost_stock, cost_stock / fx, "NPL/cai + processing/cai + carton/cai"),
            ("Selling Price / cai", selling_order, selling_order / fx, f"Cost/(1-loi_nhuan) | {warn}".strip()),
        ]
        for r in rows:
            is_missing = r[1] <= 0 and r[0] not in {"Processing / cai", "Carton / cai"}
            if r[0].startswith("Cost Price"):
                row_tag = "row_total"
            elif r[0] == "Selling Price / cai":
                row_tag = "row_profit"
            elif is_missing:
                row_tag = "row_missing"
            else:
                row_tag = ""
            ratio = (r[1] / cost_order * 100) if cost_order > 0 else 0.0
            self.tree_summary.insert(
                "",
                "end",
                values=(r[0], format_money(r[1]), f"{r[2]:,.2f}", f"{ratio:.1f}%", r[3]),
                tags=(row_tag,) if row_tag else (),
            )

        self.kpi_cost_var.set(f"Cost Price / cai: {format_money(cost_order)} VND | {cost_order/fx:,.2f} USD")
        self.kpi_selling_var.set(f"Selling Price / cai: {format_money(selling_order)} VND | {selling_order/fx:,.2f} USD")
        self.kpi_margin_var.set(f"Loi nhuan setup: {margin*100:.1f}% | Delta cost->selling: {delta_pct:.1f}%")
        self.kpi_warning_var.set(warn)

        self.summary_var.set(
            f"Cost Price / cai (Don): {format_money(cost_order)} VND | {cost_order/fx:,.2f} USD\n"
            f"Selling Price / cai: {format_money(selling_order)} VND | {selling_order/fx:,.2f} USD\n"
            f"Ty le chenh cost->selling: {delta_pct:.1f}% {warn}"
        )
        self.tabs.select(self.tab_step3)
        self._highlight_step_button(3)
        self.status_var.set("Buoc 3 xong: da tong hop cost/selling va bang chi tiet.")

    def _go_back(self) -> None:
        if self.back_to_launcher is None:
            return
        self.root.destroy()
        self.back_to_launcher()


def main(back_to_launcher: callable | None = None) -> None:
    root = tk.Tk()
    QuotationSupportApp(root, back_to_launcher=back_to_launcher)
    root.mainloop()


if __name__ == "__main__":
    main()
