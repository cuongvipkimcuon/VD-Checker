import tkinter as tk
from tkinter import filedialog, messagebox, simpledialog, ttk
from pathlib import Path
import sqlite3
import unicodedata
import json
import pandas as pd
from openpyxl.styles import PatternFill


DB_FILE = "npl_checker.db"
CONFIG_FILE = "npl_checker_config.json"
RESULT_COLUMNS = [
    "so_o",
    "ma_npl",
    "ten_npl",
    "ton_thuc_te",
    "ton_dm_chua_xuat",
    "ket_luan",
]


def normalize_text(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return text.replace("đ", "d")


def find_header_row(file_path: str, must_have_keywords: list[str]) -> int:
    preview = pd.read_excel(file_path, sheet_name=0, header=None, nrows=40)
    for idx, row in preview.iterrows():
        row_text = " | ".join(
            normalize_text(cell) for cell in row.tolist() if normalize_text(cell)
        )
        if all(keyword in row_text for keyword in must_have_keywords):
            return int(idx)
    raise ValueError(f"Khong tim thay dong header phu hop trong file: {file_path}")


def find_column(df: pd.DataFrame, expected_keywords: list[str]) -> str:
    col_map = {col: normalize_text(col) for col in df.columns}
    for col, col_text in col_map.items():
        if all(keyword in col_text for keyword in expected_keywords):
            return col
    raise ValueError(f"Khong tim thay cot voi tu khoa: {expected_keywords}")


def init_db() -> None:
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_name TEXT,
            so_o TEXT NOT NULL,
            bom_file TEXT,
            stock_file TEXT,
            note TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS run_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            so_o TEXT,
            ma_npl TEXT,
            ten_npl TEXT,
            ton_thuc_te REAL,
            ton_dm_chua_xuat REAL,
            ket_luan TEXT,
            FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
        )
        """
    )
    conn.commit()
    conn.close()


def load_config() -> dict:
    config_path = Path(CONFIG_FILE)
    if not config_path.exists():
        return {}
    try:
        return json.loads(config_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_config(data: dict) -> None:
    config_path = Path(CONFIG_FILE)
    config_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


class App:
    def __init__(self, root: tk.Tk):
        init_db()
        self.root = root
        self.root.title("NPL Checker - Kiem tra ton am + database")
        self.root.geometry("1320x760")

        self.bom_file_var = tk.StringVar()
        self.stock_file_var = tk.StringVar()
        self.o_number_var = tk.StringVar()
        self.search_result_var = tk.StringVar()
        self.search_db_var = tk.StringVar()

        self.last_result_df: pd.DataFrame | None = None
        self.loaded_run_id: int | None = None
        self.config = load_config()

        self._build_ui()
        self._load_last_paths()
        self.refresh_runs()

    def _build_ui(self) -> None:
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill="both", expand=True)

        self.tab_check = ttk.Frame(self.notebook, padding=10)
        self.tab_db = ttk.Frame(self.notebook, padding=10)
        self.tab_summary = ttk.Frame(self.notebook, padding=10)
        self.notebook.add(self.tab_check, text="Fast Check")
        self.notebook.add(self.tab_db, text="Data")
        self.notebook.add(self.tab_summary, text="Super Report")

        self._build_tab_check()
        self._build_tab_db()
        self._build_tab_summary()

    def _build_tab_check(self) -> None:
        top = ttk.Frame(self.tab_check)
        top.pack(fill="x")

        ttk.Label(top, text="File bang ke dinh muc:").grid(
            row=0, column=0, sticky="w", padx=(0, 8), pady=4
        )
        ttk.Entry(top, textvariable=self.bom_file_var, width=95).grid(
            row=0, column=1, padx=(0, 8), pady=4, sticky="ew"
        )
        ttk.Button(top, text="Chon file", command=self.choose_bom_file).grid(row=0, column=2)

        ttk.Label(top, text="File tong hop nhap xuat ton:").grid(
            row=1, column=0, sticky="w", padx=(0, 8), pady=4
        )
        ttk.Entry(top, textvariable=self.stock_file_var, width=95).grid(
            row=1, column=1, padx=(0, 8), pady=4, sticky="ew"
        )
        ttk.Button(top, text="Chon file", command=self.choose_stock_file).grid(row=1, column=2)

        ttk.Label(top, text="So O can check:").grid(
            row=2, column=0, sticky="w", padx=(0, 8), pady=8
        )
        ttk.Entry(top, textvariable=self.o_number_var, width=30).grid(row=2, column=1, sticky="w")
        action_row = ttk.Frame(top)
        action_row.grid(row=2, column=2, sticky="e")
        ttk.Button(action_row, text="Kiem tra", command=self.run_check).pack(side="left")
        ttk.Button(action_row, text="Xuat Excel", command=self.export_excel).pack(side="left", padx=(6, 0))

        ttk.Label(top, text="Search ket qua:").grid(row=3, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(top, textvariable=self.search_result_var, width=40).grid(
            row=3, column=1, sticky="w", pady=(8, 0)
        )
        filter_row = ttk.Frame(top)
        filter_row.grid(row=3, column=2, sticky="e", pady=(8, 0))
        ttk.Button(filter_row, text="Loc", command=self.filter_current_results).pack(side="left")
        ttk.Button(filter_row, text="Bo loc", command=self.clear_result_filter).pack(side="left", padx=(6, 0))
        top.columnconfigure(1, weight=1)

        self.status_label = ttk.Label(
            self.tab_check, text="San sang.", foreground="#1f4e79", padding=(0, 8)
        )
        self.status_label.pack(fill="x")

        self.result_tree = self._create_result_tree(self.tab_check)

        action_wrap = ttk.Frame(self.tab_check)
        action_wrap.pack(fill="x", pady=(8, 0))
        ttk.Button(action_wrap, text="Save vao database", command=self.save_current_run).pack(side="left")
        ttk.Button(action_wrap, text="Mo tab tong hop", command=self.switch_to_summary).pack(side="left", padx=6)

    def _build_tab_db(self) -> None:
        top = ttk.Frame(self.tab_db)
        top.pack(fill="x")
        ttk.Label(top, text="Search run (so O / ten run / ghi chu):").pack(side="left")
        ttk.Entry(top, textvariable=self.search_db_var, width=45).pack(side="left", padx=6)
        ttk.Button(top, text="Search", command=self.refresh_runs).pack(side="left")
        ttk.Button(top, text="Refresh", command=self.refresh_runs).pack(side="left", padx=6)

        run_columns = ("id", "run_name", "so_o", "created_at", "note", "item_count")
        self.runs_tree = ttk.Treeview(self.tab_db, columns=run_columns, show="headings", height=12)
        for col, width in [
            ("id", 70),
            ("run_name", 220),
            ("so_o", 120),
            ("created_at", 160),
            ("note", 360),
            ("item_count", 90),
        ]:
            self.runs_tree.heading(col, text=col)
            self.runs_tree.column(col, width=width, anchor="w")
        self.runs_tree.pack(fill="x", pady=(8, 8))

        btn_wrap = ttk.Frame(self.tab_db)
        btn_wrap.pack(fill="x", pady=(0, 8))
        ttk.Button(btn_wrap, text="Load run", command=self.load_selected_run).pack(side="left")
        ttk.Button(btn_wrap, text="Update note", command=self.update_selected_run_note).pack(side="left", padx=6)
        ttk.Button(btn_wrap, text="Delete run", command=self.delete_selected_run).pack(side="left")

        ttk.Label(self.tab_db, text="Du lieu chi tiet run da chon:").pack(anchor="w")
        self.db_item_tree = self._create_result_tree(self.tab_db)

    def _build_tab_summary(self) -> None:
        top = ttk.Frame(self.tab_summary)
        top.pack(fill="x")
        ttk.Button(
            top,
            text="Tao bao cao tu run da chon",
            command=self.build_summary_report,
        ).pack(side="left")
        ttk.Button(top, text="Refresh", command=self.build_summary_report).pack(side="left", padx=6)

        run_picker = ttk.Frame(self.tab_summary)
        run_picker.pack(fill="x", pady=(8, 0))
        ttk.Label(run_picker, text="Chon run de gom (giu Ctrl/Shift de chon nhieu):").pack(
            anchor="w"
        )
        list_wrap = ttk.Frame(run_picker)
        list_wrap.pack(fill="x", pady=(4, 0))
        self.summary_runs_listbox = tk.Listbox(
            list_wrap,
            selectmode="extended",
            height=6,
            exportselection=False,
        )
        self.summary_runs_listbox.pack(side="left", fill="x", expand=True)
        summary_list_scroll = ttk.Scrollbar(
            list_wrap, orient="vertical", command=self.summary_runs_listbox.yview
        )
        self.summary_runs_listbox.configure(yscrollcommand=summary_list_scroll.set)
        summary_list_scroll.pack(side="right", fill="y")

        picker_actions = ttk.Frame(run_picker)
        picker_actions.pack(fill="x", pady=(4, 0))
        ttk.Button(
            picker_actions, text="Load danh sach run", command=self.refresh_summary_run_list
        ).pack(side="left")
        ttk.Button(
            picker_actions, text="Chon tat ca", command=self.select_all_summary_runs
        ).pack(side="left", padx=6)
        ttk.Button(
            picker_actions, text="Bo chon", command=self.clear_summary_run_selection
        ).pack(side="left")

        self.summary_label = ttk.Label(
            self.tab_summary, text="Bao cao tong hop chua duoc tao.", padding=(0, 8)
        )
        self.summary_label.pack(fill="x")

        cols = (
            "ma_npl",
            "ten_npl",
            "so_o",
            "run_id",
            "ton_thuc_te",
            "ton_dm_chua_xuat",
            "ket_luan",
        )
        self.summary_tree = ttk.Treeview(self.tab_summary, columns=cols, show="headings")
        for name, width, anchor in [
            ("ma_npl", 150, "center"),
            ("ten_npl", 420, "w"),
            ("so_o", 120, "center"),
            ("run_id", 80, "center"),
            ("ton_thuc_te", 140, "center"),
            ("ton_dm_chua_xuat", 170, "center"),
            ("ket_luan", 180, "center"),
        ]:
            self.summary_tree.heading(name, text=name)
            self.summary_tree.column(name, width=width, anchor=anchor)
        self.summary_tree.tag_configure("negative", background="#ffd9d9")
        self.summary_tree.tag_configure("normal", background="#e7f7e7")

        wrap = ttk.Frame(self.tab_summary)
        wrap.pack(fill="both", expand=True)
        y_scroll = ttk.Scrollbar(wrap, orient="vertical", command=self.summary_tree.yview)
        self.summary_tree.configure(yscrollcommand=y_scroll.set)
        self.summary_tree.pack(side="left", fill="both", expand=True)
        y_scroll.pack(side="right", fill="y")

    def _create_result_tree(self, parent: ttk.Frame) -> ttk.Treeview:
        wrap = ttk.Frame(parent)
        wrap.pack(fill="both", expand=True)

        columns = tuple(RESULT_COLUMNS)
        tree = ttk.Treeview(wrap, columns=columns, show="headings")
        tree.heading("so_o", text="So O")
        tree.heading("ma_npl", text="Ma NPL")
        tree.heading("ten_npl", text="Ten NPL")
        tree.heading("ton_thuc_te", text="Ton thuc te")
        tree.heading("ton_dm_chua_xuat", text="Ton - dinh muc chua xuat")
        tree.heading("ket_luan", text="Ket luan")
        tree.column("so_o", width=140, anchor="center")
        tree.column("ma_npl", width=150, anchor="center")
        tree.column("ten_npl", width=400, anchor="w")
        tree.column("ton_thuc_te", width=120, anchor="center")
        tree.column("ton_dm_chua_xuat", width=160, anchor="center")
        tree.column("ket_luan", width=220, anchor="center")
        tree.tag_configure("negative", background="#ffd9d9")
        tree.tag_configure("normal", background="#e7f7e7")

        y_scroll = ttk.Scrollbar(wrap, orient="vertical", command=tree.yview)
        x_scroll = ttk.Scrollbar(wrap, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=y_scroll.set, xscrollcommand=x_scroll.set)
        tree.grid(row=0, column=0, sticky="nsew")
        y_scroll.grid(row=0, column=1, sticky="ns")
        x_scroll.grid(row=1, column=0, sticky="ew")
        wrap.rowconfigure(0, weight=1)
        wrap.columnconfigure(0, weight=1)
        return tree

    def choose_bom_file(self) -> None:
        path = filedialog.askopenfilename(
            title="Chon file Bang ke dinh muc",
            filetypes=[("Excel files", "*.xlsx *.xls"), ("All files", "*.*")],
        )
        if path:
            self.bom_file_var.set(path)
            self._save_last_paths()

    def choose_stock_file(self) -> None:
        path = filedialog.askopenfilename(
            title="Chon file Tong hop nhap xuat ton",
            filetypes=[("Excel files", "*.xlsx *.xls"), ("All files", "*.*")],
        )
        if path:
            self.stock_file_var.set(path)
            self._save_last_paths()

    def _load_last_paths(self) -> None:
        bom = self.config.get("last_bom_file", "")
        stock = self.config.get("last_stock_file", "")
        if bom and Path(bom).exists():
            self.bom_file_var.set(bom)
        if stock and Path(stock).exists():
            self.stock_file_var.set(stock)

    def _save_last_paths(self) -> None:
        self.config["last_bom_file"] = self.bom_file_var.get().strip()
        self.config["last_stock_file"] = self.stock_file_var.get().strip()
        save_config(self.config)

    def run_check(self) -> None:
        try:
            bom_file = self.bom_file_var.get().strip()
            stock_file = self.stock_file_var.get().strip()
            o_number_input = self.o_number_var.get().strip()
            if not bom_file or not stock_file or not o_number_input:
                messagebox.showwarning("Thieu du lieu", "Hay chon 2 file va nhap so O.")
                return
            if not Path(bom_file).exists() or not Path(stock_file).exists():
                messagebox.showerror("Loi file", "Duong dan file khong ton tai.")
                return

            self.status_label.config(text="Dang doc file va xu ly...")
            self.root.update_idletasks()
            self._save_last_paths()

            result_df = self.check_negative_stock(bom_file, stock_file, o_number_input)
            self.last_result_df = result_df.copy()
            self.loaded_run_id = None
            self.render_result(self.result_tree, result_df)
            negatives = (result_df["ket_luan"] == "AM (can xu ly)").sum()
            self.status_label.config(
                text=f"Xong: {len(result_df)} dong lien quan, {negatives} dong ton am."
            )
        except Exception as exc:
            messagebox.showerror("Loi", str(exc))
            self.status_label.config(text="Co loi trong qua trinh xu ly.")

    def check_negative_stock(
        self, bom_file: str, stock_file: str, o_number_input: str
    ) -> pd.DataFrame:
        bom_header = find_header_row(bom_file, must_have_keywords=["s/o", "ma npl", "ten npl"])
        stock_header = find_header_row(stock_file, must_have_keywords=["ma vat tu", "ton thuc te"])
        bom_df = pd.read_excel(bom_file, sheet_name=0, header=bom_header)
        stock_df = pd.read_excel(stock_file, sheet_name=0, header=stock_header)

        so_o_col = find_column(bom_df, ["s/o"])
        ma_npl_col = find_column(bom_df, ["ma", "npl"])
        ten_npl_col = find_column(bom_df, ["ten", "npl"])
        stock_ma_col = find_column(stock_df, ["ma", "vat tu"])
        ton_thuc_te_col = find_column(stock_df, ["ton", "thuc", "te"])
        ton_dm_chua_xuat_col = find_column(stock_df, ["ton", "dinh", "muc", "chua"])

        o_lookup = normalize_text(o_number_input)
        filtered = bom_df[bom_df[so_o_col].apply(normalize_text) == o_lookup].copy()
        if filtered.empty:
            raise ValueError(f"Khong tim thay so O: {o_number_input}")

        filtered["ma_npl_norm"] = filtered[ma_npl_col].apply(normalize_text)
        stock_df["ma_vat_tu_norm"] = stock_df[stock_ma_col].apply(normalize_text)

        merged = filtered.merge(
            stock_df[[stock_ma_col, "ma_vat_tu_norm", ton_thuc_te_col, ton_dm_chua_xuat_col]],
            how="left",
            left_on="ma_npl_norm",
            right_on="ma_vat_tu_norm",
        )
        merged[ton_thuc_te_col] = pd.to_numeric(merged[ton_thuc_te_col], errors="coerce")
        merged[ton_dm_chua_xuat_col] = pd.to_numeric(merged[ton_dm_chua_xuat_col], errors="coerce")

        def classify(row: pd.Series) -> str:
            if pd.isna(row[ton_thuc_te_col]) and pd.isna(row[ton_dm_chua_xuat_col]):
                return "Khong tim thay ma trong ton kho"
            if (pd.notna(row[ton_thuc_te_col]) and row[ton_thuc_te_col] < 0) or (
                pd.notna(row[ton_dm_chua_xuat_col]) and row[ton_dm_chua_xuat_col] < 0
            ):
                return "AM (can xu ly)"
            return "Binh thuong"

        merged["ket_luan"] = merged.apply(classify, axis=1)
        return pd.DataFrame(
            {
                "so_o": merged[so_o_col],
                "ma_npl": merged[ma_npl_col],
                "ten_npl": merged[ten_npl_col],
                "ton_thuc_te": merged[ton_thuc_te_col],
                "ton_dm_chua_xuat": merged[ton_dm_chua_xuat_col],
                "ket_luan": merged["ket_luan"],
            }
        )

    def render_result(self, tree: ttk.Treeview, result_df: pd.DataFrame) -> None:
        for row_id in tree.get_children():
            tree.delete(row_id)
        for _, row in result_df.iterrows():
            tag = "negative" if row["ket_luan"] == "AM (can xu ly)" else "normal"
            tree.insert(
                "",
                "end",
                values=(
                    row["so_o"],
                    row["ma_npl"],
                    row["ten_npl"],
                    "" if pd.isna(row["ton_thuc_te"]) else f"{row['ton_thuc_te']:.4f}",
                    "" if pd.isna(row["ton_dm_chua_xuat"]) else f"{row['ton_dm_chua_xuat']:.4f}",
                    row["ket_luan"],
                ),
                tags=(tag,),
            )

    def filter_current_results(self) -> None:
        if self.last_result_df is None:
            return
        kw = normalize_text(self.search_result_var.get())
        if not kw:
            self.render_result(self.result_tree, self.last_result_df)
            return
        mask = self.last_result_df.apply(
            lambda row: kw in normalize_text(row["so_o"])
            or kw in normalize_text(row["ma_npl"])
            or kw in normalize_text(row["ten_npl"])
            or kw in normalize_text(row["ket_luan"]),
            axis=1,
        )
        self.render_result(self.result_tree, self.last_result_df[mask].copy())

    def clear_result_filter(self) -> None:
        self.search_result_var.set("")
        if self.last_result_df is not None:
            self.render_result(self.result_tree, self.last_result_df)

    def export_excel(self) -> None:
        if self.last_result_df is None or self.last_result_df.empty:
            messagebox.showwarning("Chua co du lieu", "Hay bam 'Kiem tra' truoc khi xuat.")
            return
        output_path = filedialog.asksaveasfilename(
            title="Luu file ket qua",
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx")],
            initialfile=f"ket_qua_kiem_tra_{self.o_number_var.get().strip() or 'so_o'}.xlsx",
        )
        if not output_path:
            return
        export_df = self.last_result_df.copy()
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            export_df.to_excel(writer, sheet_name="KetQua", index=False)
            ws = writer.sheets["KetQua"]
            red_fill = PatternFill(start_color="FFD9D9", end_color="FFD9D9", fill_type="solid")
            green_fill = PatternFill(start_color="E7F7E7", end_color="E7F7E7", fill_type="solid")
            ket_luan_col_idx = list(export_df.columns).index("ket_luan") + 1
            for row_idx in range(2, len(export_df) + 2):
                fill = red_fill if ws.cell(row=row_idx, column=ket_luan_col_idx).value == "AM (can xu ly)" else green_fill
                for col_idx in range(1, len(export_df.columns) + 1):
                    ws.cell(row=row_idx, column=col_idx).fill = fill
        self.status_label.config(text=f"Da xuat file: {output_path}")
        messagebox.showinfo("Thanh cong", f"Da xuat file Excel:\n{output_path}")

    def save_current_run(self) -> None:
        if self.last_result_df is None or self.last_result_df.empty:
            messagebox.showwarning("Chua co du lieu", "Khong co du lieu de save.")
            return
        run_name = simpledialog.askstring("Ten run", "Nhap ten run de luu:", initialvalue=f"Run {self.o_number_var.get().strip()}")
        if run_name is None:
            return
        note = simpledialog.askstring("Ghi chu", "Nhap ghi chu (co the bo trong):", initialvalue="")
        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO runs (run_name, so_o, bom_file, stock_file, note) VALUES (?, ?, ?, ?, ?)",
            (
                run_name.strip() or f"Run {self.o_number_var.get().strip()}",
                self.o_number_var.get().strip(),
                self.bom_file_var.get().strip(),
                self.stock_file_var.get().strip(),
                (note or "").strip(),
            ),
        )
        run_id = cur.lastrowid
        rows = [
            (
                run_id,
                str(r["so_o"]),
                str(r["ma_npl"]),
                str(r["ten_npl"]),
                None if pd.isna(r["ton_thuc_te"]) else float(r["ton_thuc_te"]),
                None if pd.isna(r["ton_dm_chua_xuat"]) else float(r["ton_dm_chua_xuat"]),
                str(r["ket_luan"]),
            )
            for _, r in self.last_result_df.iterrows()
        ]
        cur.executemany(
            """
            INSERT INTO run_items
            (run_id, so_o, ma_npl, ten_npl, ton_thuc_te, ton_dm_chua_xuat, ket_luan)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
        conn.close()
        self.refresh_runs()
        messagebox.showinfo("Thanh cong", f"Da luu database, run_id = {run_id}")

    def refresh_runs(self) -> None:
        for item in self.runs_tree.get_children():
            self.runs_tree.delete(item)
        kw = normalize_text(self.search_db_var.get().strip())
        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT r.id, r.run_name, r.so_o, r.created_at, r.note, COUNT(i.id) AS item_count
            FROM runs r
            LEFT JOIN run_items i ON i.run_id = r.id
            GROUP BY r.id, r.run_name, r.so_o, r.created_at, r.note
            ORDER BY r.id DESC
            """
        )
        rows = cur.fetchall()
        conn.close()
        for row in rows:
            row_text = " ".join([normalize_text(v) for v in row if v is not None])
            if kw and kw not in row_text:
                continue
            self.runs_tree.insert("", "end", values=row)
        self.refresh_summary_run_list()

    def refresh_summary_run_list(self) -> None:
        if not hasattr(self, "summary_runs_listbox"):
            return
        self.summary_runs_listbox.delete(0, tk.END)
        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, run_name, so_o, created_at
            FROM runs
            ORDER BY id DESC
            """
        )
        rows = cur.fetchall()
        conn.close()
        for run_id, run_name, so_o, created_at in rows:
            display_text = f"{run_id} | {run_name} | {so_o} | {created_at}"
            self.summary_runs_listbox.insert(tk.END, display_text)

    def select_all_summary_runs(self) -> None:
        if not hasattr(self, "summary_runs_listbox"):
            return
        self.summary_runs_listbox.select_set(0, tk.END)

    def clear_summary_run_selection(self) -> None:
        if not hasattr(self, "summary_runs_listbox"):
            return
        self.summary_runs_listbox.selection_clear(0, tk.END)

    def _selected_summary_run_ids(self) -> list[int]:
        selected_indices = self.summary_runs_listbox.curselection()
        run_ids: list[int] = []
        for idx in selected_indices:
            text = self.summary_runs_listbox.get(idx)
            run_id_text = text.split("|", 1)[0].strip()
            if run_id_text.isdigit():
                run_ids.append(int(run_id_text))
        return run_ids

    def _selected_run_id(self) -> int | None:
        selected = self.runs_tree.selection()
        if not selected:
            messagebox.showwarning("Chua chon run", "Hay chon 1 run trong bang.")
            return None
        return int(self.runs_tree.item(selected[0], "values")[0])

    def load_selected_run(self) -> None:
        run_id = self._selected_run_id()
        if run_id is None:
            return
        conn = sqlite3.connect(DB_FILE)
        query = """
            SELECT so_o, ma_npl, ten_npl, ton_thuc_te, ton_dm_chua_xuat, ket_luan
            FROM run_items
            WHERE run_id = ?
            ORDER BY id
        """
        df = pd.read_sql_query(query, conn, params=(run_id,))
        cur = conn.cursor()
        cur.execute("SELECT so_o, bom_file, stock_file FROM runs WHERE id = ?", (run_id,))
        run_meta = cur.fetchone()
        conn.close()
        if df.empty:
            messagebox.showwarning("Trong", "Run nay khong co item.")
            return
        self.last_result_df = df.copy()
        self.loaded_run_id = run_id
        self.o_number_var.set(str(run_meta[0] if run_meta else ""))
        self.bom_file_var.set(str(run_meta[1] if run_meta else ""))
        self.stock_file_var.set(str(run_meta[2] if run_meta else ""))
        self.render_result(self.result_tree, self.last_result_df)
        self.render_result(self.db_item_tree, self.last_result_df)
        self.notebook.select(self.tab_check)
        self.status_label.config(text=f"Da load run_id={run_id}, so dong={len(df)}")

    def update_selected_run_note(self) -> None:
        run_id = self._selected_run_id()
        if run_id is None:
            return
        new_note = simpledialog.askstring("Update note", "Nhap ghi chu moi:")
        if new_note is None:
            return
        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()
        cur.execute("UPDATE runs SET note = ? WHERE id = ?", (new_note.strip(), run_id))
        conn.commit()
        conn.close()
        self.refresh_runs()

    def delete_selected_run(self) -> None:
        run_id = self._selected_run_id()
        if run_id is None:
            return
        if not messagebox.askyesno("Xac nhan", f"Xoa run_id={run_id}?"):
            return
        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()
        cur.execute("DELETE FROM run_items WHERE run_id = ?", (run_id,))
        cur.execute("DELETE FROM runs WHERE id = ?", (run_id,))
        conn.commit()
        conn.close()
        self.refresh_runs()
        for item in self.db_item_tree.get_children():
            self.db_item_tree.delete(item)

    def build_summary_report(self) -> None:
        selected_run_ids = self._selected_summary_run_ids()
        if not selected_run_ids:
            messagebox.showwarning(
                "Chua chon run",
                "Hay chon it nhat 1 run trong danh sach de tao Super Report.",
            )
            return

        conn = sqlite3.connect(DB_FILE)
        placeholders = ",".join("?" for _ in selected_run_ids)
        query = """
            SELECT
                ma_npl,
                ten_npl,
                so_o,
                run_id,
                ton_thuc_te,
                ton_dm_chua_xuat,
                ket_luan
            FROM run_items
            WHERE run_id IN ({run_ids})
            ORDER BY ma_npl, so_o, run_id, id
        """.format(run_ids=placeholders)
        df = pd.read_sql_query(query, conn, params=selected_run_ids)
        conn.close()
        for item in self.summary_tree.get_children():
            self.summary_tree.delete(item)
        if df.empty:
            self.summary_label.config(text="Chua co du lieu trong database.")
            return

        df["ma_npl"] = df["ma_npl"].fillna("").astype(str)
        df["ten_npl"] = df["ten_npl"].fillna("").astype(str)
        df["so_o"] = df["so_o"].fillna("").astype(str)
        df["ket_luan"] = df["ket_luan"].fillna("").astype(str)

        grouped_npl = int(df["ma_npl"].nunique())
        neg_count = int((df["ket_luan"] == "AM (can xu ly)").sum())
        self.summary_label.config(
            text=f"File gop ({len(selected_run_ids)} run): {len(df)} dong, {grouped_npl} ma NPL, {neg_count} dong am."
        )

        last_ma_npl = None
        for _, row in df.iterrows():
            show_group_header = row["ma_npl"] != last_ma_npl
            last_ma_npl = row["ma_npl"]
            tag = "negative" if row["ket_luan"] == "AM (can xu ly)" else "normal"
            self.summary_tree.insert(
                "",
                "end",
                values=(
                    row["ma_npl"] if show_group_header else "",
                    row["ten_npl"] if show_group_header else "",
                    row["so_o"],
                    int(row["run_id"]) if pd.notna(row["run_id"]) else "",
                    "" if pd.isna(row["ton_thuc_te"]) else f"{row['ton_thuc_te']:.4f}",
                    ""
                    if pd.isna(row["ton_dm_chua_xuat"])
                    else f"{row['ton_dm_chua_xuat']:.4f}",
                    row["ket_luan"],
                ),
                tags=(tag,),
            )

    def switch_to_summary(self) -> None:
        self.build_summary_report()
        self.notebook.select(self.tab_summary)


def main() -> None:
    root = tk.Tk()
    app = App(root)
    root.mainloop()


if __name__ == "__main__":
    main()
