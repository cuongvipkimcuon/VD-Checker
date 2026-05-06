import json
import math
import re
import sqlite3
import tkinter as tk
from io import StringIO
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
    if not text:
        return ""
    canonical = text.replace(".", " ").replace("_", " ").strip().lower()
    if canonical == "new material":
        return "NEW MATERIAL"
    return text.replace(" ", ".")


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


class FormulaError(ValueError):
    pass


@dataclass
class ExprNode:
    kind: str
    value: str | float | None = None
    left: "ExprNode | None" = None
    right: "ExprNode | None" = None
    args: list["ExprNode"] | None = None


class FormulaParser:
    _token_re = re.compile(
        r"\s*(>=|<=|==|!=|[+\-*/(),<>^!]|\d+(?:\.\d+)?|[A-Za-z_][A-Za-z0-9_]*)"
    )

    def __init__(self, formula: str):
        self.formula = formula.strip()
        self.tokens = self._tokenize(self.formula)
        self.pos = 0

    @classmethod
    def _tokenize(cls, text: str) -> list[str]:
        tokens: list[str] = []
        idx = 0
        while idx < len(text):
            m = cls._token_re.match(text, idx)
            if not m:
                raise FormulaError(f"Ky tu khong hop le gan vi tri {idx + 1}.")
            token = m.group(1)
            tokens.append(token)
            idx = m.end()
        return tokens

    def parse(self) -> ExprNode:
        if not self.tokens:
            raise FormulaError("Cong thuc trong.")
        node = self._parse_comparison()
        if self.pos != len(self.tokens):
            raise FormulaError(f"Token du: '{self.tokens[self.pos]}'.")
        return node

    def _current(self) -> str | None:
        if self.pos >= len(self.tokens):
            return None
        return self.tokens[self.pos]

    def _consume(self, expected: str | None = None) -> str:
        token = self._current()
        if token is None:
            raise FormulaError("Cong thuc bi thieu toan hang.")
        if expected is not None and token != expected:
            raise FormulaError(f"Can '{expected}' nhung gap '{token}'.")
        self.pos += 1
        return token

    def _parse_comparison(self) -> ExprNode:
        node = self._parse_term()
        while self._current() in {">", "<", ">=", "<=", "==", "!="}:
            op = self._consume()
            rhs = self._parse_term()
            node = ExprNode(kind="binary", value=op, left=node, right=rhs)
        return node

    def _parse_term(self) -> ExprNode:
        node = self._parse_factor()
        while self._current() in {"+", "-"}:
            op = self._consume()
            rhs = self._parse_factor()
            node = ExprNode(kind="binary", value=op, left=node, right=rhs)
        return node

    def _parse_factor(self) -> ExprNode:
        node = self._parse_power()
        while self._current() in {"*", "/"}:
            op = self._consume()
            rhs = self._parse_power()
            node = ExprNode(kind="binary", value=op, left=node, right=rhs)
        return node

    def _parse_power(self) -> ExprNode:
        node = self._parse_unary()
        if self._current() == "^":
            op = self._consume("^")
            rhs = self._parse_power()
            node = ExprNode(kind="binary", value=op, left=node, right=rhs)
        return node

    def _parse_unary(self) -> ExprNode:
        token = self._current()
        if token == "-":
            self._consume("-")
            return ExprNode(kind="unary", value="-", right=self._parse_unary())
        return self._parse_postfix()

    def _parse_postfix(self) -> ExprNode:
        node = self._parse_primary()
        while self._current() == "!":
            self._consume("!")
            node = ExprNode(kind="factorial", right=node)
        return node

    def _parse_primary(self) -> ExprNode:
        token = self._current()
        if token is None:
            raise FormulaError("Cong thuc bi thieu toan hang.")
        if token == "(":
            self._consume("(")
            node = self._parse_comparison()
            self._consume(")")
            return node
        if re.fullmatch(r"\d+(?:\.\d+)?", token):
            self._consume()
            return ExprNode(kind="number", value=float(token))
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", token):
            name = self._consume().upper()
            if self._current() == "(":
                self._consume("(")
                args: list[ExprNode] = []
                if self._current() != ")":
                    while True:
                        args.append(self._parse_comparison())
                        if self._current() == ",":
                            self._consume(",")
                            continue
                        break
                self._consume(")")
                return ExprNode(kind="func", value=name, args=args)
            return ExprNode(kind="var", value=name)
        raise FormulaError(f"Token khong hop le: '{token}'.")


def eval_formula_ast(node: ExprNode, env: dict[str, float]) -> float | bool:
    if node.kind == "number":
        return float(node.value)
    if node.kind == "var":
        key = str(node.value).upper()
        if key not in env:
            raise FormulaError(f"Bien '{key}' khong duoc ho tro.")
        return float(env[key])
    if node.kind == "unary":
        val = eval_formula_ast(node.right, env)  # type: ignore[arg-type]
        if node.value == "-":
            return -float(val)
        raise FormulaError(f"Toan tu unary khong ho tro: {node.value}")
    if node.kind == "binary":
        left = eval_formula_ast(node.left, env)  # type: ignore[arg-type]
        right = eval_formula_ast(node.right, env)  # type: ignore[arg-type]
        op = node.value
        if op == "+":
            return float(left) + float(right)
        if op == "-":
            return float(left) - float(right)
        if op == "*":
            return float(left) * float(right)
        if op == "/":
            if float(right) == 0:
                raise FormulaError("Khong the chia cho 0.")
            return float(left) / float(right)
        if op == "^":
            return float(left) ** float(right)
        if op == ">":
            return float(left) > float(right)
        if op == "<":
            return float(left) < float(right)
        if op == ">=":
            return float(left) >= float(right)
        if op == "<=":
            return float(left) <= float(right)
        if op == "==":
            return float(left) == float(right)
        if op == "!=":
            return float(left) != float(right)
        raise FormulaError(f"Toan tu khong ho tro: {op}")
    if node.kind == "factorial":
        val = float(eval_formula_ast(node.right, env))  # type: ignore[arg-type]
        if val < 0 or int(val) != val:
            raise FormulaError("Giai thua chi ap dung cho so nguyen khong am.")
        return float(math.factorial(int(val)))
    if node.kind == "func":
        fn = str(node.value).upper()
        args = node.args or []
        if fn == "ROUND":
            if len(args) not in {1, 2}:
                raise FormulaError("ROUND can 1 hoac 2 tham so.")
            x = float(eval_formula_ast(args[0], env))
            digits = int(float(eval_formula_ast(args[1], env))) if len(args) == 2 else 0
            return round(x, digits)
        if fn == "IF":
            if len(args) != 3:
                raise FormulaError("IF can dung 3 tham so: IF(dieu_kien,a,b).")
            cond = bool(eval_formula_ast(args[0], env))
            return float(eval_formula_ast(args[1], env) if cond else eval_formula_ast(args[2], env))
        if fn == "SQRT":
            if len(args) != 1:
                raise FormulaError("SQRT can 1 tham so.")
            x = float(eval_formula_ast(args[0], env))
            if x < 0:
                raise FormulaError("SQRT khong nhan so am.")
            return math.sqrt(x)
        if fn == "ROOT":
            if len(args) != 2:
                raise FormulaError("ROOT can 2 tham so: ROOT(x, bac).")
            x = float(eval_formula_ast(args[0], env))
            degree = float(eval_formula_ast(args[1], env))
            if degree == 0:
                raise FormulaError("Bac can khong duoc bang 0.")
            if x < 0 and int(degree) == degree and int(degree) % 2 == 1:
                return -((-x) ** (1.0 / degree))
            if x < 0:
                raise FormulaError("Chi cho phep can so am voi bac le.")
            return x ** (1.0 / degree)
        if fn == "FACT":
            if len(args) != 1:
                raise FormulaError("FACT can 1 tham so.")
            x = float(eval_formula_ast(args[0], env))
            if x < 0 or int(x) != x:
                raise FormulaError("FACT chi nhan so nguyen khong am.")
            return float(math.factorial(int(x)))
        raise FormulaError(f"Ham '{fn}' khong duoc ho tro.")
    raise FormulaError("AST khong hop le.")


def evaluate_carton_formula(formula: str, l: float, w: float, h: float, base: float) -> float:
    ast = FormulaParser(formula).parse()
    value = eval_formula_ast(ast, {"L": l, "W": w, "H": h, "BASE": base})
    return float(value)


@dataclass
class PricingSettings:
    inbound_file: str = ""
    stock_file: str = ""
    fx_usd_vnd: float = 24500.0
    processing_cost_vnd: float = 0.0
    processing_named_json: str = "[]"
    profit_rate: float = 0.3
    extra_cost_rate: float = 0.0
    extra_cost_mode: str = "direct"
    extra_cost_tiers_json: str = "[]"
    pricing_setup_done: bool = False
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
        self.bom_sheet_var = tk.StringVar(value="1")
        self.status_var = tk.StringVar(value="San sang.")
        self.summary_var = tk.StringVar(value="")
        self.kpi_cost_var = tk.StringVar(value="Cost Price: -")
        self.kpi_selling_var = tk.StringVar(value="Selling Price: -")
        self.kpi_margin_var = tk.StringVar(value="Margin: -")
        self.kpi_warning_var = tk.StringVar(value="")
        self.fixed_inbound_var = tk.StringVar(value="")
        self.fixed_stock_var = tk.StringVar(value="")
        self.cache_step1_var = tk.StringVar(value="Step 1: chua co cache")
        self.cache_step2_var = tk.StringVar(value="Step 2: chua co cache")
        self.cache_step3_var = tk.StringVar(value="Step 3: chua co cache")
        self.cache_latest_quote_var = tk.StringVar(value="Latest Quotation: chua co")
        self.quote_pick_var = tk.StringVar(value="")
        self.quote_pick_map: dict[str, int] = {}
        self.step2_show_usd_var = tk.BooleanVar(value=False)
        self.setup_carton_done_var = tk.BooleanVar(value=False)
        self.setup_pricing_done_var = tk.BooleanVar(value=False)
        self.setup_extra_done_var = tk.BooleanVar(value=False)

        self.df_bom_raw: pd.DataFrame | None = None
        self.df_step1: pd.DataFrame = pd.DataFrame()
        self.df_step2: pd.DataFrame = pd.DataFrame()
        self.df_inbound: pd.DataFrame | None = None
        self.df_stock: pd.DataFrame | None = None
        self.estimated_qty: int = 0
        self.manual_quote_qty: int = 0
        self.qty_from_h1: int = 0
        self.manual_qty_var = tk.StringVar(value="")

        self._build_ui()
        self._init_db()
        self._refresh_settings_labels()
        self._refresh_cache_status()

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
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS step_cache (
                step_name TEXT PRIMARY KEY,
                payload_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS quotation_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                quote_name TEXT,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        try:
            cur.execute("ALTER TABLE quotation_snapshots ADD COLUMN quote_name TEXT")
        except sqlite3.OperationalError:
            pass
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
            return pd.read_json(StringIO(row[1]), orient="split"), True
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
        top = ttk.Frame(self.root, padding=(10, 10, 10, 6))
        top.pack(fill="x")
        if self.back_to_launcher is not None:
            ttk.Button(top, text="Back ve Launcher", command=self._go_back).pack(side="right")

        self.main_tabs = ttk.Notebook(self.root)
        self.main_tabs.pack(fill="both", expand=True)
        self.tab_workstation = ttk.Frame(self.main_tabs, padding=8)
        self.tab_fixed_setup = ttk.Frame(self.main_tabs, padding=10)
        self.tab_quote_cache = ttk.Frame(self.main_tabs, padding=10)
        self.main_tabs.add(self.tab_workstation, text="Workstation")
        self.main_tabs.add(self.tab_fixed_setup, text="Setup Du Lieu Co Dinh")
        self.main_tabs.add(self.tab_quote_cache, text="Quotation Cache")

        workstation_top = ttk.Frame(self.tab_workstation)
        workstation_top.pack(fill="x")
        ttk.Label(workstation_top, text="File BOM quotation:").pack(side="left")
        ttk.Entry(workstation_top, textvariable=self.bom_path_var, width=62).pack(side="left", padx=6)
        ttk.Button(workstation_top, text="Chon BOM", command=self._choose_bom).pack(side="left")
        ttk.Label(workstation_top, text="Sheet:").pack(side="left", padx=(10, 4))
        ttk.Entry(workstation_top, textvariable=self.bom_sheet_var, width=10).pack(side="left")

        step_bar = ttk.Frame(self.tab_workstation, padding=(0, 8, 0, 6))
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

        ttk.Label(self.tab_workstation, textvariable=self.status_var, foreground="#1f4e79", padding=(0, 0, 0, 6)).pack(fill="x")

        self.tabs = ttk.Notebook(self.tab_workstation)
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
        self._build_fixed_setup_tab()
        self._build_quote_cache_tab()
        self._highlight_step_button(1)

    def _build_fixed_setup_tab(self) -> None:
        box = ttk.LabelFrame(self.tab_fixed_setup, text="Setup 2 file Excel dung nhieu lan")
        box.pack(fill="x", pady=(0, 10))
        ttk.Label(box, text="Bang ke nhap kho theo don hang").grid(row=0, column=0, sticky="w", padx=10, pady=8)
        ttk.Entry(box, textvariable=self.fixed_inbound_var, width=90).grid(row=0, column=1, sticky="ew", padx=10, pady=8)
        ttk.Button(box, text="Chon file", command=self._choose_inbound_file).grid(row=0, column=2, padx=10, pady=8)

        ttk.Label(box, text="Tong hop nhap xuat ton").grid(row=1, column=0, sticky="w", padx=10, pady=8)
        ttk.Entry(box, textvariable=self.fixed_stock_var, width=90).grid(row=1, column=1, sticky="ew", padx=10, pady=8)
        ttk.Button(box, text="Chon file", command=self._choose_stock_file).grid(row=1, column=2, padx=10, pady=8)

        ttk.Button(box, text="Luu setup file co dinh", command=self._save_fixed_files_from_tab).grid(
            row=2, column=1, sticky="e", padx=10, pady=(8, 10)
        )
        box.columnconfigure(1, weight=1)

    def _build_quote_cache_tab(self) -> None:
        step_box = ttk.LabelFrame(self.tab_quote_cache, text="Trang thai cache theo buoc")
        step_box.pack(fill="x", pady=(0, 10))
        ttk.Label(step_box, textvariable=self.cache_step1_var).pack(anchor="w", padx=10, pady=6)
        ttk.Label(step_box, textvariable=self.cache_step2_var).pack(anchor="w", padx=10, pady=6)
        ttk.Label(step_box, textvariable=self.cache_step3_var).pack(anchor="w", padx=10, pady=6)
        ttk.Button(step_box, text="Refresh trang thai", command=self._refresh_cache_status).pack(
            anchor="e", padx=10, pady=(0, 10)
        )

        quote_box = ttk.LabelFrame(self.tab_quote_cache, text="Quotation snapshot")
        quote_box.pack(fill="x", pady=(0, 10))
        ttk.Label(quote_box, textvariable=self.cache_latest_quote_var).pack(anchor="w", padx=10, pady=8)
        pick_wrap = ttk.Frame(quote_box)
        pick_wrap.pack(fill="x", padx=10, pady=(0, 8))
        ttk.Label(pick_wrap, text="Chon quotation:").pack(side="left")
        self.quote_pick_combo = ttk.Combobox(
            pick_wrap, textvariable=self.quote_pick_var, state="readonly", width=70
        )
        self.quote_pick_combo.pack(side="left", padx=8, fill="x", expand=True)
        actions = ttk.Frame(quote_box)
        actions.pack(fill="x", padx=10, pady=(0, 10))
        ttk.Button(actions, text="Save Quotation", command=self._save_current_quotation).pack(side="left")
        ttk.Button(actions, text="Load Latest Quotation", command=self._load_latest_quotation).pack(side="left", padx=8)
        ttk.Button(actions, text="Load Quotation Da Chon", command=self._load_selected_quotation).pack(side="left")

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

        cols = ("ma_npl", "ten_npl", "sldm", "dvt")
        self.tree_step1 = ttk.Treeview(self.tab_step1, columns=cols, show="headings", selectmode="browse")
        for c, w in [("ma_npl", 220), ("ten_npl", 560), ("sldm", 140), ("dvt", 140)]:
            self.tree_step1.heading(c, text=c)
            self.tree_step1.column(c, width=w, anchor="center")
        self.tree_step1.tag_configure("row_missing", background="#ffe0b2", foreground="#bf360c")
        self.tree_step1.tag_configure("row_no_n", background="#ffcdd2", foreground="#b71c1c")
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
        ttk.Checkbutton(
            actions,
            text="Hien thi USD",
            variable=self.step2_show_usd_var,
            command=self._on_step2_currency_toggle,
        ).pack(side="right", padx=(8, 0))
        ttk.Label(actions, text="Qty bao gia:").pack(side="right", padx=(12, 4))
        ttk.Entry(actions, textvariable=self.manual_qty_var, width=10).pack(side="right")
        ttk.Button(actions, text="Setup Phu Phi (Extra Cost)", command=self._open_extra_cost_setup).pack(side="right", padx=(8, 0))
        ttk.Button(actions, text="Setup Gia Co Ban", command=self._open_pricing_setup).pack(side="right")
        ttk.Button(actions, text="Setup Quy Cach Thung", command=self._open_cost_setup).pack(side="right")

        setup_status = ttk.LabelFrame(self.tab_step2, text="Checklist setup buoc 2")
        setup_status.pack(fill="x", pady=(0, 8))
        ttk.Checkbutton(setup_status, text="Da setup Quy cach thung", variable=self.setup_carton_done_var, state="disabled").pack(
            side="left", padx=10, pady=6
        )
        ttk.Checkbutton(setup_status, text="Da setup Gia co ban", variable=self.setup_pricing_done_var, state="disabled").pack(
            side="left", padx=10, pady=6
        )
        ttk.Checkbutton(setup_status, text="Da setup Phu phi", variable=self.setup_extra_done_var, state="disabled").pack(
            side="left", padx=10, pady=6
        )
        self._refresh_step2_setup_checklist()
        cols = (
            "ma_npl",
            "ten_npl",
            "sldm",
            "dvt",
            "gia_nguon_don",
            "bien_do_gia",
            "gia_nguon_ton",
            "gia_tinh",
            "tong_chi_phi",
        )
        self.tree_step2 = ttk.Treeview(self.tab_step2, columns=cols, show="headings", selectmode="browse")
        widths = [150, 300, 90, 80, 150, 130, 150, 150, 180]
        for c, w in zip(cols, widths):
            self.tree_step2.heading(c, text=c)
            self.tree_step2.column(c, width=w, anchor="center")
        self._update_step2_money_headers()
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

    def _open_pricing_setup(self) -> None:
        dlg = tk.Toplevel(self.root)
        dlg.title("Setup Gia Co Ban")
        dlg.geometry("760x430")
        dlg.transient(self.root)
        dlg.grab_set()

        vars_map = {
            "fx_usd_vnd": tk.StringVar(value=str(self.settings.fx_usd_vnd)),
            "processing_cost_vnd": tk.StringVar(value=str(self.settings.processing_cost_vnd)),
            "profit_rate": tk.StringVar(value=str(self.settings.profit_rate)),
        }
        labels = [
            ("Ty gia USD/VND", "fx_usd_vnd"),
            ("Processing / cai (VND)", "processing_cost_vnd"),
            ("Loi nhuan (vd 0.3)", "profit_rate"),
        ]
        for i, (lb, key) in enumerate(labels):
            ttk.Label(dlg, text=lb).grid(row=i, column=0, sticky="w", padx=10, pady=8)
            ttk.Entry(dlg, textvariable=vars_map[key], width=44).grid(row=i, column=1, sticky="ew", padx=10, pady=8)

        ttk.Label(
            dlg,
            text="Extra cost setup rieng bang nut 'Setup Extra Cost'.",
            foreground="#555555",
        ).grid(row=len(labels), column=0, columnspan=2, sticky="w", padx=10, pady=(2, 8))

        named_box = ttk.LabelFrame(dlg, text="Chi phi dat ten (VND)")
        named_box.grid(row=len(labels) + 1, column=0, columnspan=2, sticky="nsew", padx=10, pady=(0, 8))
        ttk.Label(named_box, text="Moi dong: ten_chi_phi,gia_vnd").pack(anchor="w", padx=8, pady=(8, 4))
        named_text = tk.Text(named_box, height=8, width=72)
        named_text.pack(fill="both", expand=True, padx=8, pady=(0, 8))
        current_named = self._parse_processing_named()
        if current_named:
            named_text.insert(
                "1.0",
                "\n".join(f"{norm_text(it.get('name', ''))},{to_float(it.get('value_vnd', 0), 0):g}" for it in current_named),
            )
        else:
            named_text.insert("1.0", "In line QC,0")

        def save() -> None:
            self.settings.fx_usd_vnd = to_float(vars_map["fx_usd_vnd"].get(), 24500.0)
            self.settings.processing_cost_vnd = to_float(vars_map["processing_cost_vnd"].get(), 0.0)
            self.settings.profit_rate = to_float(vars_map["profit_rate"].get(), 0.3)
            self.settings.processing_named_json = json.dumps(
                self._parse_processing_named_from_text(named_text.get("1.0", "end").strip()),
                ensure_ascii=False,
            )
            self.settings.pricing_setup_done = True
            self._save_settings()
            self._refresh_settings_labels()
            self._update_step2_money_headers()
            self._render_step2()
            dlg.destroy()

        ttk.Button(dlg, text="Luu", command=save).grid(row=len(labels) + 2, column=1, sticky="e", padx=10, pady=10)
        ttk.Button(dlg, text="Dong", command=dlg.destroy).grid(row=len(labels) + 2, column=1, sticky="w", padx=10, pady=10)
        dlg.columnconfigure(1, weight=1)
        dlg.rowconfigure(len(labels) + 1, weight=1)

    def _parse_processing_named(self) -> list[dict]:
        raw = norm_text(self.settings.processing_named_json)
        if not raw:
            return []
        try:
            data = json.loads(raw)
            if isinstance(data, list):
                out: list[dict] = []
                for row in data:
                    name = norm_text((row or {}).get("name", ""))
                    value_vnd = float(to_float((row or {}).get("value_vnd", 0), 0))
                    if name:
                        out.append({"name": name, "value_vnd": value_vnd})
                return out
        except Exception:
            return []
        return []

    def _parse_processing_named_from_text(self, raw_text: str) -> list[dict]:
        items: list[dict] = []
        for ln in [x.strip() for x in raw_text.splitlines() if x.strip()]:
            if "," in ln:
                name_part, value_part = ln.split(",", 1)
            else:
                parts = ln.rsplit(" ", 1)
                if len(parts) != 2:
                    continue
                name_part, value_part = parts[0], parts[1]
            name = norm_text(name_part)
            value_vnd = float(to_float(value_part, 0))
            if not name:
                continue
            items.append({"name": name, "value_vnd": value_vnd})
        return items

    def _on_step2_currency_toggle(self) -> None:
        self._update_step2_money_headers()
        self._render_step2()

    def _refresh_step2_setup_checklist(self) -> None:
        carton_done = (
            self.settings.carton_length > 0
            and self.settings.carton_width > 0
            and self.settings.carton_height > 0
            and self.settings.carton_base_price > 0
            and self.settings.bags_per_carton > 0
        )
        pricing_done = bool(self.settings.pricing_setup_done)
        if (self.settings.extra_cost_mode or "direct") == "tiered":
            extra_done = len(self._parse_extra_cost_tiers()) > 0
        else:
            extra_done = True
        self.setup_carton_done_var.set(carton_done)
        self.setup_pricing_done_var.set(pricing_done)
        self.setup_extra_done_var.set(extra_done)

    def _update_step2_money_headers(self) -> None:
        unit = "USD" if self.step2_show_usd_var.get() else "VND"
        if hasattr(self, "tree_step2"):
            self.tree_step2.heading("gia_nguon_don", text=f"gia_nguon_don ({unit})")
            self.tree_step2.heading("gia_nguon_ton", text=f"gia_nguon_ton ({unit})")
            self.tree_step2.heading("gia_tinh", text=f"gia_tinh ({unit})")
            self.tree_step2.heading("tong_chi_phi", text=f"tong_chi_phi ({unit})")

    def _open_extra_cost_setup(self) -> None:
        dlg = tk.Toplevel(self.root)
        dlg.title("Setup Phu Phi (Extra Cost)")
        dlg.geometry("700x520")
        dlg.transient(self.root)
        dlg.grab_set()

        mode_var = tk.StringVar(value=self.settings.extra_cost_mode or "direct")
        direct_var = tk.StringVar(value=str(self.settings.extra_cost_rate))
        tiers_text = tk.Text(dlg, height=14, width=72)

        ttk.Radiobutton(dlg, text="1) Nhap thang", variable=mode_var, value="direct").grid(
            row=0, column=0, sticky="w", padx=10, pady=(10, 6)
        )
        ttk.Entry(dlg, textvariable=direct_var, width=18).grid(row=0, column=1, sticky="w", padx=6, pady=(10, 6))
        ttk.Label(dlg, text="(0.1 = 10%)").grid(row=0, column=2, sticky="w", padx=6, pady=(10, 6))

        ttk.Radiobutton(dlg, text="2) Che do so khop theo nguong so luong tui", variable=mode_var, value="tiered").grid(
            row=1, column=0, columnspan=3, sticky="w", padx=10, pady=(6, 4)
        )
        ttk.Label(
            dlg,
            text="Moi dong: min max rate  (vd: 1 999 0.05). Co the dung dau cach hoac dau phay.",
            foreground="#555555",
        ).grid(row=2, column=0, columnspan=3, sticky="w", padx=10, pady=(0, 6))
        tiers_text.grid(row=3, column=0, columnspan=3, sticky="nsew", padx=10, pady=(0, 8))

        current_tiers = self._parse_extra_cost_tiers()
        if current_tiers:
            tiers_text.insert(
                "1.0",
                "\n".join(f"{int(t['min_qty'])} {int(t['max_qty'])} {float(t['rate'])}" for t in current_tiers),
            )
        else:
            tiers_text.insert("1.0", "1 999 0.05\n1000 999999 0.08")

        def save() -> None:
            mode = mode_var.get().strip()
            if mode not in {"direct", "tiered"}:
                messagebox.showwarning("Extra Cost", "Mode khong hop le.")
                return
            self.settings.extra_cost_mode = mode
            self.settings.extra_cost_rate = to_float(direct_var.get(), 0.0)
            tiers_raw = tiers_text.get("1.0", "end").strip()
            parsed = self._parse_tiers_from_text(tiers_raw)
            err = self._validate_tier_ranges(parsed)
            if err:
                messagebox.showwarning("Extra Cost", err)
                return
            self.settings.extra_cost_tiers_json = json.dumps(parsed, ensure_ascii=False)
            self._save_settings()
            self._refresh_settings_labels()
            dlg.destroy()

        ttk.Button(dlg, text="Luu", command=save).grid(row=4, column=2, sticky="e", padx=10, pady=10)
        ttk.Button(dlg, text="Dong", command=dlg.destroy).grid(row=4, column=2, sticky="w", padx=10, pady=10)
        dlg.columnconfigure(0, weight=1)
        dlg.columnconfigure(1, weight=1)
        dlg.columnconfigure(2, weight=1)
        dlg.rowconfigure(3, weight=1)

    def _parse_tiers_from_text(self, raw_text: str) -> list[dict]:
        tiers: list[dict] = []
        lines = [ln.strip() for ln in raw_text.splitlines() if ln.strip()]
        for ln in lines:
            normalized = ln.replace(",", " ")
            parts = [p.strip() for p in normalized.split() if p.strip()]
            if len(parts) != 3:
                continue
            left_qty = int(to_float(parts[0], 0))
            right_qty = int(to_float(parts[1], 0))
            rate = float(to_float(parts[2], 0))
            min_qty = min(left_qty, right_qty)
            max_qty = max(left_qty, right_qty)
            if min_qty <= 0 or max_qty < min_qty:
                continue
            tiers.append({"min_qty": min_qty, "max_qty": max_qty, "rate": rate})
        return tiers

    def _validate_tier_ranges(self, tiers: list[dict]) -> str:
        if not tiers:
            return ""
        ranges = sorted(
            [(int(t["min_qty"]), int(t["max_qty"])) for t in tiers],
            key=lambda x: (x[0], x[1]),
        )
        prev_min, prev_max = ranges[0]
        if prev_min <= 0:
            return "Nguong khong hop le: min phai > 0."
        for cur_min, cur_max in ranges[1:]:
            if cur_min <= prev_max:
                return (
                    "Nguong bi trung/lap. Hay sua de moi khoang so luong khong giao nhau.\n"
                    f"Bi trung tai: [{prev_min}, {prev_max}] va [{cur_min}, {cur_max}]"
                )
            prev_min, prev_max = cur_min, cur_max
        return ""

    def _parse_extra_cost_tiers(self) -> list[dict]:
        raw = norm_text(self.settings.extra_cost_tiers_json)
        if not raw:
            return []
        try:
            data = json.loads(raw)
            if isinstance(data, list):
                out: list[dict] = []
                for row in data:
                    min_qty = int(to_float((row or {}).get("min_qty", 0), 0))
                    max_qty = int(to_float((row or {}).get("max_qty", 0), 0))
                    rate = float(to_float((row or {}).get("rate", 0), 0))
                    if min_qty > 0 and max_qty >= min_qty:
                        out.append({"min_qty": min_qty, "max_qty": max_qty, "rate": rate})
                return out
        except Exception:
            return []
        return []

    def _format_step2_money(self, value_vnd: float) -> str:
        if self.step2_show_usd_var.get():
            fx = self.settings.fx_usd_vnd or 1
            return f"{(value_vnd / fx):,.4f}"
        return format_money(value_vnd)

    def _save_step_cache(self, step_name: str, payload: dict) -> None:
        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()
        now = datetime.now().isoformat(timespec="seconds")
        cur.execute(
            """
            INSERT INTO step_cache(step_name, payload_json, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(step_name) DO UPDATE SET
                payload_json = excluded.payload_json,
                updated_at = excluded.updated_at
            """,
            (step_name, json.dumps(payload, ensure_ascii=False), now),
        )
        conn.commit()
        conn.close()

    def _refresh_cache_status(self) -> None:
        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()
        rows = cur.execute("SELECT step_name, updated_at FROM step_cache").fetchall()
        snap = cur.execute(
            "SELECT id, quote_name, created_at FROM quotation_snapshots ORDER BY id DESC LIMIT 1"
        ).fetchone()
        all_quotes = cur.execute(
            "SELECT id, quote_name, created_at FROM quotation_snapshots ORDER BY id DESC LIMIT 50"
        ).fetchall()
        conn.close()
        lookup = {str(r[0]): str(r[1]) for r in rows}
        self.cache_step1_var.set(f"Step 1: {lookup.get('step1', 'chua co cache')}")
        self.cache_step2_var.set(f"Step 2: {lookup.get('step2', 'chua co cache')}")
        self.cache_step3_var.set(f"Step 3: {lookup.get('step3', 'chua co cache')}")
        if snap:
            quote_name = snap[1] if snap[1] else "(khong ten)"
            self.cache_latest_quote_var.set(f"Latest Quotation: #{snap[0]} - {quote_name} - {snap[2]}")
        else:
            self.cache_latest_quote_var.set("Latest Quotation: chua co")
        self.quote_pick_map = {}
        quote_labels: list[str] = []
        for qid, qname, created_at in all_quotes:
            name = qname if qname else "(khong ten)"
            label = f"#{qid} - {name} - {created_at}"
            quote_labels.append(label)
            self.quote_pick_map[label] = int(qid)
        if hasattr(self, "quote_pick_combo"):
            self.quote_pick_combo["values"] = quote_labels
            if quote_labels and self.quote_pick_var.get() not in self.quote_pick_map:
                self.quote_pick_var.set(quote_labels[0])

    def _save_current_quotation(self) -> None:
        default_name = datetime.now().strftime("Quotation %Y-%m-%d %H:%M")
        quote_name = simpledialog.askstring("Save Quotation", "Nhap ten quotation:", initialvalue=default_name)
        if quote_name is None:
            return
        quote_name = quote_name.strip() or default_name
        payload = {
            "settings": asdict(self.settings),
            "estimated_qty": self.estimated_qty,
            "qty_from_h1": self.qty_from_h1,
            "manual_quote_qty": int(to_float(self.manual_qty_var.get(), 0) or 0),
            "df_step1": self.df_step1.to_json(orient="split", force_ascii=False),
            "df_step2": self.df_step2.to_json(orient="split", force_ascii=False),
            "summary_text": self.summary_var.get(),
            "kpi_cost": self.kpi_cost_var.get(),
            "kpi_selling": self.kpi_selling_var.get(),
            "kpi_margin": self.kpi_margin_var.get(),
            "kpi_warning": self.kpi_warning_var.get(),
        }
        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()
        now = datetime.now().isoformat(timespec="seconds")
        cur.execute(
            "INSERT INTO quotation_snapshots(quote_name, payload_json, created_at) VALUES (?, ?, ?)",
            (quote_name, json.dumps(payload, ensure_ascii=False), now),
        )
        conn.commit()
        conn.close()
        self._refresh_cache_status()
        messagebox.showinfo("Quotation", f"Da save quotation: {quote_name}")

    def _load_latest_quotation(self) -> None:
        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()
        row = cur.execute(
            "SELECT payload_json FROM quotation_snapshots ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        if not row:
            messagebox.showwarning("Quotation", "Chua co quotation snapshot de load.")
            return
        self._apply_quotation_payload(row[0])
        messagebox.showinfo("Quotation", "Da load quotation snapshot moi nhat.")

    def _load_selected_quotation(self) -> None:
        label = self.quote_pick_var.get().strip()
        if not label or label not in self.quote_pick_map:
            messagebox.showwarning("Quotation", "Hay chon quotation can load.")
            return
        quote_id = self.quote_pick_map[label]
        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()
        row = cur.execute(
            "SELECT payload_json FROM quotation_snapshots WHERE id = ?",
            (quote_id,),
        ).fetchone()
        conn.close()
        if not row:
            messagebox.showwarning("Quotation", "Khong tim thay quotation da chon.")
            return
        self._apply_quotation_payload(row[0])
        messagebox.showinfo("Quotation", f"Da load quotation #{quote_id}.")

    def _apply_quotation_payload(self, payload_json: str) -> None:
        payload = json.loads(payload_json)
        data_settings = payload.get("settings", {})
        self.settings = PricingSettings(**{k: data_settings.get(k, v) for k, v in asdict(PricingSettings()).items()})
        self.estimated_qty = int(payload.get("estimated_qty", 0) or 0)
        self.qty_from_h1 = int(payload.get("qty_from_h1", 0) or 0)
        self.manual_quote_qty = int(payload.get("manual_quote_qty", 0) or 0)
        self.manual_qty_var.set(str(self.manual_quote_qty) if self.manual_quote_qty > 0 else "")
        df1_json = payload.get("df_step1", "")
        df2_json = payload.get("df_step2", "")
        self.df_step1 = pd.read_json(StringIO(df1_json), orient="split") if df1_json else pd.DataFrame()
        self.df_step2 = pd.read_json(StringIO(df2_json), orient="split") if df2_json else pd.DataFrame()
        self.summary_var.set(payload.get("summary_text", ""))
        self.kpi_cost_var.set(payload.get("kpi_cost", "Cost Price: -"))
        self.kpi_selling_var.set(payload.get("kpi_selling", "Selling Price: -"))
        self.kpi_margin_var.set(payload.get("kpi_margin", "Margin: -"))
        self.kpi_warning_var.set(payload.get("kpi_warning", ""))
        self._render_step1()
        self._render_step2()
        self._refresh_settings_labels()
        self._refresh_cache_status()
        self.main_tabs.select(self.tab_workstation)

    def _refresh_settings_labels(self) -> None:
        self.fixed_inbound_var.set(self.settings.inbound_file or "")
        self.fixed_stock_var.set(self.settings.stock_file or "")
        if hasattr(self, "fx_var"):
            self.fx_var.set(str(self.settings.fx_usd_vnd))
        if hasattr(self, "processing_var"):
            self.processing_var.set(str(self.settings.processing_cost_vnd))
        if hasattr(self, "profit_var"):
            self.profit_var.set(str(self.settings.profit_rate))
        self.status_var.set(
            "File co dinh: "
            f"Nhap kho={Path(self.settings.inbound_file).name if self.settings.inbound_file else '(chua setup)'} | "
            f"Ton kho={Path(self.settings.stock_file).name if self.settings.stock_file else '(chua setup)'}"
        )
        self._refresh_step2_setup_checklist()

    def _choose_bom(self) -> None:
        path = filedialog.askopenfilename(
            title="Chon file quotation BOM",
            filetypes=[("Excel files", "*.xlsx *.xls"), ("All files", "*.*")],
        )
        if path:
            self.bom_path_var.set(path)

    def _setup_fixed_files(self) -> None:
        # Kept for backward compatibility, routed to tab save behavior.
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

    def _choose_inbound_file(self) -> None:
        path = filedialog.askopenfilename(
            title="Chon file BANG KE NHAP KHO THEO DON HANG",
            filetypes=[("Excel files", "*.xlsx *.xls"), ("All files", "*.*")],
        )
        if path:
            self.fixed_inbound_var.set(path)

    def _choose_stock_file(self) -> None:
        path = filedialog.askopenfilename(
            title="Chon file TONG HOP NHAP XUAT TON",
            filetypes=[("Excel files", "*.xlsx *.xls"), ("All files", "*.*")],
        )
        if path:
            self.fixed_stock_var.set(path)

    def _save_fixed_files_from_tab(self) -> None:
        inbound = self.fixed_inbound_var.get().strip()
        stock = self.fixed_stock_var.get().strip()
        if not inbound or not Path(inbound).exists():
            messagebox.showwarning("Setup file", "File nhap kho khong hop le.")
            return
        if not stock or not Path(stock).exists():
            messagebox.showwarning("Setup file", "File ton kho khong hop le.")
            return
        self.settings.inbound_file = inbound
        self.settings.stock_file = stock
        self._save_settings()
        self._refresh_settings_labels()
        self.main_tabs.select(self.tab_workstation)

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
            self.estimated_qty = 0
            self.qty_from_h1 = 0
            sheet_name = self._resolve_bom_sheet_name(bom_path, self.bom_sheet_var.get().strip())
            self.df_bom_raw = pd.read_excel(bom_path, sheet_name=sheet_name, header=None)
            h1_val = to_float(self.df_bom_raw.iat[0, 7] if self.df_bom_raw.shape[1] > 7 else 0, 0)
            self.qty_from_h1 = int(h1_val) if h1_val > 0 else 0
            stock_df, stock_cached = self._load_fixed_file_cached("stock", self.settings.stock_file)
            self.df_stock = stock_df

            b = self.df_bom_raw.iloc[:, 1].map(norm_text) if self.df_bom_raw.shape[1] > 1 else pd.Series([""] * len(self.df_bom_raw))
            j = self.df_bom_raw.iloc[:, 9].map(norm_text) if self.df_bom_raw.shape[1] > 9 else pd.Series([""] * len(self.df_bom_raw))
            valid_rows = self.df_bom_raw[(b != "") & (j != "")]

            stock_map_name = {}
            for _, row in stock_df.iterrows():
                code = normalize_code(row.iloc[0] if len(row) > 0 else "")
                name = norm_text(row.iloc[1] if len(row) > 1 else "")
                if code and name and code not in stock_map_name:
                    stock_map_name[code] = name

            rows = []
            qty_candidates: list[int] = []
            qty_counts: dict[int, int] = {}
            for idx, row in valid_rows.iterrows():
                code = normalize_code(row.iloc[0] if len(row) > 0 else "")
                ma_npl = code if code else "NEW MATERIAL"
                ten_npl = ""
                col_b_text = norm_text(row.iloc[1] if len(row) > 1 else "")
                if code and code in stock_map_name:
                    ten_npl = stock_map_name[code]
                if not ten_npl:
                    if "consumption" in col_b_text.lower():
                        ten_npl = self._find_nearest_name_above(idx)
                    else:
                        ten_npl = col_b_text
                sldm = self._derive_sldm(row)
                if sldm <= 0:
                    continue
                rows.append(
                    {
                        "ma_npl": ma_npl,
                        "ten_npl": ten_npl,
                        "sldm": sldm,
                        "dvt": norm_text(row.iloc[9]),
                        "has_col_n": norm_text(row.iloc[13] if len(row) > 13 else "") != "",
                        "bom_price_usd": to_float(row.iloc[15] if len(row) > 15 else 0, 0),
                        "bom_price_vnd": to_float(row.iloc[16] if len(row) > 16 else 0, 0),
                    }
                )
                qty_guess = self._derive_order_qty(row)
                if qty_guess > 0:
                    qty_candidates.append(qty_guess)
                    qty_counts[qty_guess] = qty_counts.get(qty_guess, 0) + 1
                    if qty_counts[qty_guess] >= 3:
                        self.estimated_qty = qty_guess

            self.df_step1 = pd.DataFrame(rows)
            if self.estimated_qty <= 0:
                self.estimated_qty = self._pick_most_common_qty(qty_candidates)
            self._render_step1()
            self._auto_detect_carton()
            source_note = "cache" if stock_cached else "read file"
            self.status_var.set(
                f"Buoc 1 xong: doc {len(self.df_step1)} dong hop le (sheet '{sheet_name}'). Ton kho: {source_note}."
            )
            self._save_step_cache(
                "step1",
                {
                    "sheet_name": sheet_name,
                    "estimated_qty": self.estimated_qty,
                    "qty_from_h1": self.qty_from_h1,
                    "row_count": len(self.df_step1),
                    "df_step1": self.df_step1.to_json(orient="split", force_ascii=False),
                },
            )
            self._refresh_cache_status()
            self.tabs.select(self.tab_step1)
            self._highlight_step_button(1)
        except Exception as exc:
            messagebox.showerror("Quotation", f"Loi doc Step 1: {exc}")

    def _resolve_bom_sheet_name(self, bom_path: str, sheet_input: str) -> str:
        with pd.ExcelFile(bom_path) as xls:
            sheet_names = list(xls.sheet_names)
        if not sheet_names:
            raise ValueError("File BOM khong co sheet.")
        raw_input = sheet_input.strip()
        if not raw_input:
            self.bom_sheet_var.set("1")
            return sheet_names[0]

        # Uu tien tim theo ten truoc.
        sheet_input = raw_input
        for name in sheet_names:
            if name == sheet_input:
                return name
        lower_input = sheet_input.lower()
        for name in sheet_names:
            if name.lower() == lower_input:
                return name
        for name in sheet_names:
            if lower_input in name.lower():
                return name

        # Neu khong match ten thi fallback ve so thu tu sheet (1-based): "1", "01", "#1", "sheet 1", ...
        num_match = re.search(r"\d+", raw_input)
        if num_match:
            pos = int(num_match.group(0))
            if 1 <= pos <= len(sheet_names):
                return sheet_names[pos - 1]

        raise ValueError(f"Khong tim thay sheet '{raw_input}'.")

    def _derive_order_qty(self, row: pd.Series) -> int:
        i_val = to_float(row.iloc[8] if len(row) > 8 else 0, 0)
        h_val = to_float(row.iloc[7] if len(row) > 7 else 0, 0)
        g_val = to_float(row.iloc[6] if len(row) > 6 else 0, 0)
        base = h_val if h_val > 0 else g_val
        if i_val > 0 and base > 0:
            return int(i_val / base)
        return 0

    def _pick_most_common_qty(self, values: list[int]) -> int:
        if not values:
            return 0
        counts: dict[int, int] = {}
        for v in values:
            counts[v] = counts.get(v, 0) + 1
        return max(counts.items(), key=lambda item: (item[1], item[0]))[0]

    def _derive_sldm(self, row: pd.Series) -> float:
        h_val = to_float(row.iloc[7] if len(row) > 7 else 0, 0)
        if h_val > 0:
            return h_val
        g_val = to_float(row.iloc[6] if len(row) > 6 else 0, 0)
        if g_val > 0:
            return g_val
        return 0.0

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
                or norm_text(row.get("ma_npl", "")) == "NEW MATERIAL"
                or norm_text(row.get("ten_npl", "")) == ""
                or to_float(row.get("sldm", 0), 0) <= 0
                or norm_text(row.get("dvt", "")) == ""
            )
            has_col_n = bool(row.get("has_col_n", False))
            if not has_col_n:
                tag = "row_no_n"
            elif missing:
                tag = "row_missing"
            else:
                tag = ""
            self.tree_step1.insert(
                "",
                "end",
                iid=str(idx),
                values=(row["ma_npl"], row["ten_npl"], row["sldm"], row["dvt"]),
                tags=(tag,) if tag else (),
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
        dvt_var = tk.StringVar(value=(initial or {}).get("dvt", ""))
        ttk.Label(dlg, text="Ma NPL").grid(row=0, column=0, sticky="w", padx=10, pady=8)
        ttk.Entry(dlg, textvariable=ma_var, width=44).grid(row=0, column=1, padx=10, pady=8)
        ttk.Label(dlg, text="Ten NPL").grid(row=1, column=0, sticky="w", padx=10, pady=8)
        ttk.Entry(dlg, textvariable=ten_var, width=44).grid(row=1, column=1, padx=10, pady=8)
        ttk.Label(dlg, text="SLDM").grid(row=2, column=0, sticky="w", padx=10, pady=8)
        ttk.Entry(dlg, textvariable=sldm_var, width=44).grid(row=2, column=1, padx=10, pady=8)
        ttk.Label(dlg, text="DVT").grid(row=3, column=0, sticky="w", padx=10, pady=8)
        ttk.Entry(dlg, textvariable=dvt_var, width=44).grid(row=3, column=1, padx=10, pady=8)

        result = {"value": None}

        def save() -> None:
            result["value"] = {
                "ma_npl": norm_text(ma_var.get()) or "NEW MATERIAL",
                "ten_npl": norm_text(ten_var.get()),
                "sldm": to_float(sldm_var.get(), 0.0),
                "dvt": norm_text(dvt_var.get()),
            }
            dlg.destroy()

        ttk.Button(dlg, text="Luu", command=save).grid(row=4, column=1, sticky="e", padx=10, pady=12)
        ttk.Button(dlg, text="Huy", command=dlg.destroy).grid(row=4, column=1, sticky="w", padx=10, pady=12)
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
            out["gia_nguon_don"] = 0.0
            out["bien_do_gia"] = 0.0
            out["gia_nguon_ton"] = 0.0
            out["gia_tinh"] = 0.0
            out["gia_tinh_note"] = ""
            out["tong_chi_phi"] = 0.0
            out["gia_cao_nhat"] = 0.0
            out["gia_thap_nhat"] = 0.0
            out["gia_5_lan_gan_nhat"] = ""
            out["is_new_npl"] = False

            stock_price_map = {}
            for _, row in self.df_stock.iterrows():
                code = normalize_code(row.iloc[0] if len(row) > 0 else "")
                price = to_float(row.iloc[8] if len(row) > 8 else 0)
                if code and code not in stock_price_map:
                    stock_price_map[code] = price

            today = pd.Timestamp(datetime.now()).normalize()
            for idx, row in out.iterrows():
                code = normalize_code(row["ma_npl"])
                dvt = norm_text(row["dvt"]).lower()
                inbound_matches = self._find_inbound_matches(code, dvt)
                selected_price_total = 0.0
                bien_do_pct = 0.0
                if inbound_matches:
                    past_or_today = [m for m in inbound_matches if pd.notna(m["date"]) and m["date"].normalize() <= today]
                    if past_or_today:
                        chosen = max(past_or_today, key=lambda x: x["date"])
                    else:
                        chosen = max(
                            inbound_matches,
                            key=lambda x: x["date"] if pd.notna(x["date"]) else pd.Timestamp.min,
                        )
                    selected_price_total = chosen["price"]
                    all_prices = [m["price"] for m in inbound_matches if m["price"] > 0]
                    sorted_by_date = sorted(
                        inbound_matches,
                        key=lambda x: x["date"] if pd.notna(x["date"]) else pd.Timestamp.min,
                        reverse=True,
                    )
                    recent_prices = [m["price"] for m in sorted_by_date[:5] if m["price"] > 0]
                    if len(all_prices) > 1:
                        min_price = min(all_prices)
                        max_price = max(all_prices)
                        bien_do_pct = ((max_price - min_price) / min_price * 100) if min_price > 0 else 0.0
                    else:
                        min_price = all_prices[0] if all_prices else 0.0
                        max_price = all_prices[0] if all_prices else 0.0
                else:
                    min_price = 0.0
                    max_price = 0.0
                    recent_prices = []
                stock_total = stock_price_map.get(code, 0.0)
                gia_tinh = stock_total if stock_total > 0 else selected_price_total
                gia_tinh_note = ""
                if stock_total <= 0 and selected_price_total <= 0:
                    bom_vnd = to_float(row.get("bom_price_vnd", 0), 0)
                    bom_usd = to_float(row.get("bom_price_usd", 0), 0)
                    if bom_vnd > 0:
                        gia_tinh = bom_vnd
                        gia_tinh_note = "autofill(Q-VND)"
                    elif bom_usd > 0:
                        gia_tinh = bom_usd * self.settings.fx_usd_vnd
                        gia_tinh_note = "autofill(P-USD)"
                is_new_npl = (selected_price_total <= 0) and (stock_total <= 0)
                if is_new_npl:
                    if gia_tinh <= 0:
                        gia_tinh = 0.0
                tong_chi_phi = gia_tinh * to_float(row.get("sldm", 0), 0)
                out.at[idx, "gia_nguon_don"] = selected_price_total
                out.at[idx, "bien_do_gia"] = bien_do_pct
                out.at[idx, "gia_nguon_ton"] = stock_total
                out.at[idx, "gia_tinh"] = gia_tinh
                out.at[idx, "gia_tinh_note"] = gia_tinh_note
                out.at[idx, "tong_chi_phi"] = tong_chi_phi
                out.at[idx, "gia_cao_nhat"] = max_price
                out.at[idx, "gia_thap_nhat"] = min_price
                out.at[idx, "gia_5_lan_gan_nhat"] = json.dumps(recent_prices, ensure_ascii=False)
                out.at[idx, "is_new_npl"] = is_new_npl

            self.df_step2 = out
            self._render_step2()
            self.status_var.set(
                "Buoc 2 xong: da tinh gia nguon, gia tinh va tong chi phi. "
                f"Nhap kho={'cache' if inbound_cached else 'read file'} | Ton kho={'cache' if stock_cached else 'read file'}."
            )
            self._save_step_cache(
                "step2",
                {
                    "row_count": len(self.df_step2),
                    "df_step2": self.df_step2.to_json(orient="split", force_ascii=False),
                },
            )
            self._refresh_cache_status()
            self.tabs.select(self.tab_step2)
            self._highlight_step_button(2)
        except Exception as exc:
            messagebox.showerror("Step 2", f"Loi tinh toan: {exc}")

    def _find_inbound_matches(self, code: str, dvt_lower: str) -> list[dict]:
        if self.df_inbound is None:
            return []
        strict_matches: list[dict] = []
        loose_matches: list[dict] = []
        for _, row in self.df_inbound.iterrows():
            row_code = normalize_code(row.iloc[4] if len(row) > 4 else "")
            if row_code != code:
                continue
            price = to_float(row.iloc[9] if len(row) > 9 else 0)
            if price <= 0:
                continue
            date = pd.to_datetime(row.iloc[1] if len(row) > 1 else None, errors="coerce", dayfirst=True)
            row_item = {"price": price, "date": date}
            loose_matches.append(row_item)

            row_dvt = norm_text(row.iloc[6] if len(row) > 6 else "").lower()
            if dvt_lower and row_dvt and dvt_lower != row_dvt:
                continue
            strict_matches.append(row_item)
        if strict_matches:
            return strict_matches
        return loose_matches

    def _render_step2(self) -> None:
        for item in self.tree_step2.get_children():
            self.tree_step2.delete(item)
        for idx, row in self.df_step2.iterrows():
            is_missing = (
                to_float(row.get("sldm", 0), 0) <= 0
                or to_float(row.get("gia_tinh", 0), 0) <= 0
            )
            bien_do = to_float(row.get("bien_do_gia", 0), 0)
            bien_do_text = f"{bien_do:.1f}%"
            if bien_do > 20:
                bien_do_text = f"CANH BAO {bien_do:.1f}%"
            tag = "row_missing" if is_missing else ""
            self.tree_step2.insert(
                "",
                "end",
                iid=str(idx),
                values=(
                    row["ma_npl"],
                    row["ten_npl"],
                    f"{to_float(row['sldm']):,.4f}",
                    row["dvt"],
                    self._format_step2_money(to_float(row.get("gia_nguon_don", 0))),
                    bien_do_text,
                    self._format_step2_money(to_float(row.get("gia_nguon_ton", 0))),
                    (
                        f"{self._format_step2_money(to_float(row.get('gia_tinh', 0)))} ({norm_text(row.get('gia_tinh_note', ''))})"
                        if norm_text(row.get("gia_tinh_note", ""))
                        else self._format_step2_money(to_float(row.get("gia_tinh", 0)))
                    ),
                    self._format_step2_money(to_float(row.get("tong_chi_phi", 0))),
                ),
                tags=(tag,) if tag else (),
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
        row = self.df_step2.loc[idx]
        dlg = tk.Toplevel(self.root)
        dlg.title("Nhap don gia thu cong")
        dlg.geometry("640x380")
        dlg.transient(self.root)
        dlg.grab_set()

        price_var = tk.StringVar(value="")
        currency_var = tk.StringVar(value="VND")
        converted_var = tk.StringVar(value="Quy doi: -")
        recent_raw = norm_text(row.get("gia_5_lan_gan_nhat", ""))
        recent_prices: list[float] = []
        if recent_raw:
            try:
                arr = json.loads(recent_raw)
                if isinstance(arr, list):
                    recent_prices = [to_float(x, 0) for x in arr if to_float(x, 0) > 0]
            except Exception:
                recent_prices = []
        recent_text = ", ".join(format_money(v) for v in recent_prices) if recent_prices else "(khong co du lieu)"
        stock_price = to_float(row.get("gia_nguon_ton", 0), 0)
        highest = to_float(row.get("gia_cao_nhat", 0), 0)
        lowest = to_float(row.get("gia_thap_nhat", 0), 0)
        avg_recent = (sum(recent_prices) / len(recent_prices)) if recent_prices else 0.0

        info_box = ttk.LabelFrame(dlg, text="Thong tin gia tham khao")
        info_box.grid(row=0, column=0, columnspan=2, sticky="ew", padx=10, pady=(10, 8))
        ttk.Label(info_box, text=f"NPL: {norm_text(row.get('ma_npl', ''))} - {norm_text(row.get('ten_npl', ''))}").grid(
            row=0, column=0, sticky="w", padx=8, pady=4
        )
        ttk.Label(info_box, text=f"5 lan gan nhat: {recent_text}").grid(row=1, column=0, sticky="w", padx=8, pady=4)
        ttk.Label(
            info_box,
            text=f"Gia cao nhat: {format_money(highest) if highest > 0 else '(khong co)'} | "
            f"Gia thap nhat: {format_money(lowest) if lowest > 0 else '(khong co)'}",
        ).grid(row=2, column=0, sticky="w", padx=8, pady=4)
        ttk.Label(
            info_box,
            text=f"Gia trong ton: {format_money(stock_price) if stock_price > 0 else '(khong co)'}",
        ).grid(row=3, column=0, sticky="w", padx=8, pady=(4, 8))
        info_box.columnconfigure(0, weight=1)

        ttk.Label(dlg, text="Gia tinh custom:").grid(row=1, column=0, sticky="w", padx=10, pady=10)
        ttk.Entry(dlg, textvariable=price_var, width=30).grid(row=1, column=1, sticky="w", padx=10, pady=10)
        ttk.Label(dlg, text="Tien te:").grid(row=2, column=0, sticky="w", padx=10, pady=8)
        ccy_combo = ttk.Combobox(dlg, textvariable=currency_var, state="readonly", values=("USD", "VND"), width=23)
        ccy_combo.grid(row=2, column=1, sticky="w", padx=10, pady=8)
        ccy_combo.current(1)
        ttk.Label(dlg, textvariable=converted_var, foreground="#1f4e79").grid(
            row=3, column=0, columnspan=2, sticky="w", padx=10, pady=(0, 8)
        )

        quick_pick = ttk.Frame(dlg)
        quick_pick.grid(row=1, column=2, rowspan=3, sticky="nsw", padx=(4, 10), pady=8)

        def update_conversion_label(*_args: object) -> None:
            raw = to_float(price_var.get(), -1)
            if raw < 0:
                converted_var.set("Quy doi: -")
                return
            fx = self.settings.fx_usd_vnd or 1
            ccy = currency_var.get().strip().upper()
            if ccy == "USD":
                vnd = raw * fx
                converted_var.set(f"Quy doi: {raw:,.4f} USD = {format_money(vnd)} VND")
            else:
                usd = raw / fx
                converted_var.set(f"Quy doi: {format_money(raw)} VND = {usd:,.4f} USD")

        def apply_stock_price() -> None:
            if stock_price > 0:
                price_var.set(f"{stock_price:g}")
                currency_var.set("VND")
                update_conversion_label()
            else:
                messagebox.showwarning("Step 2", "Khong co gia ton de ap dung.")

        def apply_recent_avg() -> None:
            if avg_recent > 0:
                price_var.set(f"{avg_recent:g}")
                currency_var.set("VND")
                update_conversion_label()
            else:
                messagebox.showwarning("Step 2", "Khong du du lieu 5 ky gan nhat de tinh trung binh.")

        ttk.Button(quick_pick, text="Lay gia ton", command=apply_stock_price).pack(fill="x", pady=(0, 6))
        ttk.Button(quick_pick, text="TB 5 ky gan nhat", command=apply_recent_avg).pack(fill="x")
        price_var.trace_add("write", update_conversion_label)
        currency_var.trace_add("write", update_conversion_label)
        update_conversion_label()

        result = {"ok": False}

        def confirm() -> None:
            val = to_float(price_var.get(), -1)
            if val < 0:
                messagebox.showwarning("Step 2", "Don gia khong hop le.")
                return
            result["ok"] = True
            dlg.destroy()

        ttk.Button(dlg, text="Luu", command=confirm).grid(row=4, column=1, sticky="e", padx=10, pady=10)
        ttk.Button(dlg, text="Huy", command=dlg.destroy).grid(row=4, column=1, sticky="w", padx=10, pady=10)
        dlg.columnconfigure(1, weight=1)
        self.root.wait_window(dlg)
        if not result["ok"]:
            return

        val = to_float(price_var.get(), 0.0)
        ccy = currency_var.get().strip().upper()
        value_vnd = val * self.settings.fx_usd_vnd if ccy == "USD" else val
        if highest > 0 and lowest > 0 and (value_vnd > highest or value_vnd < lowest):
            ok = messagebox.askyesno(
                "Canh bao gia custom",
                (
                    f"Gia custom {format_money(value_vnd)} dang nam ngoai bien do lich su.\n"
                    f"Min: {format_money(lowest)} | Max: {format_money(highest)}\n"
                    "Ban co chac chan muon luu?"
                ),
            )
            if not ok:
                return
        # Custom gia tinh tren dong duoc chon.
        self.df_step2.at[idx, "gia_tinh"] = value_vnd
        self.df_step2.at[idx, "gia_tinh_note"] = "manual"
        self.df_step2.at[idx, "is_new_npl"] = True
        sldm = to_float(self.df_step2.at[idx, "sldm"], 0)
        self.df_step2.at[idx, "tong_chi_phi"] = value_vnd * sldm
        self._render_step2()

    def _open_cost_setup(self) -> None:
        dlg = tk.Toplevel(self.root)
        dlg.title("Setup Quy Cach Thung")
        dlg.geometry("760x560")
        dlg.transient(self.root)
        dlg.grab_set()

        vars_map = {
            "carton_length": tk.StringVar(value=str(self.settings.carton_length)),
            "carton_width": tk.StringVar(value=str(self.settings.carton_width)),
            "carton_height": tk.StringVar(value=str(self.settings.carton_height)),
            "bags_per_carton": tk.StringVar(value=str(self.settings.bags_per_carton)),
            "carton_base_price": tk.StringVar(value=str(self.settings.carton_base_price)),
            "carton_formula": tk.StringVar(value=self.settings.carton_formula),
        }
        formula_preview_var = tk.StringVar(value="Preview: -")
        labels = [
            ("Dai carton", "carton_length"),
            ("Rong carton", "carton_width"),
            ("Cao carton", "carton_height"),
            ("So tui / thung", "bags_per_carton"),
            ("Gia co so carton", "carton_base_price"),
        ]
        for i, (lb, key) in enumerate(labels):
            ttk.Label(dlg, text=lb).grid(row=i, column=0, sticky="w", padx=10, pady=6)
            ttk.Entry(dlg, textvariable=vars_map[key], width=58).grid(row=i, column=1, sticky="ew", padx=10, pady=6)

        row_formula = len(labels)
        ttk.Label(dlg, text="Cong thuc carton (optional)").grid(row=row_formula, column=0, sticky="w", padx=10, pady=6)
        ttk.Entry(dlg, textvariable=vars_map["carton_formula"], width=58).grid(
            row=row_formula, column=1, sticky="ew", padx=10, pady=6
        )
        ttk.Label(
            dlg,
            text=(
                "Bien: L=Dai carton, W=Rong carton, H=Cao carton, BASE=Gia co so carton. "
                "Bo co ban: + - * / ^ !, ngoac, ROUND(x,n), SQRT(x), ROOT(x,bac), FACT(x)."
            ),
            foreground="#555555",
        ).grid(row=row_formula + 1, column=0, columnspan=2, sticky="w", padx=10, pady=(2, 8))

        keypad = ttk.Frame(dlg)
        keypad.grid(row=row_formula + 2, column=0, columnspan=2, sticky="ew", padx=10, pady=(0, 6))
        for i in range(6):
            keypad.columnconfigure(i, weight=1)

        def append_formula(text: str) -> None:
            current = vars_map["carton_formula"].get().strip()
            vars_map["carton_formula"].set((current + " " + text).strip())

        def backspace_formula() -> None:
            parts = vars_map["carton_formula"].get().strip().split()
            if parts:
                parts.pop()
            vars_map["carton_formula"].set(" ".join(parts))

        def clear_formula() -> None:
            vars_map["carton_formula"].set("")
            formula_preview_var.set("Preview: -")

        def preview_formula() -> None:
            formula = vars_map["carton_formula"].get().strip()
            if not formula:
                formula_preview_var.set("Preview: (mac dinh)")
                return
            l = to_float(vars_map["carton_length"].get(), 0)
            w = to_float(vars_map["carton_width"].get(), 0)
            h = to_float(vars_map["carton_height"].get(), 0)
            base = to_float(vars_map["carton_base_price"].get(), 0)
            try:
                val = evaluate_carton_formula(formula, l, w, h, base)
                formula_preview_var.set(f"Preview: {val:,.2f} VND")
            except Exception as exc:
                formula_preview_var.set(f"Preview error: {exc}")

        basic_tokens = [
            "L",
            "W",
            "H",
            "BASE",
            "(",
            ")",
            "+",
            "-",
            "*",
            "/",
            "^",
            "!",
            "ROUND(",
            "SQRT(",
            "ROOT(",
            "FACT(",
            ",",
            "0",
            "1",
            "2",
            "3",
            "4",
            "5",
            "6",
            "7",
            "8",
            "9",
        ]
        advanced_tokens = [">", "<", ">=", "<=", "==", "!=", "IF("]

        for idx, token in enumerate(basic_tokens):
            r = idx // 6
            c = idx % 6
            ttk.Button(keypad, text=token, command=lambda t=token: append_formula(t)).grid(
                row=r, column=c, sticky="ew", padx=3, pady=3
            )

        advanced_open = tk.BooleanVar(value=False)
        advanced_frame = ttk.Frame(keypad)
        for i in range(6):
            advanced_frame.columnconfigure(i, weight=1)
        for idx, token in enumerate(advanced_tokens):
            r = idx // 6
            c = idx % 6
            ttk.Button(advanced_frame, text=token, command=lambda t=token: append_formula(t)).grid(
                row=r, column=c, sticky="ew", padx=3, pady=3
            )

        adv_row = (len(basic_tokens) - 1) // 6 + 1

        def toggle_advanced() -> None:
            if advanced_open.get():
                advanced_frame.grid_forget()
                advanced_open.set(False)
                btn_advanced.configure(text="Hien nang cao")
            else:
                advanced_frame.grid(row=adv_row, column=0, columnspan=6, sticky="ew")
                advanced_open.set(True)
                btn_advanced.configure(text="An nang cao")

        btn_advanced = ttk.Button(keypad, text="Hien nang cao", command=toggle_advanced)
        btn_advanced.grid(row=adv_row, column=0, columnspan=2, sticky="ew", padx=3, pady=3)

        action_row = adv_row + 1
        ttk.Button(keypad, text="Xoa token", command=backspace_formula).grid(
            row=action_row, column=0, columnspan=2, sticky="ew", padx=3, pady=3
        )
        ttk.Button(keypad, text="Clear", command=clear_formula).grid(row=action_row, column=2, sticky="ew", padx=3, pady=3)
        ttk.Button(keypad, text="Preview", command=preview_formula).grid(
            row=action_row, column=3, columnspan=3, sticky="ew", padx=3, pady=3
        )
        ttk.Label(dlg, textvariable=formula_preview_var, foreground="#1f4e79").grid(
            row=row_formula + 3, column=0, columnspan=2, sticky="w", padx=10, pady=(0, 8)
        )

        def save() -> None:
            self.settings.carton_length = to_float(vars_map["carton_length"].get(), 0)
            self.settings.carton_width = to_float(vars_map["carton_width"].get(), 0)
            self.settings.carton_height = to_float(vars_map["carton_height"].get(), 0)
            self.settings.bags_per_carton = to_float(vars_map["bags_per_carton"].get(), 0)
            self.settings.carton_base_price = to_float(vars_map["carton_base_price"].get(), 0)
            self.settings.carton_formula = vars_map["carton_formula"].get().strip().upper()
            if self.settings.carton_formula:
                try:
                    evaluate_carton_formula(
                        self.settings.carton_formula,
                        self.settings.carton_length,
                        self.settings.carton_width,
                        self.settings.carton_height,
                        self.settings.carton_base_price,
                    )
                except Exception as exc:
                    messagebox.showwarning("Cong thuc carton", f"Cong thuc chua hop le:\n{exc}")
                    return
            self._save_settings()
            self._refresh_settings_labels()
            dlg.destroy()

        ttk.Button(dlg, text="Luu", command=save).grid(row=row_formula + 4, column=1, sticky="e", padx=10, pady=10)
        ttk.Button(dlg, text="Dong", command=dlg.destroy).grid(row=row_formula + 4, column=1, sticky="w", padx=10, pady=10)
        dlg.columnconfigure(1, weight=1)

    def _calc_carton_cost(self) -> float:
        l = self.settings.carton_length
        w = self.settings.carton_width
        h = self.settings.carton_height
        base = self.settings.carton_base_price
        bags_per_carton = self.settings.bags_per_carton
        if self.settings.carton_formula:
            try:
                val = evaluate_carton_formula(self.settings.carton_formula, l, w, h, base)
                carton_total = float(val)
                return carton_total / bags_per_carton if bags_per_carton > 0 else carton_total
            except Exception:
                carton_total = default_carton_price(l, w, h, base)
                return carton_total / bags_per_carton if bags_per_carton > 0 else carton_total
        carton_total = default_carton_price(l, w, h, base)
        return carton_total / bags_per_carton if bags_per_carton > 0 else carton_total

    def _run_step3(self) -> None:
        if self.df_step2.empty:
            messagebox.showwarning("Step 3", "Can tinh step 2 truoc.")
            return
        self.estimated_qty = self._resolve_quote_qty()
        material_cost_total = float(self.df_step2["tong_chi_phi"].sum())
        warnings: list[str] = []
        if self.settings.pricing_setup_done:
            processing = self.settings.processing_cost_vnd
            processing_named = self._parse_processing_named()
            margin = self.settings.profit_rate
            if (self.settings.extra_cost_mode or "direct") == "tiered":
                tiers = self._parse_extra_cost_tiers()
                matched_rate = 0.0
                qty = int(self.estimated_qty or 0)
                for t in tiers:
                    if int(t["min_qty"]) <= qty <= int(t["max_qty"]):
                        matched_rate = float(t["rate"])
                        break
                extra_rate = matched_rate
                if tiers and matched_rate <= 0 and qty > 0:
                    warnings.append("Extra cost mode so khop: khong tim thay nguong phu hop, extra = 0.")
            else:
                extra_rate = self.settings.extra_cost_rate
        else:
            processing = 0.0
            processing_named = []
            margin = 0.0
            extra_rate = 0.0
            warnings.append("Chua setup Gia + Cach tinh, cac thanh phan lien quan dat = 0.")

        carton_ready = (
            self.settings.carton_length > 0
            and self.settings.carton_width > 0
            and self.settings.carton_height > 0
            and self.settings.carton_base_price > 0
            and self.settings.bags_per_carton > 0
        )
        if carton_ready:
            carton = self._calc_carton_cost()
        else:
            carton = 0.0
            warnings.append("Thieu thong so carton, Carton / cai dat = 0.")
        processing_named_total = sum(float(it.get("value_vnd", 0)) for it in processing_named)
        cost_order = material_cost_total + processing + processing_named_total + carton
        if margin >= 1:
            messagebox.showwarning("Step 3", "Loi nhuan phai < 1.")
            return
        safe_price = cost_order * (1 + extra_rate)
        selling_order = safe_price / (1 - margin) if margin < 1 else 0
        delta_pct = ((selling_order - cost_order) / cost_order * 100) if cost_order else 0
        warn = "CANH BAO: chenh lech cost/selling lon." if delta_pct > 300 else ""
        fx = self.settings.fx_usd_vnd or 1

        for item in self.tree_summary.get_children():
            self.tree_summary.delete(item)

        rows = [
            ("Tong chi phi NPL", material_cost_total, material_cost_total / fx, "Tong (gia tinh * sldm)"),
            ("Processing / cai", processing, processing / fx, "Gia processing setup"),
            ("Carton / cai", carton, carton / fx, "Tinh theo carton setup"),
            ("Cost Price", cost_order, cost_order / fx, "Tong chi phi NPL + processing + chi phi dat ten + carton"),
            ("Safe Price", safe_price, safe_price / fx, f"Cost * (1 + extra={extra_rate:.2f})"),
            ("Selling Price / cai", selling_order, selling_order / fx, f"Cost/(1-loi_nhuan) | {warn}".strip()),
        ]
        for item in processing_named:
            rows.insert(
                2,
                (
                    norm_text(item.get("name", "Chi phi dat ten")),
                    float(to_float(item.get("value_vnd", 0), 0)),
                    float(to_float(item.get("value_vnd", 0), 0)) / fx,
                    "Chi phi setup theo ten",
                ),
            )
        for r in rows:
            is_missing = r[1] <= 0 and r[0] not in {"Processing / cai", "Carton / cai"}
            if r[0].startswith("Cost Price") or r[0] == "Safe Price":
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

        self.kpi_cost_var.set(f"Cost Price: {format_money(cost_order)} VND | {cost_order/fx:,.2f} USD")
        self.kpi_selling_var.set(f"Selling Price: {format_money(selling_order)} VND | {selling_order/fx:,.2f} USD")
        self.kpi_margin_var.set(
            f"Loi nhuan setup: {margin*100:.1f}% | Extra: {extra_rate*100:.1f}% | Delta cost->selling: {delta_pct:.1f}%"
        )
        self.kpi_warning_var.set(warn)
        total_qty = float(self.estimated_qty)

        self.summary_var.set(
            f"Bao gia hien tai tren so luong: {total_qty:,.0f} tui\n"
            f"Cost Price: {format_money(cost_order)} VND | {cost_order/fx:,.2f} USD\n"
            f"Safe Price: {format_money(safe_price)} VND | {safe_price/fx:,.2f} USD\n"
            f"Selling Price: {format_money(selling_order)} VND | {selling_order/fx:,.2f} USD\n"
            f"Ty le chenh cost->selling: {delta_pct:.1f}% {warn}"
        )
        if warnings:
            messagebox.showwarning("Step 3", "\n".join(warnings))
        self._save_step_cache(
            "step3",
            {
                "cost_price": cost_order,
                "selling_price": selling_order,
                "delta_pct": delta_pct,
                "summary_text": self.summary_var.get(),
            },
        )
        self._refresh_cache_status()
        self.tabs.select(self.tab_step3)
        self._highlight_step_button(3)
        self.status_var.set("Buoc 3 xong: da tong hop cost/selling va bang chi tiet.")

    def _resolve_quote_qty(self) -> int:
        manual = int(to_float(self.manual_qty_var.get(), 0) or 0)
        if manual > 0:
            self.manual_quote_qty = manual
            return manual
        if self.qty_from_h1 > 0:
            return self.qty_from_h1
        return int(self.estimated_qty or 0)

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
