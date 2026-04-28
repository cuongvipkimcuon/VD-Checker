import tkinter as tk
from tkinter import ttk

import check_npl_ton_am
import check_bom
import orderlist_emg_checker


class SuperAppLauncher:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Super APP - EMG + NPL + BOM")
        self.root.geometry("460x260")
        self.root.resizable(False, False)
        self._build_ui()

    def _build_ui(self) -> None:
        wrap = ttk.Frame(self.root, padding=20)
        wrap.pack(fill="both", expand=True)

        ttk.Label(
            wrap,
            text="Chọn tool cần mở",
            font=("Segoe UI", 12, "bold"),
        ).pack(anchor="w", pady=(0, 14))

        ttk.Button(
            wrap,
            text="Orderlist Checker",
            command=self.open_emg_checker,
            width=38,
        ).pack(anchor="w", pady=(0, 8))

        ttk.Button(
            wrap,
            text="Material Checker",
            command=self.open_npl_checker,
            width=38,
        ).pack(anchor="w", pady=(0, 8))

        ttk.Button(
            wrap,
            text="Check BOM",
            command=self.open_check_bom,
            width=38,
        ).pack(anchor="w")

        ttk.Label(
            wrap,
            text="App sẽ đóng menu này và mở tool tương ứng.",
            foreground="#666666",
        ).pack(anchor="w", pady=(14, 0))

    def open_emg_checker(self) -> None:
        self.root.destroy()
        orderlist_emg_checker.main(back_to_launcher=main)

    def open_npl_checker(self) -> None:
        self.root.destroy()
        check_npl_ton_am.main(back_to_launcher=main)

    def open_check_bom(self) -> None:
        self.root.destroy()
        check_bom.main(back_to_launcher=main)


def main() -> None:
    root = tk.Tk()
    SuperAppLauncher(root)
    root.mainloop()


if __name__ == "__main__":
    main()
