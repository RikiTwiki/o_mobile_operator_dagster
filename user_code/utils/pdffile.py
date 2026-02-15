from datetime import datetime
import os
from matplotlib.backends.backend_pdf import PdfPages
import requests
from io import BytesIO
import matplotlib.pyplot as plt

class PDFReport:
    """
    Утилитарный класс для по‑страничной сборки PDF с фиксированным размером 1920x1080 (в пикселях).
    """
    def __init__(
        self,
        output_dir: str = "reports",
        prefix: str = "report",
        report_date: datetime | None = None,
        default_dpi: int = 100,
        text_scale: float = 1.0,
        figure_scale: float = 1.5,
    ):
        os.makedirs(output_dir, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.path = os.path.join(output_dir, f"{prefix}_{ts}.pdf")
        self._pdf = PdfPages(self.path)

        self.report_date = report_date or datetime.now()
        self.Subject: str = ""
        self.header_title = ""
        self.header_type = "nod"
        self.header_config: dict = {}
        self.greet: str = ""
        self.contents: list[tuple[str, str, str | None]] = []

        self.figure_scale = figure_scale

        self.default_dpi = default_dpi
        self.default_figsize = (1920 / default_dpi, 1080 / default_dpi)  # 1920x1080 px
        self.text_scale = text_scale

    @property
    def formatted_report_date(self) -> str:
        return self.report_date.strftime("%d.%m.%Y")

    @property
    def subject(self) -> str:
        return self.Subject or f"{self.formatted_report_date} | {self.header_title}" or 'Report'

    @subject.setter
    def subject(self, value: str) -> None:
        self.Subject = value

    def add_figure_with_header(self, fig=None):
        if fig is None:
            fig = plt.figure(figsize=self.default_figsize, dpi=self.default_dpi)
        else:
            fig.set_size_inches(*self.default_figsize)

        w, h = fig.get_size_inches()
        fig.set_size_inches(w * self.figure_scale,
                            h * self.figure_scale)
        fig.tight_layout(pad=0.2)

        header_h = 0.20
        ax_header = fig.add_axes([0, 1 - header_h, 1, header_h], label="header")
        self._render_plot(ax_header)
        self._pdf.savefig(fig)
        plt.close(fig)

    def set_basic_header(self, title: str, type_: str = "nod") -> None:
        config = {
            'nod': {
                'logo_url': "https://cca.o.kg/logo/logo.png",
                'logo_width': 50,
                'h1': "NUR OPEN DATA",
                'h1_width': 130
            },
            'saima': {
                'logo_url': "https://cca.o.kg/logo/logo_saima.png",
                'logo_width': 180,
                'h1': "",
                'h1_width': 0
            }
        }[type_]
        self.header_title = title
        self.header_type = type_
        self.header_config = config

        hour = datetime.now().hour
        if 0 <= hour < 6:
            self.greet = 'ЗДРАВСТВУЙТЕ!'
        elif 6 <= hour < 11:
            self.greet = 'ДОБРОЕ УТРО!'
        elif 11 <= hour < 17:
            self.greet = 'ДОБРЫЙ ДЕНЬ!'
        else:
            self.greet = 'ДОБРЫЙ ВЕЧЕР!'

    def add_block_header(self, name: str) -> None:
        self.contents.append(("block", name, None))

    def add_module_header(self, name: str, target: str = "") -> None:
        self.contents.append(("module", name, target or None))

    def add_module_child_header(self, name: str, target: str = "") -> None:
        self.contents.append(("moduleChild", name, target or None))

    def add_module_child_title_header(self, name: str, target: str = "") -> None:
        self.contents.append(("moduleChildTitle", name, target or None))

    def add_text(self, text: str) -> None:
        self.contents.append(("text", text, None))

    def _render_plot(self, ax) -> None:
        ax.axis('off')
        cfg = self.header_config or {'logo_url': None, 'logo_width': 0, 'h1': '', 'h1_width': 0}
        fig = ax.figure
        total_width_px = fig.get_figwidth() * fig.dpi

        logo_norm = cfg.get('logo_width', 0) / total_width_px
        h1_norm = cfg.get('h1_width', 0) / total_width_px
        y_mid = 0.6


        if cfg.get('logo_url'):
            try:
                resp = requests.get(cfg['logo_url'], verify=False)
                resp.raise_for_status()
                img = plt.imread(BytesIO(resp.content), format='png')
                logo_width = cfg['logo_width'] / total_width_px
                x_logo = 0.5 - (h1_norm + logo_width + 0.02) / 2
                ax.imshow(
                    img,
                    extent=(x_logo, x_logo + logo_width, y_mid, y_mid + 0.15),
                    transform=ax.transAxes,
                    aspect='auto',
                    zorder=1,
                )
            except Exception:
                pass

        if cfg.get('h1') and cfg.get('h1_width'):
            x_h1 = 0.5 - (h1_norm + logo_norm + 0.02) / 2 + logo_norm + 0.02
            ax.text(
                x_h1,
                y_mid + 0.15,
                cfg['h1'],
                transform=ax.transAxes,
                ha='center', va='bottom',
                fontsize=16 * self.text_scale, weight='bold', zorder=2
            )

        ax.text(
            0.5,
            y_mid,
            self.greet,
            transform=ax.transAxes,
            ha='center', va='center', fontsize=14 * self.text_scale, zorder=2
        )
        ax.text(
            0.5,
            y_mid - 0.05,
            self.header_title,
            transform=ax.transAxes,
            ha='center', va='center', fontsize=18 * self.text_scale, weight='bold', zorder=2
        )

        y_text = y_mid - 0.15
        for typ, text, _ in self.contents:
            if y_text < 0.1:
                break
            ax.text(
                0.5,
                y_text,
                text,
                transform=ax.transAxes,
                ha='center', va='center', fontsize=12 * self.text_scale,
            )
            y_text -= 0.06

    def add_figure(self, fig=None):
        if fig is None:
            fig = plt.figure(figsize=self.default_figsize, dpi=self.default_dpi)
        else:
            fig.set_size_inches(*self.default_figsize)

        w, h = fig.get_size_inches()
        fig.set_size_inches(w * self.figure_scale,
                            h * self.figure_scale)
        fig.tight_layout(pad=0.2)

        self._pdf.savefig(fig)
        plt.close(fig)

    def add_text_page(self):
        fig, ax = plt.subplots(figsize=self.default_figsize, dpi=self.default_dpi)
        self._render_plot(ax)
        self._pdf.savefig(fig)
        plt.close(fig)
        self.contents.clear()

    def close(self):
        self._pdf.close()
