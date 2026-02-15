from datetime import datetime
import os
from matplotlib.backends.backend_pdf import PdfPages
import requests
from io import BytesIO
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle

class PDFReport2:
    """
    Утилитарный класс для постраничной сборки PDF через matplotlib.
    Создаёт страницы 1920×1080, сохраняет фигуры как есть, без преобразований.
    """
    FIGSIZE = (19.2, 10.8)  # 1920x1080 при dpi=100
    DPI = 100

    def __init__(self, output_dir: str = "reports", prefix: str = "report", report_date: datetime | None = None):
        os.makedirs(output_dir, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.path = os.path.join(output_dir, f"{prefix}_{ts}.pdf")
        self._pdf = PdfPages(self.path)

        self.report_date = report_date or datetime.now()
        self.Subject = ""

        self.header_title = ""
        self.header_type = "nod"
        self.header_config: dict = {}
        self.greet: str = ""

        self.contents: list[tuple[str, str, str | None]] = []

    @property
    def formatted_report_date(self) -> str:
        return self.report_date.strftime("%d.%m.%Y")

    @property
    def subject(self) -> str:
        return self.Subject or f"{self.formatted_report_date} | {self.header_title}" or 'Report'

    @subject.setter
    def subject(self, value: str) -> None:
        self.Subject = value

    def add_figure_with_header(self, fig):
        # 1) Устанавливаем фиксированную ширину…
        orig_w, orig_h = fig.get_size_inches()
        target_w = self.FIGSIZE[0]
        aspect = orig_h / orig_w
        # 2) Высоту либо подгоняем по соотношению сторон…
        target_h = target_w * aspect
        #    либо ограничиваем max-высоту:
        # target_h = min(target_h, self.FIGSIZE[1])
        fig.set_size_inches(target_w, target_h, forward=True)

        # 3) Рисуем заголовок…
        header_h = 0.20
        ax_header = fig.add_axes([0, 1 - header_h, 1, header_h], label="header", zorder=10)
        self._render_plot(ax_header, is_title_page=False)

        # 4) Сохраняем
        self._pdf.savefig(fig, dpi=self.DPI, bbox_inches="tight")
        plt.close(fig)

    def set_basic_header(
            self,
            title: str,
            type_: str = "nod",
            logo_width: int | None = None,
            h1_width: int | None = None
    ) -> None:
        print("[PDFReport.set_basic_header] Called with:")
        print(f"  title      = {title}")
        print(f"  type_      = {type_}")
        print(f"  logo_width = {logo_width}")
        print(f"  h1_width   = {h1_width}")
        config_map = {
            'nod': {
                'logo_url': "https://cca.o.kg/logo/logo.png",
                'logo_width': 200,
                'h1': "",
                'h1_width': 220
            },
            'saima': {
                'logo_url': "https://cca.o.kg/logo/logo_saima.png",
                'logo_width': 180,
                'h1': "",
                'h1_width': 0
            }
        }

        base_config = config_map[type_]
        config = {
            'logo_url': base_config['logo_url'],
            'logo_width': logo_width if logo_width is not None else base_config['logo_width'],
            'h1': base_config['h1'],
            'h1_width': h1_width if h1_width is not None else base_config['h1_width'],
        }

        self.header_title = title.strip().upper()
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



    def _render_plot(self, ax, is_title_page=False) -> None:
        ax.axis('off')
        cfg = self.header_config or {'logo_url': None, 'logo_width': 0, 'h1': '', 'h1_width': 0}
        fig = ax.figure
        total_width_px = fig.get_figwidth() * fig.dpi
        logo_norm = cfg.get('logo_width', 0) / total_width_px
        h1_norm = cfg.get('h1_width', 0) / total_width_px

        y_mid = 0.65
        logo_height = 0.12

        # ✅ Добавлять логотип только на титульной странице
        if is_title_page and cfg.get('logo_url'):
            try:
                resp = requests.get(cfg['logo_url'], verify=False)
                resp.raise_for_status()
                img = plt.imread(BytesIO(resp.content), format='png')

                img_ratio = img.shape[0] / img.shape[1]  # height / width
                logo_height = logo_norm * img_ratio

                # Левый верхний угол
                x_logo = 0.01
                y_logo = 0.88

                ax.imshow(
                    img,
                    extent=(x_logo, x_logo + logo_norm, y_logo, y_logo + logo_height),
                    transform=ax.transAxes,
                    aspect='auto',
                    zorder=1,
                )
            except Exception as e:
                print(f"[PDFReport2] Не удалось загрузить логотип: {e}")

        # Текст
        ax.text(0.5, y_mid - 0.02, self.greet, transform=ax.transAxes,
                ha='center', va='center', fontsize=14, zorder=2)
        ax.text(0.5, y_mid - 0.08, self.header_title, transform=ax.transAxes,
                ha='center', va='center', fontsize=18, weight='bold', zorder=2)

        y_text = y_mid - 0.18
        for typ, text, _ in self.contents:
            if y_text < 0.1:
                break
            ax.text(0.5, y_text, text, transform=ax.transAxes,
                    ha='center', va='center', fontsize=12)
            y_text -= 0.06

    def add_figure(self, fig):
        # то же самое для чистых фигур без заголовка
        orig_w, orig_h = fig.get_size_inches()
        target_w = self.FIGSIZE[0]
        aspect = orig_h / orig_w
        target_h = target_w * aspect
        fig.set_size_inches(target_w, target_h, forward=True)

        self._pdf.savefig(fig, dpi=self.DPI, bbox_inches="tight")
        plt.close(fig)

    def add_text_page(self):
        fig = plt.figure(figsize=self.FIGSIZE, dpi=self.DPI)
        ax = fig.add_subplot(111)
        self._render_plot(ax, is_title_page=True)  # ✅ указать, что титульная
        self._pdf.savefig(fig)
        plt.close(fig)
        self.contents.clear()

    def close(self):
        self._pdf.close()

    def _draw_section_header(
            self,
            fig,
            headers: list[dict],
            # Список заголовков: [{'number': '2', 'title': 'Статистика...', 'color': '#e6007e'}, ...]
    ):
        dpi = fig.get_dpi()
        fig_width_in, fig_height_in = fig.get_size_inches()

        # --- Размеры ---
        bar_height_px = 50
        padding_px = 2
        total_height_px = len(headers) * (bar_height_px + padding_px)
        total_frac = total_height_px / (fig_height_in * dpi)

        # --- Отрисовка каждого слоя ---
        for i, header in enumerate(headers):
            bottom_px = i * (bar_height_px + padding_px)
            bottom_frac = bottom_px / (fig_height_in * dpi)
            height_frac = bar_height_px / (fig_height_in * dpi)

            ax = fig.add_axes([0, 1 - bottom_frac - height_frac, 1, height_frac], label=f"header_{i}")
            ax.axis("off")

            # Цвета
            base_color = header.get('bg', '#111111')
            stripe_color = header.get('color', '#e6007e')  # Левая полоса
            text_color = header.get('text_color', 'white')

            # Фон и полоса
            ax.add_patch(Rectangle((0, 0), 1, 1, transform=ax.transAxes, facecolor=base_color))
            ax.add_patch(Rectangle((0, 0), 0.07, 1, transform=ax.transAxes, facecolor=stripe_color))

            # Текст
            if 'number' in header:
                ax.text(0.035, 0.5, header['number'], color=text_color, weight='bold',
                        ha='center', va='center', transform=ax.transAxes, fontsize=14)
            if 'title' in header:
                ax.text(0.08, 0.5, header['title'], color=text_color, weight='bold',
                        ha='left', va='center', transform=ax.transAxes, fontsize=14)

        # --- Смещение остальных осей вниз ---
        for ax in fig.axes:
            if not ax.get_label().startswith("header_"):
                pos = ax.get_position()
                ax.set_position([
                    pos.x0,
                    pos.y0,
                    pos.width,
                    pos.height - total_frac
                ])





        ax.text(
            0.08, 0.5,
            title,
            color="white",
            weight='bold',
            ha='left', va='center',
            transform=ax.transAxes,
            fontsize=14,
            zorder=2
        )