from user_code.utils.mail.dynamic_matplotlib_screenshot_maker import DynamicMatplotlibScreenshotMaker
import matplotlib.pyplot as plt
import io
import base64
import datetime


class BasicColumnGraphics(DynamicMatplotlibScreenshotMaker):
    def __init__(
        self,
        graph_title: str = 'Динамика в процентах',
        x_axes_name: str = 'XAxes',
        y_axes_name: str = 'YAxes',
        target: int | None = None,
        show_tooltips: bool = True,
        y_axes_min: int | None = None,
        y_axes_max: int | None = None,
        height: int = 400,
        width: int = 1920,
        fields: list[dict] | None = None,
        items: list[dict] | None = None,
    ):
        super().__init__()
        self.graph_title = graph_title
        self.x_axes_name = x_axes_name
        self.y_axes_name = y_axes_name
        self.target = target
        self.show_tooltips = show_tooltips
        self.y_axes_min = y_axes_min
        self.y_axes_max = y_axes_max
        self.height = height
        self.width = width
        # Defaults из PHP-кода
        self.fields = fields or [
            {'title': 'Два', 'field': 'two', 'type': 'label'},
            {'title': 'Три', 'field': 'three'},
            {'title': 'Один', 'field': 'one'},
        ]
        self.items = items or [
            {'one': '1', 'two': '2', 'three': '3'},
            {'one': '4', 'two': '5', 'three': '6'},
            {'one': '7', 'two': '8', 'three': '9'},
        ]
        self._prepared = False

    def make_preparation(self) -> None:
        """Подготовка меток (labels) и наборов данных (datasets)"""
        self.labels: list[str] = []
        self.datasets: list[dict] = []

        for field in self.fields:
            if field.get('type') == 'label':
                raw = [item[field['field']] for item in self.items]
                # если поле date, форматируем
                if field['field'] == 'date':
                    dates = [datetime.datetime.strptime(r, '%Y-%m-%d') for r in raw]
                    self.labels = [d.strftime('%d.%m') for d in dates]
                else:
                    self.labels = raw
                self.x_axes_name = field['title']
            else:
                data = []
                for item in self.items:
                    val = float(item.get(field['field'], 0))
                    if 'round' in field:
                        val = round(val, field['round'])
                    data.append(val)
                self.datasets.append({
                    'name': field['title'],
                    'data': data,
                })

        self._prepared = True

    def render_graphics(self) -> bytes:
        """
        Рисует столбчатый график и возвращает байты PNG.
        """
        if not self._prepared:
            self.make_preparation()

        dpi = 100
        fig, ax = plt.subplots(
            figsize=(self.width / dpi, self.height / dpi),
            dpi=dpi
        )

        x = range(len(self.labels))
        n = len(self.datasets)
        bar_width = 0.8 / max(n, 1)

        # рисуем столбцы
        for i, ds in enumerate(self.datasets):
            offsets = [xi + i * bar_width for xi in x]
            ax.bar(offsets, ds['data'], width=bar_width, label=ds['name'])

        # оформление
        ax.set_title(self.graph_title)
        ax.set_xlabel(self.x_axes_name)
        ax.set_ylabel(self.y_axes_name)
        ax.set_xticks([xi + bar_width * (n - 1) / 2 for xi in x])
        ax.set_xticklabels(self.labels)

        # лимиты Y-оси
        if self.y_axes_min is not None or self.y_axes_max is not None:
            ax.set_ylim(self.y_axes_min, self.y_axes_max)

        # линия цели
        if self.target is not None:
            ax.axhline(self.target, linestyle='--', label='цель')

        ax.legend()
        fig.tight_layout()

        # сохраняем в буфер
        buf = io.BytesIO()
        fig.savefig(buf, format='png', dpi=dpi)
        plt.close(fig)
        buf.seek(0)
        return buf.getvalue()

    def get_graphics_base64(self) -> str:
        """
        Возвращает PNG-изображение графика закодированное в Base64.
        """
        img_bytes = self.render_graphics()
        return base64.b64encode(img_bytes).decode('ascii')