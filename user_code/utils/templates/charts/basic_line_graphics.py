import matplotlib.pyplot as plt
import io
import base64
from datetime import datetime
import matplotlib.dates as mdates


class BasicLineGraphics:
    def __init__(
        self,
        graph_title: str = '',
        x_axes_name: str = '',
        y_axes_name: str = '',
        target: float | None = None,
        stroke: str = 'straight',  # 'straight' (step) or 'smooth'
        show_tooltips: bool = True,
        y_axes_min: float | None = None,
        y_axes_max: float | None = None,
        pivot_to_table: dict | bool = False,
        fields: list[dict] | None = None,
        items: list[dict] | None = None,
        width: int = 1920,
        height: int = 400,
    ):
        self.height = height
        self.width = width
        self.graph_title = graph_title
        self.x_axes_name = x_axes_name
        self.y_axes_name = y_axes_name
        self.target = target
        self.stroke = stroke
        self.show_tooltips = show_tooltips
        self.y_axes_min = y_axes_min
        self.y_axes_max = y_axes_max
        self.pivot_to_table = pivot_to_table
        self.width = width
        self.height = height
        # Defaults from PHP
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
        # If pivot requested, convert items to pivot table
        if self.pivot_to_table:
            cfg = self.pivot_to_table
            # unique dates and rows
            dates = sorted({item[cfg['date_field']] for item in self.items})
            rows = sorted({item[cfg['row_field']] for item in self.items})
            # build new fields
            new_fields = [{'field': 'date', 'title': 'Дни', 'type': 'label'}]
            for r in rows:
                new_fields.append({'field': r, 'title': r})
            self.fields = new_fields
            # pivot items
            pivoted = []
            for d in dates:
                row = {'date': d}
                for r in rows:
                    # find matching item
                    val = None
                    for it in self.items:
                        if it.get(cfg['date_field']) == d and it.get(cfg['row_field']) == r:
                            val = it.get(cfg['value_field'])
                            break
                    row[r] = val
                pivoted.append(row)
            self.items = pivoted

        # now build labels and datasets
        self.labels = []
        self.datasets = []

        # --- ДОБАВЬ ЭТО: сортировка self.items по дате, если label = 'date'
        label_field_name = next((f['field'] for f in self.fields if f.get('type') == 'label'), None)

        def _parse_dt(s: str) -> datetime:
            """Пробует разные форматы даты/времени"""
            for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d-%m-%Y", "%d-%m-%Y %H:%M:%S"):
                try:
                    return datetime.strptime(s, fmt)
                except ValueError:
                    continue
            raise ValueError(f"Неизвестный формат даты: {s}")

        if label_field_name == 'date':
            self.items.sort(key=lambda it: _parse_dt(it['date']))
        for field in self.fields:
            if field.get('type') == 'label':
                raw = [item[field['field']] for item in self.items]
                if field['field'] == 'date':
                    # format dates
                    dates = []
                    for r in raw:
                        s = str(r).strip()
                        try:
                            dt = _parse_dt(s)
                        except ValueError:
                            dt = _parse_dt(s)
                        dates.append(dt)

                    self.label_datetimes = dates[:]

                    self.labels = [dt.strftime('%d.%m') for dt in dates]
                else:
                    self.labels = [str(r) for r in raw]
                self.x_axes_name = field['title']
            else:
                series = []
                for item in self.items:
                    val = item.get(field['field'], None)
                    if val is not None:
                        v = float(val)
                        if 'round' in field:
                            v = round(v, field['round'])
                        series.append(v)
                    else:
                        series.append(None)
                self.datasets.append({'name': field['title'], 'data': series})
        self._prepared = True

    def render_graphics(self) -> bytes:
        if not self._prepared:
            self.make_preparation()

        dpi = 100
        fig, ax = plt.subplots(
            figsize=(self.width / dpi, self.height / dpi),
            dpi=dpi
        )
        x = list(range(len(self.labels)))

        for ds in self.datasets:
            y = ds['data']
            if self.stroke == 'straight':
                ax.step(x, y, where='mid', label=ds['name'])
            else:
                ax.plot(
                    x, y,
                    marker='o' if self.show_tooltips else None,
                    label=ds['name'],
                    linestyle='-'
                )

        ax.set_title(self.graph_title)
        ax.set_xlabel(self.x_axes_name)
        ax.set_ylabel(self.y_axes_name)
        ax.set_xticks(x)
        ax.set_xticklabels(self.labels)

        if self.y_axes_min is not None or self.y_axes_max is not None:
            ax.set_ylim(self.y_axes_min, self.y_axes_max)

        if self.target is not None:
            ax.axhline(self.target, linestyle='--', label='цель')

        ax.legend()
        fig.tight_layout()

        buf = io.BytesIO()
        fig.savefig(buf, format='png', dpi=dpi)
        plt.close(fig)
        buf.seek(0)
        return buf.getvalue()

    def get_graphics_base64(self) -> str:
        img_bytes = self.render_graphics()
        return base64.b64encode(img_bytes).decode('ascii')

def color_weekend_xticklabels(ax):
    """Красим только подписи дат выходных на оси X."""
    from matplotlib import dates as _md  # локально и точно тот модуль

    ax.figure.canvas.draw()  # гарантируем, что метки построены
    ticks  = ax.get_xticks()
    labels = ax.get_xticklabels()

    for t, lab in zip(ticks, labels):
        try:
            dt = _md.num2date(float(t))
        except Exception:
            continue
        if dt.weekday() >= 5:
            lab.set_color('#6b7280')
            lab.set_fontweight('bold')