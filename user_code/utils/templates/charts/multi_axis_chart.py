import math

import numpy as np
from scipy.interpolate import make_interp_spline, PchipInterpolator
from datetime import datetime
import base64
from typing import List, Dict, Any, Optional
from matplotlib.ticker import FuncFormatter



def smart_number(x, digits=1, seperator=False, coerce_empty_to_zero=False):
    if coerce_empty_to_zero:
        # 1) Пустые/None/строки-None/NaN → 0
        if x is None:
            return 0
        if isinstance(x, str):
            s = x.strip()
            if not s or s.lower() in ("nan", "none", "null"):
                return 0
            s = s.replace("%", "").replace(",", ".")
            try:
                f = float(s)
            except (TypeError, ValueError):
                return 0
            if math.isnan(f):
                return 0
            # дальше — обычная логика форматирования
            x = f

        # числа/прочие → пытаемся к float
        try:
            f = float(x)
        except (TypeError, ValueError):
            return 0
        if math.isnan(f):
            return 0

        # если целое → int
        if f.is_integer():
            return int(f)
        # округление, если digits — int
        if isinstance(digits, int):
            return round(f, digits)
        return format_number(f) if seperator else f

    # ===== СТАРОЕ ПОВЕДЕНИЕ (без изменений) =====
    if x is None:
        return ''
    try:
        f = float(x)
    except (TypeError, ValueError):
        return x
    if math.isnan(f):
        return ''
    if f.is_integer():
        return int(f)
    if isinstance(digits, int):
        return round(f, digits)
    if seperator:
        return format_number(f)
    return f

def format_number(x):
    """
    Форматирует число с разделителем групп разрядов — пробелом.
    1000 -> "1 000"
    2345.5 -> "2 345.5"
    None/invalid -> "0"
    """
    try:
        f = float(x)
    except (TypeError, ValueError):
        return "0"
    sign = "-" if f < 0 else ""
    f_abs = abs(f)

    # Целое число
    if float(f_abs).is_integer():
        return sign + f"{int(f_abs):,}".replace(",", " ")
    # Дробное — показываем до 2 знаков (убираем лишние нули)
    s = f"{f_abs:.2f}".rstrip("0").rstrip(".")
    if "." in s:
        int_part, frac_part = s.split(".")
        int_part = f"{int(int_part):,}".replace(",", " ")
        return sign + int_part + "." + frac_part
    else:
        # на всякий случай
        int_part = f"{int(float(s)):,}".replace(",", " ")
        return sign + int_part


class MultiAxisChart:
    """
    Python-версия PHP-класса MultiAxisChart,
    наследуется от DynamicMatplotlibScreenshotMaker.
    Имплементация рендеринга в стиле Jivo.
    """
    def __init__(
            self,
            graph_title: str = "",
            x_axes_name: str = "",
            y_axes_name: str = "",
            font_size: Optional[int] = None,
            position: Optional[str] = None,
            target: Optional[int] = None,
            show_tooltips: bool = True,
            pointer: int = 5000,
            y_axes_min: Optional[int] = None,
            y_axes_max: Optional[int] = None,
            stack: Optional[bool] = None,
            execute_one_series: Optional[bool] = None,
            width: int = 1920,
            height: int = 500,
            dpi: int = 100,
            fields: Optional[List[Dict[str, Any]]] = None,
            items: Optional[List[Dict[str, Any]]] = None,
            is_marker: bool = False,
            dot = False,
            to_hundreds = True,
            base = 50
    ):
        self.width = width
        self.height = height
        self.dpi = dpi
        self.GraphTitle = graph_title
        self.XAxesName = x_axes_name
        self.YAxesName = y_axes_name
        self.FontSize = font_size
        self.Position = position
        self.Target = target
        self.ShowTooltips = show_tooltips
        self.Pointer = pointer
        self.YAxesMin = y_axes_min
        self.YAxesMax = y_axes_max
        self.Stack = stack
        self.ExecuteOneSeries = execute_one_series
        self.Fields = fields or []
        self.Items = items or []
        self.colors = ["#F0047F", "#C8C8C8", "#919191", "#32a932"]
        self._combine_cfg: Optional[Dict[str, Any]] = None
        self.Dot = dot
        self.to_hundreds = to_hundreds
        self.is_marker = is_marker
        self.base = base

    def _parse_fields(self):
        label_field = None
        bar_fields = []
        line_fields = []
        for f in self.Fields:
            t = f.get("type")
            ct = f.get("chartType")
            if t == "label" and label_field is None:
                label_field = f.get("field")
            elif ct in ("bar", "grouped_bars"):  # ← разрешаем grouped_bars
                bar_fields.append(f)
            elif ct == "line":
                line_fields.append(f)
        if not label_field:
            raise ValueError("Не найдено поле с type='label' в self.Fields")
        return label_field, bar_fields, line_fields

    def _prepare_data(self):
        """
        Готовим:
          - dates: список подписей оси X,
          - bar_vals: список списков значений для всех bar-серий,
          - line_vals: список списков значений для всех line-серий,
          - titles: заголовки серий (для легенды) в порядке bar + line.
        """
        label_field, bar_fields, line_fields = self._parse_fields()

        dates = []
        bar_vals = [[] for _ in bar_fields]
        line_vals = [[] for _ in line_fields]

        for row in self.Items:
            raw = row.get(label_field)
            # форматируем дату как в твоём коде
            dt = self._parse_date_any(raw)
            date_str = dt.strftime("%d.%m")
            dates.append(date_str)

            # значения для BAR-серий
            for i, f in enumerate(bar_fields):
                val = smart_number(
                    row.get(f["field"], 0),
                    digits=0,
                    coerce_empty_to_zero=True)
                bar_vals[i].append(int(val or 0))

            # значения для LINE-серий
            for j, f in enumerate(line_fields):
                line_vals[j].append(smart_number(row.get(f["field"], 0), coerce_empty_to_zero=True))

        titles = [f.get("title", "") for f in bar_fields + line_fields]
        return dates, bar_vals, line_vals, titles

    def _bars_mode(self) -> str:
        """Если в Fields есть хотя бы одно chartType='grouped_bars' — включаем новый режим."""
        return "grouped" if any(f.get("chartType") == "grouped_bars" for f in self.Fields) else "stacked"

    def _draw_grouped_bars(self, ax, x, dates, bar_vals, titles, colors=None):
        """Рисуем сгруппированные бар-серии (не стекаем). Возвращаем (bars_list, starts_list, vals_list, peak)."""
        n_groups = len(bar_vals)
        if n_groups == 0:
            return [], [], [], 0.0
        group_w = 0.9
        w = group_w / n_groups

        bars_list, starts_list, vals_list = [], [], []
        bar_max = 0.0

        for i, series in enumerate(bar_vals):
            offs = (i + 0.5) * w - group_w / 2
            xi = x + offs
            color_i = (colors[i] if colors is not None else self.colors[i % len(self.colors)])
            bars = ax.bar(xi, series, width=w, label=titles[i], color=color_i, zorder=1)
            bars_list.append(bars)
            starts_list.append(np.zeros(len(dates)))
            vals_list.append(series)
            if len(series):
                m = float(np.nanmax(series))
                if m > bar_max:
                    bar_max = m

        ax.set_xticks(x)
        ax.set_xticklabels(dates, rotation=45, fontsize=9)
        return bars_list, starts_list, vals_list, bar_max

    def render_graphics(self, ax):
        dates, bar_vals, line_vals, titles = self._prepare_data()
        label_field, _, _ = self._parse_fields()

        def _to_dt(v):
            try:
                return self._parse_date_any(v)
            except Exception:
                return datetime(1970, 1, 1)

        weekend_idx = [
            i for i, row in enumerate(self.Items)
            if _to_dt(row.get(label_field)).weekday() >= 5
        ]

        x = np.arange(len(dates))
        mode = self._bars_mode()

        # =========================
        # РЕЖИМ 1: grouped_bars
        # =========================
        if mode == "grouped":
            # левая ось — линия, правая — бары (фикс. шкала)
            ax_line = ax
            ax_bars = ax.twinx()

            # линия поверх баров
            ax_bars.set_zorder(1)
            ax_line.set_zorder(2)
            ax_line.patch.set_alpha(0.0)

            # --- БАРЫ (сгруппированные, не стек) ---
            bars_list, starts_list, vals_list, stacked_max = self._draw_grouped_bars(
                ax_bars, x, dates, bar_vals, titles, colors=None
            )

            # --- ЛИНИЯ ---
            line_max = 0.0
            for j, series in enumerate(line_vals):
                y = np.array(series, dtype=float)
                color = self.colors[(len(bar_vals) + j) % len(self.colors)]
                label = titles[len(bar_vals) + j]

                ax_to = ax_line
                if len(x) >= 4:
                    x_smooth = np.linspace(x.min(), x.max(), 2000)
                    pchip = PchipInterpolator(x, y)
                    y_smooth = pchip(x_smooth)
                    ax_to.plot(x_smooth, y_smooth, label=label, linewidth=2.5, color=color, zorder=8)
                    if self.is_marker:
                        ax_to.scatter(x, y, color=color, s=30, zorder=9)
                    line_max = max(line_max, float(np.nanmax(y_smooth)))
                    if self.Dot:
                        ax_to.plot(x, y, linestyle="", marker="o", markersize=3,
                                   color="black", markerfacecolor="black", zorder=10)
                    offset = float(np.nanmax(y_smooth)) * 0.03
                    for xi, yi in zip(x, y):
                        ax_to.text(xi, yi + offset, smart_number(yi, seperator=True),
                                   ha="center", va="bottom", fontsize=8,
                                   color="#4A4A4A", fontweight="bold", zorder=10)
                else:
                    ax_to.plot(x, y, label=label, linewidth=2.5, color=color, zorder=8)
                    if self.is_marker:
                        ax_to.scatter(x, y, color=color, s=30, zorder=9)
                    line_max = max(line_max, float(np.nanmax(y)))
                    if self.Dot:
                        ax_to.plot(x, y, linestyle="", marker="o", markersize=4,
                                   color="black", markerfacecolor="black", zorder=10)
                    offset = float(np.nanmax(y)) * 0.03
                    for xi, yi in zip(x, y):
                        ax_to.text(xi, yi + offset, smart_number(yi, seperator=True),
                                   ha="center", va="bottom", fontsize=8,
                                   color="#4A4A4A", fontweight="bold", zorder=10)

            # --- Target на левой оси ---
            if self.Target is not None:
                tgt = float(self.Target)
                ax_line.axhline(y=tgt, color="#3AA655", linewidth=2, linestyle="-", alpha=0.9, zorder=7)
                ax_line.text(0, tgt, "План", ha="left", va="center", fontsize=8, fontweight="bold",
                             color="white", zorder=9,
                             bbox=dict(boxstyle="round,pad=0.3", fc="#3AA655", ec="none", alpha=1.0),
                             transform=ax_line.get_yaxis_transform())

            # --- Подписи и оформление ---
            ax_line.set_title(self.GraphTitle, fontsize=12, fontweight="bold", loc="left")
            ax.set_xlabel(self.XAxesName, fontsize=12)
            ax_line.grid(axis="y", linestyle="--", linewidth=0.5)
            ax_line.set_axisbelow(True)


            # X-тиков рисуем на «ax» (общее)
            ax.set_xticks(x)
            ax.set_xlim(-0.5, len(dates) - 0.5)
            ax.set_xticklabels(dates, rotation=0, fontsize=7, ha="center")
            ax.tick_params(axis="x", which="both", length=0, pad=8)
            labels = ax.get_xticklabels()
            for i, lbl in enumerate(labels):
                lbl.set_fontweight("bold" if i in weekend_idx else "normal")


            ax.tick_params(axis="x", which="both", length=0)

            pad_ratio = 0.02
            ax_line.margins(x=pad_ratio)
            ax_bars.margins(x=pad_ratio)

            # прячем рамки
            for s in ax_line.spines.values():
                s.set_visible(False)
            for s in ax_bars.spines.values():
                s.set_visible(False)

            # --- Легенда (оба набора хэндлеров) ---
            h1, l1 = ax_line.get_legend_handles_labels()
            h2, l2 = ax_bars.get_legend_handles_labels()
            legend = ax_line.legend(h1 + h2, l1 + l2, loc="upper center",
                                    bbox_to_anchor=(0.5, 1.00), ncol=max(1, len(titles)),
                                    frameon=False, fontsize=10)
            for t in legend.get_texts():
                t.set_fontweight("normal");
                t.set_color("#4A4A4A")

            # --- ЛЕВАЯ ось (линия) — авто под данные + Target ---
            BASE = self.base;
            PAD_PCT = 0.25;
            PAD_MIN = 10
            ymax_line = max(line_max, float(self.Target) if self.Target is not None else 0.0)
            top_line = max(ymax_line * (1 + PAD_PCT), ymax_line + PAD_MIN)
            top_line = math.ceil(top_line / BASE) * BASE
            ax_line.set_ylim(0, top_line)

            y0, y1 = ax_line.get_ylim();
            span = y1 - y0
            base_l = 100 if span >= 1000 else (10 if span >= 100 else 1)
            ax_line.yaxis.set_major_formatter(FuncFormatter(lambda y, pos: format_number(round(y / base_l) * base_l)))

            # --- ПРАВАЯ ось (бары) — фиксированный диапазон ---
            if getattr(self, "_combine_cfg", None):
                min_r = float(self._combine_cfg.get("min", 0))
                max_r = float(self._combine_cfg.get("max", 300))
            else:
                min_r, max_r = 0.0, 300.0
            ax_bars.set_ylim(min_r, max_r)
            ax_bars.set_autoscale_on(False)
            ax_bars.margins(y=0)
            try:
                ax_bars.set_yticks(np.linspace(min_r, max_r, 6))
            except Exception:
                pass
            ax_bars.yaxis.set_major_formatter(FuncFormatter(lambda y, pos: format_number(int(round(y)))))
            ax_bars.set_ylabel("")  # без названия
            ax_bars.tick_params(axis="y",
                                which="both",
                                labelright=False, labelleft=False,  # не показывать числа
                                right=False, left=False)  # убрать риски
            if "right" in ax_bars.spines:
                ax_bars.spines["right"].set_visible(False)

            # --- подписи внутри баров по ПРАВОЙ шкале ---
            gap_inside = max((max_r - min_r) * 0.005, 0.5)
            gap_above = max((max_r - min_r) * 0.005, 0.25)
            for bars, starts, series in zip(bars_list, starts_list, vals_list):
                for bar, start, val in zip(bars, starts, series):
                    if val <= 0:
                        continue
                    height = val - start
                    if height > gap_inside + 1:
                        y_text, va = start + gap_inside, "bottom"
                    else:
                        y_text, va = val + gap_above, "bottom"
                    ax_bars.text(bar.get_x() + bar.get_width() / 2, y_text, format_number(val),
                                 ha="center", va=va, fontsize=6, color="black", fontweight="bold",
                                 zorder=2, clip_on=True)
            return

        # =========================
        # РЕЖИМ 2: старый (stacked) — БЕЗ ИЗМЕНЕНИЙ
        # =========================
        bar_width = 0.9
        bottom = np.zeros(len(dates), dtype=float)
        stacked_max = 0.0
        bars_list, starts_list, vals_list = [], [], []

        for i, series in enumerate(bar_vals):
            color = self.colors[i % len(self.colors)]
            starts = bottom.copy()
            bars = ax.bar(dates, series, bottom=starts, width=bar_width,
                          label=titles[i], color=color)
            bars_list.append(bars)
            starts_list.append(starts)
            vals_list.append(series)
            bottom += np.array(series, dtype=float)
            stacked_max = max(stacked_max, bottom.max())

        line_max = 0.0
        for j, series in enumerate(line_vals):
            y = np.array(series, dtype=float)
            color = self.colors[(len(bar_vals) + j) % len(self.colors)]
            label = titles[len(bar_vals) + j]
            if len(x) >= 4:
                x_smooth = np.linspace(x.min(), x.max(), 2000)
                pchip = PchipInterpolator(x, y)
                y_smooth = pchip(x_smooth)
                ax.plot(x_smooth, y_smooth, label=label, linewidth=2.5, color=color)
                if self.is_marker:
                    ax.scatter(x, y, color=color, s=30, zorder=3)
                line_max = max(line_max, float(y_smooth.max()))
                if self.Dot:
                    ax.plot(x, y, linestyle="", marker="o", markersize=3,
                            color="black", markerfacecolor="black", zorder=5)
                offset = max(stacked_max, y_smooth.max()) * 0.03
                for xi, yi in zip(x, y):
                    ax.text(xi, yi + offset, smart_number(yi, seperator=True),
                            ha="center", va="bottom", fontsize=8,
                            color="#4A4A4A", fontweight="bold")
            else:
                ax.plot(x, y, label=label, linewidth=2.5, color=color)
                if self.is_marker:
                    ax.scatter(x, y, color=color, s=30, zorder=3)
                line_max = max(line_max, float(y.max()))
                if self.Dot:
                    ax.plot(x, y, linestyle="", marker="o", markersize=4,
                            color="black", markerfacecolor="black", zorder=5)
                offset = max(stacked_max, y.max()) * 0.03
                for xi, yi in zip(x, y):
                    ax.text(xi, yi + offset, smart_number(yi, seperator=True),
                            ha="center", va="bottom", fontsize=8,
                            color="#4A4A4A", fontweight="bold")

        if self.Target is not None:
            tgt = float(self.Target)
            ax.axhline(y=tgt, color="#3AA655", linewidth=2, linestyle="-", alpha=0.9, zorder=2.5)
            ax.text(x=0, y=tgt, s="План", ha="left", va="center",
                    fontsize=8, fontweight="bold", color="white", zorder=4,
                    bbox=dict(boxstyle="round,pad=0.3", fc="#3AA655", ec="none", alpha=1.0),
                    transform=ax.get_yaxis_transform())

        ax.set_title(self.GraphTitle, fontsize=12, fontweight="bold", loc="left")
        ax.set_xlabel(self.XAxesName, fontsize=12)
        ax.set_ylabel(self.YAxesName, fontsize=12)
        ax.grid(axis="y", linestyle="--", linewidth=0.5)
        ax.set_axisbelow(True)

        ax.set_xticks(x)
        ax.set_xlim(-0.5, len(dates) - 0.5)
        ax.set_xticklabels(dates, rotation=0, fontsize=7, ha="center")
        ax.tick_params(axis="x", which="both", length=0, pad=8)
        labels = ax.get_xticklabels()
        for i, lbl in enumerate(labels):
            lbl.set_fontweight("bold" if i in weekend_idx else "normal")

        legend = ax.legend(loc="upper center", bbox_to_anchor=(0.5, 1.00),
                           ncol=max(1, len(titles)), frameon=False, fontsize=10)
        for text in legend.get_texts():
            text.set_fontweight("normal")
            text.set_color("#4A4A4A")
        for spine in ax.spines.values():
            spine.set_visible(False)

        ymax_candidate = max(stacked_max, line_max)
        label_gap = max(ymax_candidate * 0.015, 1.0)
        for bars, starts, series in zip(bars_list, starts_list, vals_list):
            for bar, start, val in zip(bars, starts, series):
                if val > 0:
                    ax.text(bar.get_x() + bar.get_width() / 2, start + label_gap,
                            format_number(val), ha="center", va="bottom",
                            fontsize=8, color="black", fontweight="bold")

        BASE = self.base;
        PAD_PCT = 0.25;
        PAD_MIN = 10
        ymax_candidate = max(stacked_max, line_max)
        if self.Target is not None:
            ymax_candidate = max(ymax_candidate, float(self.Target))
        if self.YAxesMax is not None:
            ymin_final = self.YAxesMin if self.YAxesMin is not None else 0
            ymax_final = self.YAxesMax
            if self.Target is not None:
                t = float(self.Target)
                ymin_final = min(ymin_final, t)
                ymax_final = max(ymax_final, t)
            ax.set_ylim(bottom=ymin_final, top=ymax_final)
        else:
            top_auto = max(ymax_candidate * (1 + PAD_PCT), ymax_candidate + PAD_MIN)
            top_auto = math.ceil(top_auto / BASE) * BASE
            ymin_final = self.YAxesMin if self.YAxesMin is not None else 0
            if self.Target is not None:
                ymin_final = min(ymin_final, float(self.Target))
            ax.set_ylim(bottom=ymin_final, top=top_auto)

        ymin, ymax = ax.get_ylim();
        span = ymax - ymin
        base = 100 if span >= 1000 else (10 if span >= 100 else 1)
        ax.yaxis.set_major_formatter(FuncFormatter(lambda y, pos: format_number(round(y / base) * base)))


    def get_graphics_in_base64(self) -> str:
        png_bytes = self.get_png(self.render_graphics)
        return base64.b64encode(png_bytes).decode('ascii')

    def set_colour_pack(self, pack_id: int):
        packs = {
            2: {
                "colors": ["#222d32", "#b8c7ce", "#e2007a"],
            },
            3: {
                "colors": ["#0779d6", "#FF0000", "#e2007a", "#49b6d6"],
                "dataLabels": {"style": {"colors": ["#000"]}},
            },
            4: {
                "colors": ["#C8C8C8", "#F0047F", "#A3A3A3", "#919191"],
                "dataLabels": {"style": {"colors": ["#000"]}},
            },
            5: {
                "colors": ["#C8C8C8", "#F0047F", "#5F5F5F", "#919191"],
                "dataLabels": {"style": {"colors": ["#000"]}},
            },
            6: {
                "colors": ["#F0047F", "#C8C8C8", "#919191", "#FF75AE", "#A50044"],
                "dataLabels": {"style": {"colors": ["#000"]}},
            },
        }

        if pack_id in packs:
            self.colors = packs[pack_id]["colors"]
            # Если дополнительно есть настройки dataLabels — сохраняем
            if "dataLabels" in packs[pack_id]:
                self.data_labels_style = packs[pack_id]["dataLabels"]["style"]["colors"]

    def combine(self, combine_id: int, max_percent: Optional[int] = None, min_percent: Optional[int] = None):
        self._combine_cfg = {
            'id': combine_id,
            'max': max_percent or 100,
            'min': min_percent or 0,
        }

    def get_png(self, render_graphics):
        pass

    def _parse_date_any(self, raw) -> datetime:
        """Принимает datetime/date или строку с датой в нескольких форматах.
        Возвращает datetime. Бросает ValueError, если ни один формат не подошёл."""
        if isinstance(raw, datetime):
            return raw
        # иногда прилетает date (без времени)
        try:
            from datetime import date as _date
            if isinstance(raw, _date):
                return datetime.combine(raw, datetime.min.time())
        except Exception:
            pass

        s = str(raw).strip()
        if not s:
            raise ValueError("Empty date string for label_field")

        fmts = (
            # ISO сначала
            "%Y-%m-%d %H:%M:%S", "%Y-%m-%d",
            # ДД-ММ-ГГГГ
            "%d-%m-%Y %H:%M:%S", "%d-%m-%Y",
            # ДД.ММ.ГГГГ
            "%d.%m.%Y %H:%M:%S", "%d.%m.%Y",
            # Запасы (на всякий)
            "%Y.%m.%d %H:%M:%S", "%Y.%m.%d",
            "%Y/%m/%d %H:%M:%S", "%Y/%m/%d",
            "%d/%m/%Y %H:%M:%S", "%d/%m/%Y",
        )
        for fmt in fmts:
            try:
                return datetime.strptime(s, fmt)
            except ValueError:
                continue
        raise ValueError(f"Unsupported date format for label_field: {s!r}")