# -*- coding: utf-8 -*-
from __future__ import annotations

import io
import math
import base64
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from scipy.interpolate import PchipInterpolator


# ---------- утилиты форматирования чисел ----------

def _format_number(x: float) -> str:
    """Число с разделителем тысяч (пробел)."""
    try:
        f = float(x)
    except (TypeError, ValueError):
        return "0"
    sign = "-" if f < 0 else ""
    f_abs = abs(f)
    if float(f_abs).is_integer():
        return sign + f"{int(f_abs):,}".replace(",", " ")
    s = f"{f_abs:.2f}".rstrip("0").rstrip(".")
    if "." in s:
        int_part, frac_part = s.split(".")
        int_part = f"{int(int_part):,}".replace(",", " ")
        return sign + int_part + "." + frac_part
    int_part = f"{int(float(s)):,}".replace(",", " ")
    return sign + int_part


def _smart_number(x, *, digits: int = 1, coerce_empty_to_zero: bool = False):
    if coerce_empty_to_zero:
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
            x = f
        try:
            f = float(x)
        except (TypeError, ValueError):
            return 0
        if math.isnan(f):
            return 0
        if f.is_integer():
            return int(f)
        return round(f, digits)
    # без принудительного приведения к 0
    if x is None:
        return ""
    try:
        f = float(x)
    except (TypeError, ValueError):
        return x
    if math.isnan(f):
        return ""
    if f.is_integer():
        return int(f)
    return round(f, digits)


# ------------------------------ КЛАСС ------------------------------

class GTMMultiAxisChart:
    """
    Matplotlib-порт PHP-класса GTMMultiAxisChart.

    Ключевая семантика:
      - Fields: один label + >=0 bar (или grouped_bars) + >=0 line
      - Combine(id, ...) — диапазон правой оси (баров):
          id=2 -> [0..max_percent]; id=3 -> [0..100]; id=4 -> [min_percent..max_percent]
      - HideRightAxis -> полностью скрывает правую ось (подписи/риски/спайн)
      - Target -> горизонтальная линия "цель"
      - Stack -> если True и нет grouped_bars, бары рисуются стекингом
    """

    def __init__(
        self,
        graph_title: str = "",
        x_axes_name: str = "XAxes",
        y_axes_name: str = "YAxes",
        font_size: Optional[int] = None,
        position: Optional[str] = None,
        target: Optional[int] = None,
        show_tooltips: bool = True,     # не используется в matplotlib, оставлено для совместимости
        pointer: int = 5000,
        tooltip_size: int = 3,          # не используется напрямую
        y_axes_min: Optional[int] = None,
        y_axes_max: Optional[int] = None,
        stack: Optional[bool] = None,
        hide_right_axis: bool = False,
        execute_one_series: Optional[bool] = None,
        height: int = 500,
        width: int = 1920,
        dpi: int = 100,
        fields: Optional[List[Dict[str, Any]]] = None,
        items: Optional[List[Dict[str, Any]]] = None,
        dot: bool = False,
        is_marker: bool = False,
        base: int = 50,
    ):
        self.GraphTitle = graph_title
        self.XAxesName = x_axes_name
        self.YAxesName = y_axes_name
        self.FontSize = font_size
        self.Position = position
        self.Target = target
        self.ShowTooltips = show_tooltips

        self.Pointer = pointer
        self.TooltipSize = tooltip_size
        self.YAxesMin = y_axes_min
        self.YAxesMax = y_axes_max
        self.Stack = stack
        self.HideRightAxis = hide_right_axis
        self.ExecuteOneSeries = execute_one_series

        self.Height = height
        self.Width = width
        self.dpi = dpi

        self.Fields = fields or []
        self.Items = items or []

        self.colors = ["#F0047F", "#C8C8C8", "#919191", "#32a932"]
        self._combine_cfg: Optional[Dict[str, float]] = None

        self.Dot = dot
        self.is_marker = is_marker
        self.base = base

    # ---------- публичные API (совместимость с PHP-именами) ----------

    def setColourPack(self, id: int) -> None:
        if id == 2:
            self.colors = ["#222d32", "#b8c7ce", "#e2007a"]
        elif id == 3:
            self.colors = ["#0779d6", "#FF0000", "#e2007a", "#49b6d6"]
        elif id == 4:
            self.colors = ["#919191", "#F0047F", "#C8C8C8", "#5F5F5F"]
        elif id == 5:
            self.colors = ["#C8C8C8", "#F0047F", "#5F5F5F", "#919191"]
        elif id == 6:
            self.colors = ["#F0047F", "#C8C8C8", "#919191", "#FF75AE", "#A50044"]

    def Combine(
        self,
        id: int,
        max_percent: Optional[int] = None,
        min_percent: Optional[int] = None,
        text: str = "Проценты",
    ) -> None:
        # фиксируем диапазон правой оси (для баров)
        if id == 2:
            self._combine_cfg = {"min": float(min_percent if min_percent is not None else 0), "max": float(max_percent or 300)}
        elif id == 3:
            self._combine_cfg = {"min": 0.0, "max": 100.0}
        elif id == 4:
            self._combine_cfg = {
                "min": float(min_percent if min_percent is not None else 100),
                "max": float(max_percent if max_percent is not None else 100),
            }
        else:
            self._combine_cfg = {"min": 0.0, "max": 100.0}

    def set_items(self, items: List[Dict[str, Any]]) -> "GTMMultiAxisChart":
        self.Items = items or []
        return self

    def set_fields(self, fields: List[Dict[str, Any]]) -> "GTMMultiAxisChart":
        self.Fields = fields or []
        return self

    # ---------- подготовка данных ----------

    def _parse_fields(self) -> Tuple[str, List[Dict[str, Any]], List[Dict[str, Any]]]:
        label_field = None
        bar_fields: List[Dict[str, Any]] = []
        line_fields: List[Dict[str, Any]] = []
        for f in self.Fields:
            t = f.get("type")
            ct = f.get("chartType") or f.get("type")
            if t == "label" and label_field is None:
                label_field = f.get("field")
            elif ct in ("bar", "grouped_bars"):
                bar_fields.append(f)
            elif ct == "line":
                line_fields.append(f)
        if not label_field:
            raise ValueError("Не найдено поле с type='label' в Fields")
        return label_field, bar_fields, line_fields

    def _parse_date_any(self, raw) -> datetime:
        if isinstance(raw, datetime):
            return raw
        try:
            from datetime import date as _date
            if isinstance(raw, _date):
                return datetime.combine(raw, datetime.min.time())
        except Exception:
            pass
        s = str(raw).strip()
        fmts = (
            "%Y-%m-%d %H:%M:%S", "%Y-%m-%d",
            "%d-%m-%Y %H:%M:%S", "%d-%m-%Y",
            "%d.%m.%Y %H:%M:%S", "%d.%m.%Y",
            "%Y.%m.%d %H:%M:%S", "%Y.%m.%d",
            "%Y/%m/%d %H:%M:%S", "%Y/%m/%d",
            "%d/%m/%Y %H:%M:%S", "%d/%m/%Y",
        )
        for fmt in fmts:
            try:
                return datetime.strptime(s, fmt)
            except ValueError:
                continue
        # fallback: ISO-like
        try:
            return datetime.fromisoformat(s[:10])
        except Exception:
            raise ValueError(f"Unsupported date format: {s!r}")

    def _prepare(self):
        label_field, bar_fields, line_fields = self._parse_fields()

        dates: List[str] = []
        x_vals = []  # для выходных
        bar_vals = [[] for _ in bar_fields]
        line_vals = [[] for _ in line_fields]

        for row in self.Items:
            raw = row.get(label_field)
            dt = self._parse_date_any(raw)
            dates.append(dt.strftime("%d.%m"))
            x_vals.append(dt)

            for i, f in enumerate(bar_fields):
                v = _smart_number(row.get(f["field"], 0), digits=0, coerce_empty_to_zero=True)
                bar_vals[i].append(int(v or 0))

            for j, f in enumerate(line_fields):
                v = _smart_number(row.get(f["field"], 0), digits=1, coerce_empty_to_zero=True)
                line_vals[j].append(float(v))

        titles = [f.get("title", "") for f in bar_fields + line_fields]

        weekend_idx = [i for i, dt in enumerate(x_vals) if dt.weekday() >= 5]
        mode = "grouped" if any((f.get("chartType") == "grouped_bars") for f in bar_fields) else ("stacked" if self.Stack else "grouped" if len(bar_fields) > 1 else "stacked")

        return {
            "dates": dates,
            "bar_fields": bar_fields,
            "line_fields": line_fields,
            "bar_vals": bar_vals,
            "line_vals": line_vals,
            "titles": titles,
            "weekend_idx": weekend_idx,
            "mode": mode,
        }

    # ---------- рендер ----------

    def render_graphics(self, ax) -> None:
        cfg = self._prepare()
        dates = cfg["dates"]
        bar_fields = cfg["bar_fields"]
        line_fields = cfg["line_fields"]
        bar_vals = cfg["bar_vals"]
        line_vals = cfg["line_vals"]
        titles = cfg["titles"]
        weekend_idx = cfg["weekend_idx"]
        mode = cfg["mode"]

        x = np.arange(len(dates))

        if mode == "grouped":
            self._render_grouped(ax, x, dates, bar_vals, line_vals, titles, weekend_idx)
        else:
            self._render_stacked(ax, x, dates, bar_vals, line_vals, titles, weekend_idx)

    def _render_grouped(self, ax, x, dates, bar_vals, line_vals, titles, weekend_idx):
        """
        ЛЕВАЯ ось (ax_bars) — бары с количествами.
        ПРАВАЯ ось (ax_line) — линия конверсии в процентах (Combine()).
        """
        # --- оси ---
        ax_bars = ax  # левая ось — бары
        ax_line = ax.twinx()  # правая ось — линия

        # чтобы линия была поверх баров
        ax_bars.set_zorder(1)
        ax_line.set_zorder(2)
        ax_line.patch.set_alpha(0.0)

        # --- бары (сгруппированные) ---
        n_groups = max(1, len(bar_vals))
        group_w = 0.9
        w = group_w / n_groups

        bars_list, starts_list, vals_list = [], [], []
        bar_max = 0.0

        for i, series in enumerate(bar_vals):
            offs = (i + 0.5) * w - group_w / 2
            xi = x + offs
            color_i = self.colors[i % len(self.colors)]
            bars = ax_bars.bar(xi, series, width=w, label=titles[i], color=color_i, zorder=1)
            bars_list.append(bars)
            starts_list.append(np.zeros(len(dates)))
            vals_list.append(series)
            if len(series):
                bar_max = max(bar_max, float(np.nanmax(series)))

        # --- линия (правая ось) ---
        line_max = 0.0
        for j, series in enumerate(line_vals):
            y = np.array(series, dtype=float)
            color = self.colors[(len(bar_vals) + j) % len(self.colors)]
            label = titles[len(bar_vals) + j] if len(titles) > len(bar_vals) + j else ""

            if len(x) >= 4:
                xs = np.linspace(x.min(), x.max(), 2000)
                ys = PchipInterpolator(x, y)(xs)
                ax_line.plot(xs, ys, label=label, linewidth=2.5, color=color, zorder=8)
                line_max = max(line_max, float(np.nanmax(ys)))
            else:
                ax_line.plot(x, y, label=label, linewidth=2.5, color=color, zorder=8)
                line_max = max(line_max, float(np.nanmax(y)))

            if self.is_marker:
                ax_line.scatter(x, y, color=color, s=30, zorder=9)
            if self.Dot:
                ax_line.plot(x, y, linestyle="", marker="o", markersize=3,
                             color="black", markerfacecolor="black", zorder=10)

            # подписи над точками линии (смещение в процентах от диапазона ПРАВОЙ оси зададим позже, после set_ylim)
        # правую ось ограничим из Combine(...)
        min_r, max_r = 0.0, 100.0
        if self._combine_cfg:
            min_r = float(self._combine_cfg.get("min", 0.0))
            max_r = float(self._combine_cfg.get("max", 100.0))
        ax_line.set_ylim(min_r, max_r)
        try:
            ax_line.set_yticks(np.linspace(min_r, max_r, 6))
        except Exception:
            pass
        ax_line.yaxis.set_major_formatter(FuncFormatter(lambda y, pos: _format_number(int(round(y)))))

        # теперь можем поставить подписи над точками линии с учётом диапазона правой оси
        span_r = max_r - min_r if max_r > min_r else 1.0
        off_r = max(span_r * 0.03, 0.5)
        for j, series in enumerate(line_vals):
            for xi, yi in zip(x, series):
                ax_line.text(xi, yi + off_r, _format_number(yi),
                             ha="center", va="bottom",
                             fontsize=(self.FontSize or 8), color="#4A4A4A",
                             fontweight="bold", zorder=10)

        # --- таргет (по левой оси) ---
        if self.Target is not None:
            tgt = float(self.Target)
            ax_bars.axhline(y=tgt, color="#3AA655", linewidth=2, linestyle="-", alpha=0.9, zorder=7)
            ax_bars.text(0, tgt, "цель", ha="left", va="center", fontsize=8, fontweight="bold",
                         color="white", zorder=9,
                         bbox=dict(boxstyle="round,pad=0.3", fc="#32a932", ec="none", alpha=1.0),
                         transform=ax_bars.get_yaxis_transform())

        # --- подписи и оформление X/легенда/спайны ---
        ax_bars.set_title(self.GraphTitle, fontsize=12, fontweight="bold", loc="left")
        ax.set_xlabel(self.XAxesName, fontsize=12)
        ax_bars.set_ylabel(self.YAxesName, fontsize=12)
        ax_bars.grid(axis="y", linestyle="--", linewidth=0.5)
        ax_bars.set_axisbelow(True)

        ax.set_xticks(x)
        ax.set_xlim(-0.5, len(dates) - 0.5)
        ax.set_xticklabels(dates, rotation=(-90 if self.FontSize else 0), fontsize=7, ha="center")
        ax.tick_params(axis="x", which="both", length=0, pad=8)
        for i, lbl in enumerate(ax.get_xticklabels()):
            lbl.set_fontweight("bold" if i in weekend_idx else "normal")

        for s in ax_bars.spines.values():
            s.set_visible(False)
        for s in ax_line.spines.values():
            s.set_visible(False)

        # легенда объединённая
        h1, l1 = ax_bars.get_legend_handles_labels()
        h2, l2 = ax_line.get_legend_handles_labels()
        legend = ax_bars.legend(h1 + h2, l1 + l2, loc="upper center",
                                bbox_to_anchor=(0.5, 1.00), ncol=max(1, len(titles)),
                                frameon=False, fontsize=10)
        for t in legend.get_texts():
            t.set_fontweight("normal")
            t.set_color("#4A4A4A")

        # --- ЛЕВАЯ ось (бары) — пределы и формат тиков ---
        if self.YAxesMax is not None:
            ymin_final = self.YAxesMin if self.YAxesMin is not None else 0
            ymax_final = self.YAxesMax
        else:
            BASE, PAD_PCT, PAD_MIN = self.base, 0.25, 10
            ymax_candidate = max(bar_max, float(self.Target) if self.Target is not None else 0.0)
            top_auto = max(ymax_candidate * (1 + PAD_PCT), ymax_candidate + PAD_MIN)
            ymax_final = math.ceil(top_auto / BASE) * BASE
            ymin_final = self.YAxesMin if self.YAxesMin is not None else 0
        ax_bars.set_ylim(ymin_final, ymax_final)

        y0, y1 = ax_bars.get_ylim()
        span_l = y1 - y0
        base_tick = 100 if span_l >= 1000 else (10 if span_l >= 100 else 1)
        ax_bars.yaxis.set_major_formatter(
            FuncFormatter(lambda y, pos: _format_number(round(y / base_tick) * base_tick)))

        # подписи значений над барами (по ЛЕВОЙ шкале)
        gap_inside = max(span_l * 0.01, 6)  # небольшой отступ
        for bars, starts, series in zip(bars_list, starts_list, vals_list):
            for bar, start, val in zip(bars, starts, series):
                if val <= 0:
                    continue
                y_text = val + gap_inside
                ax_bars.text(bar.get_x() + bar.get_width() / 2, y_text, _format_number(val),
                             ha="center", va="bottom",
                             fontsize=(self.FontSize or 8), color="black", fontweight="bold",
                             zorder=2, clip_on=True)

        # --- скрытие правой оси по флагу ---
        if self.HideRightAxis:
            ax_line.tick_params(axis="y", which="both", labelright=False, right=False)
            if "right" in ax_line.spines:
                ax_line.spines["right"].set_visible(False)

    def _render_stacked(self, ax, x, dates, bar_vals, line_vals, titles, weekend_idx):
        # старый режим: стекинг баров + линия по левой оси
        bar_width = 0.9
        bottom = np.zeros(len(dates), dtype=float)
        stacked_max = 0.0
        bars_list, starts_list, vals_list = [], [], []

        for i, series in enumerate(bar_vals):
            color = self.colors[i % len(self.colors)]
            starts = bottom.copy()
            bars = ax.bar(dates, series, bottom=starts, width=bar_width, label=titles[i], color=color)
            bars_list.append(bars)
            starts_list.append(starts)
            vals_list.append(series)
            bottom += np.array(series, dtype=float)
            stacked_max = max(stacked_max, bottom.max())

        # линия
        line_max = 0.0
        for j, series in enumerate(line_vals):
            y = np.array(series, dtype=float)
            color = self.colors[(len(bar_vals) + j) % len(self.colors)]
            label = titles[len(bar_vals) + j] if len(titles) > len(bar_vals) + j else ""

            if len(x) >= 4:
                xs = np.linspace(x.min(), x.max(), 2000)
                ys = PchipInterpolator(x, y)(xs)
                ax.plot(xs, ys, label=label, linewidth=2.5, color=color)
                line_max = max(line_max, float(np.nanmax(ys)))
            else:
                ax.plot(x, y, label=label, linewidth=2.5, color=color)
                line_max = max(line_max, float(np.nanmax(y)))

            if self.is_marker:
                ax.scatter(x, y, color=color, s=30, zorder=3)
            if self.Dot:
                ax.plot(x, y, linestyle="", marker="o", markersize=3, color="black", markerfacecolor="black", zorder=5)

            off = max(stacked_max, line_max) * 0.03
            for xi, yi in zip(x, y):
                ax.text(xi, yi + off, _format_number(yi),
                        ha="center", va="bottom", fontsize=(self.FontSize or 8),
                        color="#4A4A4A", fontweight="bold")

        # таргет
        if self.Target is not None:
            tgt = float(self.Target)
            ax.axhline(y=tgt, color="#3AA655", linewidth=2, linestyle="-", alpha=0.9, zorder=2.5)
            ax.text(x=0, y=tgt, s="цель", ha="left", va="center",
                    fontsize=8, fontweight="bold", color="white", zorder=4,
                    bbox=dict(boxstyle="round,pad=0.3", fc="#32a932", ec="none", alpha=1.0),
                    transform=ax.get_yaxis_transform())

        # подписи, сетка, легенда
        ax.set_title(self.GraphTitle, fontsize=12, fontweight="bold", loc="left")
        ax.set_xlabel(self.XAxesName, fontsize=12)
        ax.set_ylabel(self.YAxesName, fontsize=12)
        ax.grid(axis="y", linestyle="--", linewidth=0.5)
        ax.set_axisbelow(True)

        ax.set_xticks(x)
        ax.set_xlim(-0.5, len(dates) - 0.5)
        ax.set_xticklabels(dates, rotation=(-90 if self.FontSize else 0), fontsize=7, ha="center")
        ax.tick_params(axis="x", which="both", length=0, pad=8)
        for i, lbl in enumerate(ax.get_xticklabels()):
            lbl.set_fontweight("bold" if i in weekend_idx else "normal")

        legend = ax.legend(loc="upper center", bbox_to_anchor=(0.5, 1.00),
                           ncol=max(1, len(titles)), frameon=False, fontsize=10)
        for t in legend.get_texts():
            t.set_fontweight("normal"); t.set_color("#4A4A4A")
        for s in ax.spines.values():
            s.set_visible(False)

        # шкала Y
        ymax_candidate = max(stacked_max, line_max)
        if self.Target is not None:
            ymax_candidate = max(ymax_candidate, float(self.Target))

        if self.YAxesMax is not None:
            ymin_final = self.YAxesMin if self.YAxesMin is not None else 0
            ymax_final = self.YAxesMax
        else:
            PAD_PCT, PAD_MIN = 0.25, 10
            BASE = self.base
            top_auto = max(ymax_candidate * (1 + PAD_PCT), ymax_candidate + PAD_MIN)
            ymax_final = math.ceil(top_auto / BASE) * BASE
            ymin_final = self.YAxesMin if self.YAxesMin is not None else 0
            if self.Target is not None:
                ymin_final = min(ymin_final, float(self.Target))
        ax.set_ylim(ymin_final, ymax_final)

        ymin, ymax = ax.get_ylim()
        span = ymax - ymin
        base_tick = 100 if span >= 1000 else (10 if span >= 100 else 1)
        ax.yaxis.set_major_formatter(FuncFormatter(lambda y, pos: _format_number(round(y / base_tick) * base_tick)))

        # подписи значений баров
        label_gap = max(ymax_candidate * 0.015, 1.0)
        for bars, starts, series in zip(bars_list, starts_list, vals_list):
            for bar, start, val in zip(bars, starts, series):
                if val > 0:
                    ax.text(bar.get_x() + bar.get_width() / 2, start + label_gap,
                            _format_number(val), ha="center", va="bottom",
                            fontsize=(self.FontSize or 8), color="black", fontweight="bold")

    # ---------- экспорт ----------

    def getGraphicsInBase64(self) -> str:
        """Рендерит в PNG base64 (совместимость с PHP-методом)."""
        fig, ax = plt.subplots(
            figsize=(self.Width / self.dpi, self.Height / self.dpi),
            dpi=self.dpi,
        )
        try:
            self.render_graphics(ax)
            buf = io.BytesIO()
            fig.savefig(buf, format="png", bbox_inches="tight")
            plt.close(fig)
            buf.seek(0)
            return base64.b64encode(buf.read()).decode("ascii")
        finally:
            plt.close(fig)

    # совместимость: как раньше возвращали HTML, теперь — PNG base64
    def renderGraphics(self) -> str:
        return self.getGraphicsInBase64()