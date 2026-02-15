import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from typing import List, Dict, Optional, Union, Tuple
from datetime import datetime
from matplotlib.colors import LinearSegmentedColormap
import matplotlib.patches as patches

class PivotTable:
    """
    Универсальный класс для рендеринга сводных таблиц на matplotlib.
    При указании group_field и threshold включается поведение SpecialistPivotTable.
    """

    def __init__(
        self,
        items: List[Dict],
        row_field: str,
        date_field: str,
        value_field: str,
        # базовые настройки
        date_format: str = "%d.%m",
        show_total: bool = True,
        show_total_row: bool = True,
        show_total_column: bool = True,
        order_by_total: str = "desc",
        round_digits: int = 1,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,

        # specialist-настройки
        group_field: Optional[str] = None,
        column_titles: Optional[Dict[str, str]] = None,
        group_col_width: float = 0.3, # This parameter might become redundant with the new column_widths
        threshold: Optional[Union[float, str]] = None,
        split_names: bool = True,
        split_time: bool = True,
        max_name_words: int = 2,
        paint_type: str = 'asc',  # направление градиента для threshold

        # новые настройки для улучшений
        auto_column_width: bool = True,  # автоматическая ширина колонки (can be partially overridden by column_widths)
        cell_width: Optional[float] = None,  # принудительная ширина ячейки (applied if no specific width is given and auto_column_width is False)
        cell_height: Optional[float] = None,  # принудительная высота ячейки
        enable_cell_coloring: bool = True,  # включить покраску ячеек
        enable_zebra_striping: bool = True,  # включить зебру строк
        gradient_direction: str = 'asc',  # 'asc' - больше=светлее, 'desc' - больше=темнее
        zebra_color: Tuple[float, float, float] = (0.95, 0.95, 0.95),  # цвет зебры
        gradient_colors: List[str] = None,  # кастомные цвета для градиента
        font_size: int = 10,  # размер шрифта
        column_widths: Optional[Dict[str, float]] = None, # New parameter for specific column widths
        target_resolution: Tuple[int, int] = (1920, 1080)
    ):
        # common
        self.items = items
        self.row_field = row_field
        self.date_field = date_field
        self.value_field = value_field
        self.date_format = date_format
        self.show_total = show_total
        self.show_total_row = show_total_row
        self.show_total_column = show_total_column
        self.order_by_total = order_by_total
        self.round = round_digits
        self.min_date = min_date
        self.max_date = max_date

        # specialist
        self.group_field = group_field
        self.column_titles = column_titles or {}
        self.group_col_width = group_col_width # May be removed later
        self.split_names = split_names
        self.split_time = split_time
        self.max_name_words = max_name_words
        self.paint_type = paint_type

        # новые параметры
        self.auto_column_width = auto_column_width
        self.cell_width = cell_width
        self.cell_height = cell_height
        self.enable_cell_coloring = enable_cell_coloring
        self.enable_zebra_striping = enable_zebra_striping
        self.gradient_direction = gradient_direction
        self.zebra_color = zebra_color
        self.font_size = font_size
        self.column_widths = column_widths or {} # Initialize new parameter

        # настройка градиентных цветов
        if gradient_colors is None:
            if gradient_direction == 'asc':
                # чем больше значение - тем светлее (от красного к белому)
                self.gradient_colors = ["#ff0000", "#ffcccc", "#ffffff"]
            else:
                # чем больше значение - тем темнее (от белого к красному)
                self.gradient_colors = ["#ffffff", "#ffcccc", "#ff0000"]
        else:
            self.gradient_colors = gradient_colors

        # threshold parsing
        self.threshold = None
        self.threshold_is_percent = False
        self.threshold_value = None
        if threshold is not None:
            if isinstance(threshold, str) and threshold.endswith('%'):
                try:
                    self.threshold_value = float(threshold.strip('%')) / 100
                    self.threshold_is_percent = True
                except ValueError:
                    pass
            else:
                self.threshold = float(threshold)

    def _prepare_data(self):
        """Подготовка данных для таблицы"""
        # Обрезаем дату до YYYY-MM-DD
        for item in self.items:
            item[self.date_field] = item[self.date_field][:10]

        rows = sorted({item[self.row_field] for item in self.items})
        dates = sorted({item[self.date_field] for item in self.items})

        if self.min_date:
            dates = [d for d in dates if d >= self.min_date]
        if self.max_date:
            dates = [d for d in dates if d <= self.max_date]

        data = {r: {d: 0 for d in dates} for r in rows}
        for item in self.items:
            r = item[self.row_field]
            d = item[self.date_field]
            v = item.get(self.value_field, 0) or 0
            data[r][d] += v

        return rows, dates, data

    def _calculate_column_widths(self, table_data: List[List], col_fields: List[str]) -> List[float]:
      """Вычисление ширины колонок на основе исходных ключей (field names), а не текста заголовков"""
      col_widths = []
      num_cols = len(table_data[0]) if table_data else 0

      for col_idx in range(num_cols):
          field_key = col_fields[col_idx] if col_idx < len(col_fields) else str(col_idx)

          if field_key in self.column_widths:
              col_widths.append(self.column_widths[field_key])
          elif self.auto_column_width:
              max_length = 0
              for row in table_data:
                  if col_idx < len(row):
                      cell_text = str(row[col_idx])
                      lines = cell_text.split('\n')
                      max_line_length = max(len(line) for line in lines) if lines else 0
                      max_length = max(max_length, max_line_length)

              width = max(0.1, min(0.8, max_length * 0.02))
              col_widths.append(width)
          else:
              col_widths.append(self.cell_width if self.cell_width is not None else 0.1)

      return col_widths


    def _format_multiline_text(self, text: str, is_time: bool = False) -> str:
        """Helper to format text with line breaks"""
        if not self.split_names and not is_time:
            return text
        if not self.split_time and is_time:
            return text

        if is_time:
          # Split by colon for time-like strings
          return '\n'.join(text.split(':'))
        else:
          # Split by space or hyphen for names
          words = text.replace('-', ' ').split()
          if len(words) > self.max_name_words:
              return '\n'.join([' '.join(words[i:i+self.max_name_words]) for i in range(0, len(words), self.max_name_words)])
          else:
              return '\n'.join(words)


    def _get_gradient_color(self, value: float, min_val: float, max_val: float) -> Tuple[float, float, float]:
        """Получение цвета градиента для значения"""
        if max_val == min_val:
            intensity = 0.5
        else:
            intensity = (value - min_val) / (max_val - min_val)

        if self.gradient_direction == 'desc':
            intensity = 1 - intensity

        # создаем цветовую карту
        cmap = LinearSegmentedColormap.from_list("custom", self.gradient_colors)
        return cmap(intensity)[:3]

    def render(self):

      rows, dates, data = self._prepare_data()

      # Итоги по строкам
      summary = {r: sum(data[r][d] for d in dates) for r in rows}
      rev = self.order_by_total == 'desc'
      rows = sorted(rows, key=lambda r: summary[r], reverse=rev)

      # Матрица значений для расчета градиента
      all_values = [data[r][d] for r in rows for d in dates if data[r][d] > 0]
      vmin = min(all_values) if all_values else 0
      vmax = max(all_values) if all_values else 1

      threshold_val = None
      if self.threshold_is_percent and self.threshold_value is not None:
          threshold_val = vmax * self.threshold_value
      elif self.threshold is not None:
          threshold_val = self.threshold

      # Заголовки
      header = []
      col_fields = []  # <== Сюда записываем оригинальные поля колонок

      if self.group_field:
          header += [
              self.column_titles.get(self.row_field, 'Период'),
              self.column_titles.get(self.group_field, 'Группа')
          ]
          col_fields += [self.row_field, self.group_field]
      else:
          header.append(self.column_titles.get(self.row_field, self.row_field).title())
          col_fields.append(self.row_field)

      date_headers = [datetime.strptime(d, "%Y-%m-%d").strftime(self.date_format) for d in dates]
      header += date_headers
      col_fields += dates  # даты как есть (строки)

      if self.show_total_column:
          header.append(self.column_titles.get('total', 'Итого'))
          col_fields.append('total')

      stats_keys = ['avg', 'min', 'max']
      stats_headers = [self.column_titles.get(k, k.title()) for k in stats_keys]
      header += stats_headers
      col_fields += stats_keys

      # Строим таблицу данных
      table_data = [header]
      cell_colours = [[(0.2, 0.2, 0.2)] * len(header)]

      row2grp = {it[self.row_field]: it.get(self.group_field, "") for it in self.items} if self.group_field else {}

      for idx, r in enumerate(rows):
          vals = [data[r][d] for d in dates]
          formatted = ["" if v == 0 else round(v, self.round) for v in vals]

          row_cells = []
          if self.group_field:
              row_cells.append(self._format_multiline_text(r, is_time=False))
              row_cells.append(self._format_multiline_text(row2grp.get(r, ""), is_time=True))
          else:
              row_cells.append(r)

          row_cells += formatted

          total = summary[r] if self.show_total_column else 0
          non_zero_vals = [v for v in vals if v > 0]
          avg = round(np.mean(non_zero_vals), self.round) if non_zero_vals else 0
          mn = round(min(non_zero_vals), self.round) if non_zero_vals else 0
          mx = round(max(vals), self.round) if vals else 0

          if self.show_total_column:
              row_cells.append(total)
          row_cells += [avg, mn, mx]
          table_data.append(row_cells)

          base_color = self.zebra_color if self.enable_zebra_striping and idx % 2 else (1, 1, 1)
          colors = [base_color] * len(row_cells)

          if self.enable_cell_coloring:
              date_start_col = 2 if self.group_field else 1
              for j, v in enumerate(vals, start=date_start_col):
                  if v > 0 and (threshold_val is None or v >= threshold_val):
                      colors[j] = self._get_gradient_color(v, vmin, vmax)
          cell_colours.append(colors)

      if self.show_total_row:
          tot_vals = [sum(data[r][d] for r in rows) for d in dates]
          tot_row = []

          if self.group_field:
              tot_row += [self.column_titles.get('total', 'Итого'), ""]
          else:
              tot_row.append(self.column_titles.get('total', 'Итого'))

          tot_row += [round(v, self.round) for v in tot_vals]

          if self.show_total_column:
              total_sum = sum(tot_vals)
              tot_row.append(total_sum)

          non_zero_tots = [v for v in tot_vals if v > 0]
          tot_row += [
              round(np.mean(non_zero_tots), self.round) if non_zero_tots else 0,
              round(min(non_zero_tots), self.round) if non_zero_tots else 0,
              round(max(tot_vals), self.round) if tot_vals else 0
          ]

          table_data.append(tot_row)
          cell_colours.append([(0.3, 0.3, 0.3)] * len(header))

      # Настройка фигуры
      num_cols = len(col_fields)
      num_rows = len(rows) + (2 if self.show_total_row else 1)
      fig_width = num_cols * 0.8 + 2
      fig_height = num_rows * 0.6 + 2

      fig, ax = plt.subplots(figsize=(fig_width, fig_height))
      ax.axis('off')

      table = ax.table(
          cellText=table_data,
          cellColours=cell_colours,
          cellLoc='center',
          loc='center'
      )
      table.auto_set_font_size(False)
      table.set_fontsize(self.font_size)

      # 👉 Белый цвет текста в заголовке (первая строка)
      for col_idx in range(len(header)):
          cell = table[(0, col_idx)]
          cell.get_text().set_color('white')

# 👉 Белый цвет текста в итоговой строке (последняя строка)
      if self.show_total_row:
          last_row_idx = len(table_data) - 1
          for col_idx in range(len(header)):
              cell = table[(last_row_idx, col_idx)]
              cell.get_text().set_color('white')

      # 👇 Передаём поля в _calculate_column_widths
      calculated_widths = self._calculate_column_widths(table_data, col_fields)
      if calculated_widths:
          for i, width in enumerate(calculated_widths):
              for j in range(len(table_data)):
                  if (j, i) in table._cells:
                      table._cells[(j, i)].set_width(width)

      if self.cell_height is not None:
          for (row, col), cell in table._cells.items():
              cell.set_height(self.cell_height)

      plt.tight_layout()
      return fig


    def save_table(self, filename: str, dpi: int = 300):
        """Сохранение таблицы в файл"""
        fig = self.render()
        fig.savefig(filename, dpi=dpi, bbox_inches='tight',
                   facecolor='white', edgecolor='none')
        plt.close(fig)

    def get_statistics(self) -> Dict:
        """Получение статистики по данным"""
        rows, dates, data = self._prepare_data()

        all_values = []
        for r in rows:
            for d in dates:
                val = data[r][d]
                if val > 0:
                    all_values.append(val)

        if not all_values:
            return {"min": 0, "max": 0, "avg": 0, "total": 0, "count": 0}

        return {
            "min": min(all_values),
            "max": max(all_values),
            "avg": sum(all_values) / len(all_values),
            "total": sum(all_values),
            "count": len(all_values)
        }