from typing import List, Dict

import matplotlib.pyplot as plt
from statistics import mean
#from datetime import datetime


class MatplotlibTableConstructor2:
    """
    Аналог PHP/Python TableConstructor, но для вывода таблицы через matplotlib.
    Вместо HTML-таблицы формирует визуализацию таблицы с окраской ячеек,
    итоговой строкой и форматированием данных.
    """

    def __init__(self):
        self.conditions = {
            "Average Reaction Time": {"plan": 60, "operator": ">", "style": "#FF4D6E"},
            "Average Speed To Answer": {"plan": 60, "operator": ">", "style": "#FF4D6E"},
            "Bot Self Service Rate (чат-бот)": {"plan": 10, "operator": "<", "style": "#FF4D6E"},
        }

    def style_if(self, fact, plan, operator, style) -> str:
        try:
            fact = float(fact)
        except (ValueError, TypeError):
            return ""
        if operator == '>' and fact > plan:
            return style
        elif operator == '<' and fact < plan:
            return style
        elif operator == '>=' and fact >= plan:
            return style
        elif operator == '<=' and fact <= plan:
            return style
        elif operator == '=' and fact == plan:
            return style
        return ""

    def render_table(self, headers, data, title=None):
        n_rows = len(data) + 1
        n_cols = len(headers)

        row_height = 0.5
        col_width = 1.2

        fig_width = n_cols * col_width
        fig_height = n_rows * row_height + 1

        fig, ax = plt.subplots(figsize=(fig_width, fig_height))
        ax.axis('off')

        col_widths = [0.2, 0.1] + [0.08] * (len(headers) - 3) + [0.15]
        table = ax.table(cellText=data, colLabels=headers, loc='center', cellLoc='center', colWidths=col_widths)

        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1.2, 1.7)

        for j in range(len(headers)):
            cell = table[(0, j)]
            cell.set_text_props(weight='bold', color='white')
            cell.set_facecolor('#43484c')

        for i, row in enumerate(data):
            metric_name = row[0]
            condition = self.conditions.get(metric_name)
            for j in range(2, len(headers) - 1):
                cell = table[(i + 1, j)]
                val = row[j]
                if condition:
                    style_color = self.style_if(val, condition["plan"], condition["operator"], condition["style"])
                    if style_color:
                        cell.set_facecolor(style_color)

            avg_cell = table[(i + 1, len(headers) - 1)]
            avg_cell.set_text_props(weight='bold')

        plt.title(title, pad=20)
        plt.tight_layout()
        return fig

def get_data_in_dates(
        data_list: List[Dict],
        dates: List[str],
        date_key: str,
        value_key: str,
        precision: int
) -> List[float]:
    """
    Для каждого d в dates ищет в data_list запись r, где r[date_key] == d,
    и возвращает округлённое до precision число r[value_key], либо 0, если нет.
    """
    values = []
    for d in dates:
        rec = next((r for r in data_list if r.get(date_key) == d), None)
        if rec and value_key in rec:
            values.append(round(rec[value_key], precision))
        else:
            values.append(0)
    return values


