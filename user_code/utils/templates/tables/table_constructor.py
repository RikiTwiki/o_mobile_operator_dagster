import math


class TableConstructor:
    """
    Класс, аналогичный PHP-версии для генерации HTML-таблиц со стилями.
    """

    def __init__(self):
        # Публичные свойства
        self.TableStyle = (
            "border: 1px solid #32383e; "
            "border-collapse: collapse; "
            "font-size: 12px; "
            "margin: 0 auto; "
            "font-family: Calibri, Candara, 'Segoe UI', Optima, Arial, sans-serif;"
        )
        self.TDStyle: str = "border: 1px solid  #858585; text-align: center; padding-left: 3px; padding-right: 3px; font-family: Calibri, Candara, 'Segoe UI', Optima, Arial, sans-serif;"
        self.THStyle: str = "color: #fff; border: 1px solid #32383e; text-align: center; padding-left: 3px; padding-right: 3px; background-color: #43484c; font-family: Calibri, Candara, 'Segoe UI', Optima, Arial, sans-serif;"
        self.TRStyle: str = ""
        self.TRTitleStyle: str = "border: 1px solid #858585; font-weight:bold; text-align: center; background-color: #d9d9d9; font-family: Calibri, Candara, 'Segoe UI', Optima, Arial, sans-serif;"

        # Приватные и дополнительные свойства
        self.__CommentBoxStyle: str = "font-size: 16px;"
        self.__TableBody: str = ""
        self.__RowData: str = ""
        self.__colspan: str = "11"

        self.EscapeHTMLDefault: bool = False

    @staticmethod
    def _html_escape(s: str) -> str:
        import html
        return html.escape(s, quote=True)

    @staticmethod
    def _build_attrs(props: dict | None) -> str:
        if not props:
            return ""
        parts = []
        for k, v in props.items():
            parts.append(f'{k}="{v}"')
        return " " + " ".join(parts)

    def _generate_color_style(self, value, min_v, max_v) -> str:
        # 1-в-1 логика PHP generateColorStyle
        min_v = to_float_safe(min_v, default=0.0)
        max_v = to_float_safe(max_v, default=0.0)

        if max_v == min_v:
            return "background-color: #FFFFFF;"

        v = to_float_safe(value, default=None)
        if v is None:
            return ""  # пустые/нечисловые не красим

        # защита на всякий случай
        v = max(0.0, v)
        min_v = max(0.0, min_v)
        max_v = max(0.0, max_v)

        log_min = math.log(min_v + 1)
        log_max = math.log(max_v + 1)
        log_val = math.log(v + 1)

        denom = (log_max - log_min)
        percent = 0.0 if denom == 0 else (log_val - log_min) / denom
        percent = max(0.0, min(1.0, percent))

        # max -> розовый/красный, min -> белый (как в PHP)
        r_max, g_max, b_max = 0xFF, 0x4D, 0x6E  # FF4D6E
        r_min, g_min, b_min = 0xFF, 0xFF, 0xFF  # FFFFFF

        r = int(r_min + (r_max - r_min) * percent)
        g = int(g_min + (g_max - g_min) * percent)
        b = int(b_min + (b_max - b_min) * percent)

        color = f"#{r:02X}{g:02X}{b:02X}"
        return f"background-color: {color};"

    def genCellTH(self, cellData: str, style: str = None, styleIf: dict = None, props: dict = None, *, escape: bool | None = None,) -> None:
        """
        Добавляет ячейку TH (заголовок) в текущую строку RowData.

        :param cellData: Текст, который будет находиться в ячейке TH
        :param style: Дополнительные стилевые параметры
        :param styleIf: Словарь для условного применения стилей: {"plan": ..., "operator": ..., "style": ...}
        :param props: Дополнительные атрибуты ячейки (dict), например {"colspan": "2", "rowspan": "3", ...}
        """
        thBasicStyle = ""
        if styleIf is not None:
            thBasicStyle = self.__styleIf(
                fact=cellData,
                plan=styleIf.get("plan"),
                operator=styleIf.get("operator"),
                style=styleIf.get("style")
            )

        if escape is None:
            escape = self.EscapeHTMLDefault

        content = str(cellData)
        if escape:
            content = self._html_escape(content)

        attrs = self._build_attrs(props)

        combined_style = f"{self.THStyle} {style or ''} {thBasicStyle}".strip()
        thData = f'<th style="{combined_style}"{attrs}>{content}</th>'
        self.__RowData += thData

    def multiGenCellTH(self, cellsData: list, style: str = None, styleIf: dict = None, props: dict | None = None, *, escape: bool | None = None) -> None:
        """
        Добавляет несколько ячеек TH подряд.

        :param cellsData: Список данных для каждой ячейки.
        :param style: Дополнительные стилевые параметры.
        :param styleIf: Словарь для условного применения стилей, аналогичен методу genCellTH.
        """
        for cellData in cellsData:
            self.genCellTH(cellData, style, styleIf, props, escape=escape)

    def genRow(self, customRowData: str = None) -> None:
        if customRowData is None:
            customRowData = self.__RowData
        row = f'<tr style="{self.TRStyle}">{customRowData}</tr>'
        self.__addDataToTableBody(row)
        self.__RowData = ""

    def genCellTD(self, cellData, style: str = None, styleIf: dict = None, minMax: dict | None = None, *, escape: bool | None = None) -> None:
        tdBasicStyle = ""

        if minMax is not None:
            tdBasicStyle = self._generate_color_style(cellData, minMax.get("min"), minMax.get("max"))
        elif styleIf is not None:
            tdBasicStyle = self.__styleIf(
                fact=cellData,
                plan=styleIf.get("plan"),
                operator=styleIf.get("operator"),
                style=styleIf.get("style"),
            )

        if escape is None:
            escape = self.EscapeHTMLDefault

        content = "" if cellData is None else str(cellData)
        if escape:
            content = self._html_escape(content)

        combined_style = f"{self.TDStyle} {style or ''} {tdBasicStyle}".strip()
        self.__RowData += f'<td style="{combined_style}">{content}</td>'

    def multiGenCellTD(self, cellsData: list, style: str = None, styleIf: dict = None, minMax: dict | None = None, *, escape: bool | None = None) -> None:
        for cellData in cellsData:
            self.genCellTD(cellData, style, styleIf, minMax, escape=escape)

    def genRowTitle(self, customRowData: str = None) -> None:
        if customRowData is None:
            customRowData = self.__RowData
        row = f'<tr style="{self.TRTitleStyle}"><td colspan="{self.__colspan}">{customRowData}</td></tr>'
        self.__addDataToTableBody(row)
        self.__RowData = ""

    def genRowTitleAuto(self, title: str, colspan: int | None = None, *, escape: bool | None = None) -> None:
        import re
        if colspan is None:
            colspan = len(re.findall(r"<(td|th)\b", self.__RowData, flags=re.I))
            if colspan == 0:
                colspan = int(self.__colspan) if str(self.__colspan).isdigit() else 1

        if escape is None:
            escape = self.EscapeHTMLDefault
        content = self._html_escape(title) if escape else title

        row = f'<tr style="{self.TRTitleStyle}"><td colspan="{colspan}">{content}</td></tr>'
        self.__addDataToTableBody(row)
        self.__RowData = ""

    def genDataRow(self, data: dict) -> None:
        """
        Генерация строк данных с разбивкой по new_items в порядке:
        old_name, qwes_recomendet, new-name, attempt_number, service, connections, conversion

        data = {
          'old_name': str,
          'qwes_recomendet': str|None,
          'attempt_number': int,
          'service': int,
          'new_items': [ {'new-name': str, 'connections': int}, ... ],
          'conversion': str
        }
        """
        items = data.get("new_items") or []
        count = max(1, len(items))

        attempt_num = int(data.get("attempt_number") or 0)
        service_cnt = int(data.get("service") or 0)
        conversion = data.get("conversion") if data.get("conversion") not in (None, "") else "–"

        rowspan_props = {"rowspan": str(count)}

        for i in range(count):
            self.__RowData = ""

            if i == 0:
                self.genCellTD(data.get("old_name", ""), props=rowspan_props)
                self.genCellTD(data.get("qwes_recomendet", "–") or "–", props=rowspan_props)

            # динамическая колонка new-name
            if i < len(items) and isinstance(items[i], dict):
                self.genCellTD(items[i].get("new-name", ""))
            else:
                self.genCellTD("")

            if i == 0:
                self.genCellTD(str(attempt_num), props=rowspan_props)
                self.genCellTD(str(service_cnt), props=rowspan_props)

            # динамическая колонка connections
            if i < len(items) and isinstance(items[i], dict):
                self.genCellTD(str(items[i].get("connections", "")))
            else:
                self.genCellTD("")

            if i == 0:
                self.genCellTD(conversion, props=rowspan_props)

            self.genRow()

    def genCommentBox(self, comment: str = None) -> None:
        """
        Добавляет комментарий в отдельном div в тело таблицы (не совсем семантично,
        но аналогично PHP-коду).

        :param comment: Текст комментария
        """
        if comment is not None:
            row = f'<div style="{self.__CommentBoxStyle}">{comment}</div>'
            self.__addDataToTableBody(row)
            self.__RowData = ""

    def renderTable(self) -> str:
        """
        Возвращает итоговый HTML-код таблицы.
        """
        return f'<table style="{self.TableStyle}">{self.__TableBody}</table>'

    def __addDataToTableBody(self, data: str) -> None:
        """
        Добавляет произвольный HTML-код (обычно <tr>...</tr>) в тело таблицы.
        """
        self.__TableBody += data

    def __styleIf(self, fact, plan, operator, style) -> str:
        """
        Применение стиля по условию.
        :param fact: Фактическое значение
        :param plan: Значение для сравнения
        :param operator: Оператор для сравнения (>, <, =, >=, <=)
        :param style: Строка стиля, которую нужно применить
        :return: Стиль, если условие выполнено, иначе пустая строка
        """
        if style is None:
            return ""

        # Попытаемся привести fact к float, если это возможно
        try:
            fact = float(fact)
        except (ValueError, TypeError):
            pass

        result = ""
        if operator == '>=':
            if round(to_float_safe(fact)) >= round(to_float_safe(plan)):
                result = style
        elif operator == '<=':
            if round(to_float_safe(fact)) < round(to_float_safe(plan)):
                result = style
        elif operator == '<':
            if round(to_float_safe(fact)) < round(to_float_safe(plan)):
                result = style
        elif operator == '>':
            if round(to_float_safe(fact)) > round(to_float_safe(plan)):
                result = style
        elif operator == '=':
            if fact == plan:
                result = style
        return result

def to_float_safe(x, default=0.0):
    if x is None:
        return default
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if not s:
        return default
    s = s.replace("%", "").replace(",", ".")
    try:
        return float(s)
    except (TypeError, ValueError):
        return default