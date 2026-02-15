import math

from utils.transformers.mp_report import try_parse_period


class BasicTable:
    """
    Аналог PHP-класса BasicTable для формирования HTML-таблицы с агрегированием
    и «окрашиванием» ячеек по определённым правилам.
    """

    def __init__(self):
        # Данные
        self.Items = [
            {'one': '1', 'two': '2', 'three': '3'},
            {'one': '4', 'two': '5', 'three': '6'},
            {'one': '7', 'two': '8', 'three': '9'},
        ]

        # Описание полей (заголовок, ключ, прочие настройки)
        self.Fields = [
            {'title': 'Два', 'field': 'two'},
            {'title': 'Три', 'field': 'three'},
            {
                'title': 'Один',
                'field': 'one',
                'paint': True,       # Если True, используем раскраску по градиенту
                'summary': 'sum',    # Показываем итоги (сумму)
            },
        ]

        # Стили (внешний вид таблицы)
        self.TableStyle = "border: 1px solid  #32383e; border-collapse: collapse; font-size: 12; margin: 0 auto; font-family: Calibri, Candara, 'Segoe UI', Optima, Arial, sans-serif;"
        self.TDStyle = ("border: 1px solid #858585; text-align: center; padding-left: 3px; padding-right: 3px; "
                        "font-family: Calibri, Candara, \"Segoe UI\", Optima, Arial, sans-serif;")
        self.THStyle = (
            "color: #fff; border: 1px solid #32383e; text-align: center; padding-left: 3px; padding-right: 3px; "
            "background-color: #43484c; font-family: Calibri, Candara, \"Segoe UI\", Optima, Arial, sans-serif;")

        self.TRStyle = ""

        # Если нужно отображать итоговую строку
        self.ShowSummaryXAxis = False

        # Внутренние (приватные) переменные
        self._TableBody = ''
        self._RowData = ''
        self._RowIndex = 0
        self._AggregatedData = {}  # Словарь с агрегированными данными (min, max, sum и т.п.)
        self.paint_type = 'desc'

        self.CenterHorizontally: bool = True  # включаем по умолчанию
        self.TableWrapperStyle: str = "width:100%; display:flex; justify-content:center;"

    def _fmt_num(self, v, decimals: int | None = None) -> str:
        if v is None:
            return ""
        try:
            f = float(v)
        except (TypeError, ValueError):
            return str(v)

        # авто-режим: целые без .0, иначе 1 знак
        if decimals is None:
            if abs(f - round(f)) < 1e-9:
                return f"{int(round(f))}"
            return f"{f:.1f}".rstrip("0").rstrip(".")
        # фиксированная точность
        s = f"{f:.{decimals}f}"
        return s.rstrip("0").rstrip(".") if decimals > 0 else s

    def getTable(self) -> str:
        """
        Генерирует таблицу и возвращает HTML-разметку.
        """
        self._constructBody()
        return self._renderTable()

    def _is_nan(self, x) -> bool:
        try:
            return math.isnan(float(x))
        except (TypeError, ValueError):
            return False

    def _constructBody(self) -> None:
        """
        Аналог PHP constructBody():
        1) Подготовка (makePreparation)
        2) Генерация заголовков (TH)
        3) Генерация строк (TR, TD)
        4) Генерация итогов (если ShowSummaryXAxis = True)
        """
        self._makePreparation()

        # Генерируем заголовки (TH) по полям
        for field in self.Fields:
            all_values = [x.get(field['field']) for x in self.Items]
            if any(v is not None for v in all_values):
                self._genCellTH(field['title'])
        self._genRow()

        # Генерируем основную часть таблицы
        for item in self.Items:
            for field in self.Fields:
                value = item.get(field['field'], None)
                if value is not None and not self._is_nan(value):
                    style = field.get('style', '')
                    if 'paint' in field:
                        paint_type = field.get('paint_type', 'desc')
                        agg = self._AggregatedData.get(field['field'], {'min': None, 'max': None})
                        min_override = field.get('paint_min')
                        max_override = field.get('paint_max')
                        min_val = min_override if min_override is not None else agg.get('min')
                        max_val = max_override if max_override is not None else agg.get('max')
                        colour = self._gradientColour(
                            current=self._toFloat(value),
                            min_val=min_val,
                            max_val=max_val,
                            paint_type=paint_type
                        )
                        style = f"background: {colour};"


                    # Если нужно «закрашивать» относительную разницу от 0
                    if 'customPaint' in field:
                        max_val_for_colour = max(
                            abs(self._AggregatedData[field['field']]['max']),
                            abs(self._AggregatedData[field['field']]['min'])
                        )
                        colour = self._gradientColourFromZero(
                            current=self._toFloat(value),
                            max_val=max_val_for_colour,
                            paint_type=field['customPaint']
                        )

                        style = f"background: {colour};"

                    # Округление
                    if 'round' in field and value is not None:
                        try:
                            f = float(value)
                            if f.is_integer():
                                value = int(f)
                            else:
                                value = round(float(value), field['round'])

                        except (ValueError, TypeError):
                            pass

                    # Формат времени (секунды -> минуты:секунды)
                    if field.get('time_format'):
                        # Считаем, что value хранит секунды
                        value = self._secondsToMinutes(self._toInt(value))

                    # Формат даты
                    if 'date_format' in field:
                        import datetime
                        try:
                            dt = try_parse_period(value)
                        except ValueError:
                            try:
                                dt = try_parse_period(value)
                            except ValueError:
                                dt = None
                        if dt:
                            value = dt.strftime(field['date_format'])

                    # Превращаем в строку
                    value = str(value)
                else:
                    # Если значение = None
                    if field.get('time_format'):
                        value = '00:00'
                    else:
                        value = ''

                self._genCellTD(value, style)

            self._genRow()  # Закрываем строку

        # Генерация «итогов», если требуется
        if self.ShowSummaryXAxis:
            for field in self.Fields:
                all_values = [x.get(field['field']) for x in self.Items]
                should_show = (
                        any(v is not None for v in all_values)
                        or 'summary' in field
                        or 'advancedSummaryAvg' in field
                        or 'avgFor24' in field
                        or 'total_title' in field
                )

                if not should_show:
                    self._genCellTH('')
                    continue

                # 1) advancedSummaryAvg
                if 'advancedSummaryAvg' in field:
                    avg_advanced = self._AggregatedData.get(field['field'], {}).get('avgAdvanced', '')
                    if field.get('time_format') and isinstance(avg_advanced, (int, float)):
                        avg_advanced = self._secondsToMinutes(int(avg_advanced))
                    if 'round' in field and isinstance(avg_advanced, (int, float)):
                        try:
                            avg_advanced = round(float(avg_advanced), field['round'])
                        except Exception:
                            pass

                    if isinstance(avg_advanced, (int, float)) and float(avg_advanced).is_integer():
                        self._genCellTH(str(int(avg_advanced)))
                    else:
                        self._genCellTH(str(avg_advanced))

                # 2) summary (sum/min/max)
                elif 'summary' in field:
                    if field.get('time_format'):
                        summ_val = self._AggregatedData.get(field['field'], {}).get(field['summary'], 0)
                        value = self._secondsToMinutes(self._toInt(summ_val))
                        self._genCellTH(str(value))
                    else:
                        summ_val = self._AggregatedData.get(field['field'], {}).get(field['summary'], '')
                        value = self._fmt_num(summ_val, field.get('round'))

                        if isinstance(value, (int, float)) and float(value).is_integer():
                            self._genCellTH(str(int(value)))
                        else:
                            self._genCellTH(str(value))

                # 3) avgFor24
                elif 'avgFor24' in field:
                    avg_advanced = self._AggregatedData.get(field['field'], {}).get('avgAdvanced', '')
                    if isinstance(avg_advanced, (int, float)) and float(avg_advanced).is_integer():
                        self._genCellTH(str(int(avg_advanced)))
                    else:
                        self._genCellTH(str(avg_advanced))

                # 4) total_title
                elif 'total_title' in field:
                    self._genCellTH('Итого')

                else:
                    self._genCellTH('')
            self._genRow()

    def _makePreparation(self) -> None:
        """
        Аналог PHP makePreparation():
        - Сбор min, max, sum по полям, где есть 'paint' или 'summary'
        - Определение нужно ли ShowSummaryXAxis (если нашли summary)
        - Расчёт advancedSummaryAvg, avgFor24 и т.д.
        """
        self._AggregatedData = {}

        for field in self.Fields:
            has_paint = ('paint' in field) or ('summary' in field)
            if has_paint:
                arr = []
                for it in self.Items:
                    val = it.get(field['field'])
                    # Если валидное число, кладём в массив
                    f_val = self._toFloat(val)
                    if f_val is not None:
                        arr.append(f_val)

                if not arr:
                    arr = [0.0]

                max_val = max(arr)
                min_val = min(arr)
                sum_val = sum(arr)
                avg_val = sum_val / len(arr) if arr else 0

                self._AggregatedData[field['field']] = {
                    'min': min_val,
                    'max': max_val,
                    'sum': sum_val,
                    'avg': avg_val
                }

            # Если у поля есть summary или advancedSummaryAvg, значит нужна итоговая строка
            if 'summary' in field or 'advancedSummaryAvg' in field or 'avgFor24' in field:
                self.ShowSummaryXAxis = True

            # advancedSummaryAvg
            if 'advancedSummaryAvg' in field:
                numerator_arr = []
                denominator_arr = []
                adv_cfg = field['advancedSummaryAvg']

                # numerator/denominator - названия ключей
                numerator_key = adv_cfg['numerator']
                denominator_key = adv_cfg['denominator']
                for it in self.Items:
                    num_val = self._toFloat(it.get(numerator_key, 0))
                    den_val = self._toFloat(it.get(denominator_key, 0))
                    numerator_arr.append(num_val if num_val else 0)
                    denominator_arr.append(den_val if den_val else 0)

                sum_numerator = sum(numerator_arr)
                sum_denominator = sum(denominator_arr)
                multiplication = adv_cfg.get('multiplication', 100)

                if sum_numerator and sum_denominator:
                    avg_advanced = (sum_numerator / sum_denominator) * multiplication
                    # Округление
                    if 'round' in adv_cfg:
                        avg_advanced = round(avg_advanced, adv_cfg['round'])
                else:
                    avg_advanced = ''

                if field['field'] not in self._AggregatedData:
                    self._AggregatedData[field['field']] = {}
                self._AggregatedData[field['field']]['avgAdvanced'] = avg_advanced

            # avgFor24
            if 'avgFor24' in field:
                numerator_arr = []
                denominator_arr = []
                avg24_cfg = field['avgFor24']
                numerator_key = avg24_cfg['numerator']
                denominator_key = avg24_cfg['denominator']

                for it in self.Items:
                    num_val = self._toFloat(it.get(numerator_key, 0))
                    den_val = self._toFloat(it.get(denominator_key, 0))
                    # Если denominator == numerator, считаем denominator как 1
                    # (аналогично тому, что было в вашем коде)
                    if denominator_key == numerator_key:
                        den_val = 1
                    numerator_arr.append(num_val if num_val else 0)
                    denominator_arr.append(den_val if den_val else 0)

                sum_numerator = sum(numerator_arr)
                sum_denominator = sum(denominator_arr)

                if sum_numerator and sum_denominator:
                    avg_advanced = sum_numerator / sum_denominator
                    # Формат как время, если нужно
                    if 'time_format' in avg24_cfg and avg24_cfg['time_format']:
                        # переводим в формат "MM:SS"
                        # avg_advanced здесь может быть float, интерпретируем как "секунды"
                        avg_advanced = self._secondsToMinutes(int(avg_advanced))
                    if 'round' in avg24_cfg:
                        try:
                            avg_advanced = round(float(avg_advanced), avg24_cfg['round'])
                        except:
                            pass
                else:
                    avg_advanced = ''

                if field['field'] not in self._AggregatedData:
                    self._AggregatedData[field['field']] = {}
                self._AggregatedData[field['field']]['avgAdvanced'] = avg_advanced

    def _genCellTH(self, cellData: str, style: str = "") -> None:
        """
        Аналог PHP genCellTH: формирует <th>.
        """
        th_data = f"<th style='{self.THStyle} {style}'>{cellData}</th>"
        self._RowData += th_data

    def _genRow(self, customRowData: str = None) -> None:
        """
        Аналог PHP genRow: формирует <tr>...</tr> и добавляет в тело таблицы.
        """
        if customRowData is None:
            customRowData = self._RowData
        self._RowIndex += 1
        # Чередование цвета фона (нечётная строка - #f2f2f2)
        back_colour = "background-color: #f2f2f2;" if (self._RowIndex % 2) else ""
        row_html = f"<tr style='{back_colour}{self.TRStyle}'>{customRowData}</tr>"

        self._addDataToTableBody(row_html)
        self._RowData = ''

    def _addDataToTableBody(self, data: str) -> None:
        """
        Аналог PHP addDataToTableBody.
        """
        self._TableBody += data

    from typing import Optional

    def _gradientColour(self, current: float, min_val: Optional[float], max_val: Optional[float],
                    paint_type: str = 'desc') -> str:
        colours = {
            0: '#FFFFFF',
            10: '#FFEBEE',
            20: '#FFD7DE',
            30: '#FFC3CE',
            40: '#FFAFBE',
            50: '#FF9CAE',
            60: '#FF889E',
            70: '#FF748E',
            80: '#FF607E',
            90: '#FF4D6E',
            100: '#FF4D6E',
        }

        if min_val is None or max_val is None or max_val == min_val:
            return '#FFFFFF'

        percent = round((current - min_val) / (max_val - min_val), 2) * 100

        # инвертируем только когда paint_type == 'asc' (чтобы desc давал "чем больше — тем насыщеннее")
        if paint_type == 'asc':
            percent = 100 - percent

        percent = max(0, min(100, percent))
        closest_10 = round(percent / 10) * 10
        return colours.get(closest_10, '#FFFFFF')

    def _gradientColourFromZero(self, current: float, max_val: float, paint_type: str = "desc") -> str:
        colours = {
            0: "#FFFFFF",
            10: "#FFEBEE",
            20: "#FFD7DE",
            30: "#FFC3CE",
            40: "#FFAFBE",
            50: "#FF9CAE",
            60: "#FF889E",
            70: "#FF748E",
            80: "#FF607E",
            90: "#FF4D6E",
            100: "#FF4D6E",
        }

        if current == 0:
            percent = 0
        else:
            percent = abs(current) / max_val * 100

        if percent > 100:
            percent = 100

        closest = round(percent / 10) * 10

        return colours.get(closest, "#FFFFFF")



    def _genCellTD(self, cellData: str, style: str = "") -> None:
        """
        Аналог PHP genCellTD: формирует <td>.
        """
        td_data = f"<td style='{self.TDStyle} {style}'>{cellData}</td>"
        self._RowData += td_data

    def _renderTable(self) -> str:
        table_html = f"<table style='{self.TableStyle}'>{self._TableBody}</table>"
        if getattr(self, "CenterHorizontally", False):
            return f"<div style='{self.TableWrapperStyle}'>{table_html}</div>"
        return table_html

    def _secondsToMinutes(self, seconds: int | None) -> str:
        """
        Перевод секунд в строку формата MM:SS.
        Аналог PHP secondsToMinutes.
        """
        if seconds is None:
            return "00:00"
        minutes = seconds // 60
        remainder = seconds % 60
        mm = f"{minutes:02d}"
        ss = f"{remainder:02d}"
        return f"{mm}:{ss}"

    def _toFloat(self, val) -> float | None:
        """
        Попытка привести к float, если не получается — None
        """
        try:
            f = float(val)
            return None if math.isnan(f) else f
        except (ValueError, TypeError):
            return None

    def _toInt(self, val) -> int:
        """
        Попытка привести к int, в случае неудачи возвращает 0.
        """
        try:
            return int(float(val))
        except (ValueError, TypeError):
            return 0