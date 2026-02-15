import math
from datetime import datetime, timedelta, date, time
from typing import List, Dict, Any, Optional, Union

from utils.transformers.mp_report import smart_number


class PivotTable:
    """
    Аналог PHP-класса PivotTable для генерации сводной таблицы.
    """

    def __init__(self):
        self.Items: List[Dict[str, Any]] = [
            {'date': '2020-08-01', 'row': 'первый', 'value': 1},
            {'date': '2020-08-03', 'row': 'первый', 'value': 2},
            {'date': '2020-08-04', 'row': 'первый', 'value': 3},
            {'date': '2020-08-05', 'row': 'первый', 'value': 5},
            {'date': '2020-08-06', 'row': 'первый', 'value': 4},
            {'date': '2020-08-07', 'row': 'первый', 'value': 3},
            {'date': '2020-08-01', 'row': 'второй', 'value': 1},
            {'date': '2020-08-03', 'row': 'второй', 'value': 2},
            {'date': '2020-08-04', 'row': 'второй', 'value': 3},
            {'date': '2020-08-09', 'row': 'второй', 'value': 4},
            {'date': '2020-08-22', 'row': 'второй', 'value': 8},
        ]

        # Настройки полей
        self.Fields: List[Dict[str, Any]] = [
            {'field': 'date', 'type': 'period', 'format': 'd.m'},
            {'field': 'row', 'type': 'row'},
            {'field': 'value', 'type': 'value'},
        ]

        # Стилей можно вынести в отдельный класс или словарь, если надо переиспользовать
        self.TableStyle: str = (
                "border: 1px solid #32383e; "
                "border-collapse: collapse; "
                "font-size: 12px; "
                "margin: 0 auto; "
                "font-family: Calibri, Candara, 'Segoe UI', Optima, Arial, sans-serif;"
        )
        self.TDStyle: str = "border: 1px solid  #858585; text-align: center; padding-left: 3px; padding-right: 3px; font-family: Calibri, Candara, 'Segoe UI', Optima, Arial, sans-serif;"
        self.THStyle: str = "color: #fff; border: 1px solid #32383e; text-align: center; padding-left: 3px; padding-right: 3px; background-color: #43484c; font-family: Calibri, Candara, 'Segoe UI', Optima, Arial, sans-serif;"
        self.TRStyle: str = ""
        self.TitleStyle: Optional[str] = None
        self.RoundValue: Optional[int] = None
        self.RowTitleStyle: str = "font-weight: bold;"
        self.ShowSummaryXAxis: bool = True
        self.ShowSummaryYAxis: bool = True
        self.AllProjectsData: List[Dict[str, Any]] = []
        self.Results: List[Dict[str, Any]] = []
        self.ShowDoD: bool = False
        self.ShowRank: bool = False

        self.HideRow: bool = False

        self.SliceCount: bool = True
        self.ShowId: bool = True
        self.topCount: int = 20
        self.ShowTop20: bool = False
        self.AddEmptyColumnCount: int = 0
        self.ShowProjectTitle: bool = False
        self.dod = None
        self.ShowAverageYAxis: bool = True
        self.ShowCalculated: bool = False
        self.ShowAvgCalculated: bool = False
        self.ShowTotalPercentage: bool = False
        self.AdvancedAverageMultiplication: int = 100
        self.minDate: Optional[str] = None
        self.maxDate: Optional[str] = None
        self.DescriptionColumns: Optional[List[Dict[str, Any]]] = None

        # Тип сортировки по total (none|desc|asc)
        self.OrderByTotal: str = 'none'
        self.OrderByAverage: str = 'none'
        self.OrderByLastDate: str = 'none'
        self.LastDateFallbackZero: bool = True

        self.ShowMinYAxis: bool = True
        self.ShowMaxYAxis: bool = True

        # Тип закраски (none|desc|asc)
        self.PaintType: str = 'desc'
        self._AdvancedAverageOrder: Optional[str] = None
        self._AdvancedAverage: bool = False
        self._AdvancedAverageArray: List[Dict[str, Any]] = []
        self._AggregatedByRow: List[Dict[str, Any]] = []

        self._TableBody: str = ''
        self._RowData: str = ''
        self._DatePeriod: List[Dict[str, str]] = []
        self._RowIndex: int = 0
        self._Rows: List[str] = []
        self._Summary: List[float] = []
        self._CalculateItems: List[Union[float, str]] = []
        self._AllProjectItems: List[Dict[str, Any]] = []
        self._AverageX: List[Any] = []
        self._MinValue: Optional[float] = None
        self._MaxValue: Optional[float] = None
        self._RowFieldName: str = ''
        self._ValueFieldName: str = ''
        self._PeriodFieldName: str = ''
        self._PeriodFieldFormat: Optional[str] = None

        self.PeriodFieldTitle: str = "Период"
        self.NumerationTitle: str = "Rank"
        self._NumeratorTitle: str = ""
        self._DenominatorTitle: str = ""
        self.coffee = False
        self._by_row_date = None

    def getTable(self) -> str:
        """
        Генерирует и возвращает HTML-разметку таблицы.
        """
        self._constructBody()
        return self._renderTable()

    def _constructBody(self) -> None:
        """
        Основная логика построения тела таблицы.
        """
        # Если включена опция ShowTop20, обрезаем данные
        if self.ShowTop20:
            self.Items = self._showDiffRank(self.Items)

        # Если нужно вывести рассчитанные данные
        if self.ShowCalculated:
            if self.AllProjectsData:
                self._AllProjectItems = self.AllProjectsData

        # Подготавливаем все необходимые структуры (Dates, Min/Max, Summary, ...)
        self._makePreparation()

        # Цвет для заголовков
        styleTitleColor = self.TitleStyle if self.TitleStyle else None

        # Вспомогательные ячейки
        if self.ShowRank:
            self._genCellTH(self.NumerationTitle)
        if self.ShowDoD:
            self._genCellTH('DoD')

        # Заголовок с Периодом
        if self.ShowId:
            self._genCellTH(self.PeriodFieldTitle, styleTitleColor)
        # Отображение «Проект», если надо
        if self.ShowProjectTitle:
            self._genCellTH('Проект')

        if not self.HideRow:
            row_field = next((f for f in self.Fields if f.get('type') == 'row'), None)
            row_title = (row_field.get('title') if row_field and 'title' in row_field else 'Причина')
            self._genCellTH(row_title, self.TitleStyle)

        # Доп. описание столбцов
        if self.DescriptionColumns:
            for descriptionColumn in self.DescriptionColumns:
                style_add_color = descriptionColumn.get('style', None)
                self._genCellTH(descriptionColumn['title'], style_add_color)

        # Генерация заголовков (дат)
        for date_item in self._DatePeriod:
            if self._PeriodFieldFormat is not None:
                self._genCellTH(date_item['formatted'])
            else:
                self._genCellTH(date_item['dateTime'])

        # Итого/Доля
        if self.ShowSummaryYAxis:
            self._genCellTH('Итого')
            if self.ShowTotalPercentage:
                self._genCellTH('Доля %')

        # Среднее/Мин/Макс
        if self.ShowAverageYAxis:
            self._genCellTH('Сред.')
        if self.ShowMinYAxis:
            self._genCellTH('Mин.')
        if self.ShowMaxYAxis:
            self._genCellTH('Макс.')

        self._genRow()

        i = 0
        # Генерация строк таблицы
        for row_value in self._Rows:
            row_array = []
            if self.ShowRank:
                i += 1
                self._genCellTD(i, "font-weight: bold; width:50px")

            if self.ShowDoD:
                sum_dod = 0
                for date_item in self._DatePeriod:
                    for item in self.Items:
                        if (row_value == item[self._RowFieldName]
                                and self._datesEqual(date_item[self._PeriodFieldName],
                                                     item[self._PeriodFieldName])):
                            sum_dod = item.get('dod', 0)

                # Определяем строку на основе значения sum_dod
                if sum_dod == 100:
                    doD_render = "<span style='font-size:10px;color:#1f77b4;white-space:nowrap'>new</span>"
                elif 0 < sum_dod <= 99:
                    doD_render = (
                        f"<span style='white-space:nowrap'>"
                        f"<span style='font-size:16px;color:#2ecc71;margin-right:4px'>&#9650;</span>"
                        f"+{sum_dod}</span>"
                    )
                elif sum_dod < 0:
                    doD_render = (
                        f"<span style='white-space:nowrap'>"
                        f"<span style='font-size:16px;color:#e74c3c;margin-right:4px'>&#9660;</span>"
                        f"{sum_dod}</span>"
                    )
                else:
                    doD_render = "<span style='font-size:16px;color:#7f8c8d;white-space:nowrap'>&mdash;</span>"


                self._genCellTD(doD_render, "font-weight: bold; width:50px")
            else:
                if not self.HideRow:
                    self._genCellTD(row_value, self.RowTitleStyle)

            if self.DescriptionColumns:
                for desc_col in self.DescriptionColumns:
                    style_add_color = desc_col.get('style', None)
                    add_row_data = self._findFirstPair(row_value, desc_col['field'])
                    self._genCellTD(add_row_data, style_add_color)

            for date_item in self._DatePeriod:
                dt = str(date_item[self._PeriodFieldName])[:10]  # YYYY-MM-DD
                has_value = (self._by_row_date is not None
                             and row_value in self._by_row_date
                             and dt in self._by_row_date[row_value])

                if has_value:
                    val = self._by_row_date[row_value][dt]
                    if isinstance(val, float) and math.isnan(val):
                        row_array.append(None)
                        self._genCellTD('')
                        continue

                    # цвет
                    if self.coffee:
                        if val < 20:
                            colour = 'white'
                        elif val <= 30:
                            colour = '#FFC0CB'
                        else:
                            colour = '№f73e5e'
                    else:
                        colour = self._gradientColour(val, self._MinValue, self._MaxValue, self.PaintType)

                    # округление/отображение
                    if self.RoundValue is not None:
                        rounded = round(val, self.RoundValue)
                        display_val = int(rounded) if isinstance(rounded, float) and rounded.is_integer() else rounded
                    else:
                        display_val = val

                    self._genCellTD(smart_number(display_val), f"background-color: {colour};")
                    row_array.append(val)
                else:
                    row_array.append(None)
                    self._genCellTD('')

            # Удаляем все None, чтобы корректно считать сумму
            row_array = [
                x for x in row_array
                if x is not None and not (isinstance(x, float) and math.isnan(x))
            ]
            if not row_array:
                min_val = max_val = avg_val = None
            else:
                min_val = min(row_array)
                max_val = max(row_array)
                avg_val = round(sum(row_array) / len(row_array), 1)

            # Итоги по строке
            if self.ShowSummaryYAxis:
                total = sum(row_array) if row_array else 0.0
                # если целое — int, иначе с одним знаком
                if isinstance(total, float) and total.is_integer():
                    display_total = int(total)
                else:
                    display_total = round(total, 1)
                self._genCellTD(display_total, "font-weight: bold;")
                if self.ShowTotalPercentage and sum(self._Summary) != 0:
                    part = 0
                    if row_array:
                        part = round(sum(row_array) / sum(self._Summary) * 100)
                    self._genCellTD(part, "font-weight: bold;")

            if self.ShowAverageYAxis:
                if self._AdvancedAverage:
                    # Поиск усреднённой метрики для этой строки
                    adv_value = None
                    for adv_item in self._AdvancedAverageArray:
                        if adv_item['row'] == row_value:
                            if self.RoundValue is not None and adv_item['advancedAverage'] is not None:
                                adv_value = round(adv_item['advancedAverage'], self.RoundValue)
                            else:
                                adv_value = adv_item['advancedAverage']
                            self._genCellTD(smart_number(adv_value), "font-weight: bold;")
                            break
                else:
                    self._genCellTD(smart_number(avg_val), "font-weight: bold;")

            if self.ShowMinYAxis:
                if min_val is None:
                    display_min = ''
                elif isinstance(min_val, float) and min_val.is_integer():
                    display_min = int(min_val)
                else:
                    display_min = round(min_val, 1)
                self._genCellTD(display_min, "font-weight: bold;")

            if self.ShowMaxYAxis:
                if max_val is None:
                    display_max = ''
                elif isinstance(max_val, float) and max_val.is_integer():
                    display_max = int(max_val)
                else:
                    display_max = round(max_val, 1)
                self._genCellTD(display_max, "font-weight: bold;")

            self._genRow()

        # Итоги по оси X (сумма по столбцам)
        if self.ShowSummaryXAxis:

            lead = 0
            if self.ShowRank:
                lead += 1
            if self.ShowDoD:
                lead += 1
            if self.ShowId:
                lead += 1
            if self.ShowProjectTitle:
                lead += 1
            if not self.HideRow:
                lead += 1
            if self.DescriptionColumns:
                lead += (self.AddEmptyColumnCount or 0) + len(self.DescriptionColumns)

            self._genCellTH('Итого')

            for _ in range(max(0, lead - 1)):
                self._genCellTH('')

            # Для каждого дня/столбца выводим итог
            for summary_val in self._Summary:
                if isinstance(summary_val, float) and summary_val.is_integer():
                    disp = int(summary_val)
                else:
                    disp = round(summary_val, 1)
                self._genCellTH(disp)

            # Общий итог
            if self.ShowSummaryYAxis:
                total_x = sum(self._Summary)
                if isinstance(total_x, float) and total_x.is_integer():
                    disp_total_x = int(total_x)
                else:
                    disp_total_x = round(total_x, 1)
                self._genCellTH(disp_total_x)

                if self.ShowTotalPercentage:
                    self._genCellTH('100')

            # Среднее по X, если надо
            if self.ShowAverageYAxis and len(self._Summary) > 0:
                self._genCellTH(round(sum(self._Summary) / len(self._Summary), 1))

            # Min/Max
            if self.ShowMinYAxis:
                mn = min(self._Summary) if self._Summary else None
                if isinstance(mn, float) and mn.is_integer():
                    self._genCellTH(int(mn))
                else:
                    self._genCellTH(round(mn, 1) if mn is not None else '')
            if self.ShowMaxYAxis:
                mx = max(self._Summary) if self._Summary else None
                if isinstance(mx, float) and mx.is_integer():
                    self._genCellTH(int(mx))
                else:
                    self._genCellTH(round(mx, 1) if mx is not None else '')

            self._genRow()

        # Если нужно добавить вычисленные показатели
        if self.ShowCalculated:
            # Итого или Среднее
            label = 'Cред.' if self.ShowAvgCalculated else 'Итого.'
            self._genCellTH(label)

            for item in self._CalculateItems:
                self._genCellTH(item)

            if self.ShowSummaryYAxis:
                numeric_vals = [x for x in self._CalculateItems if isinstance(x, (int, float))]
                self._genCellTH(round(sum(numeric_vals), 1) if numeric_vals else 0)

            if self.ShowTotalPercentage:
                self._genCellTH('100')

            filtered_items = [x for x in self._CalculateItems if isinstance(x, (int, float))]
            if filtered_items:
                avg_y = sum(filtered_items) / len(filtered_items)
                if self.ShowAverageYAxis:
                    self._genCellTH(round(avg_y, 1))

            if self.ShowMinYAxis and self._CalculateItems:
                num_vals = [x for x in self._CalculateItems if isinstance(x, (int, float))]
                if num_vals:
                    # округляем до одного знака после запятой (или self.RoundValue)
                    min_val = round(min(num_vals), 1)
                else:
                    min_val = None
                self._genCellTH(min_val)

            if self.ShowMaxYAxis and self._CalculateItems:
                num_vals = [x for x in self._CalculateItems if isinstance(x, (int, float))]
                if num_vals:
                    max_val = round(max(num_vals), 1)
                else:
                    max_val = None
                self._genCellTH(max_val)

            self._genRow()

        # Если существуют Results (например, Факт/План/Разница)
        if self.Results:
            self._processResults(self.Results, 'Факт', 'fact_operators')
            self._processResults(self.Results, 'План', 'labor')
            self._processResults(self.Results, 'Разница', 'difference')

    def _makePreparation(self) -> None:
        """
        Подготовка необходимых данных: дат, rows, min/max, summary и т.д.
        Аналог PHP makePreparation().
        """
        # Определяем названия полей
        for field in self.Fields:
            if field['type'] == 'period':
                self._PeriodFieldName = field['field']
                self._PeriodFieldFormat = field.get('format', None)
                if 'title' in field:
                    self.PeriodFieldTitle = field['title']
            elif field['type'] == 'row':
                self._RowFieldName = field['field']
            elif field['type'] == 'value':
                self._ValueFieldName = field['field']
            if 'advancedAverage' in field:
                self._AdvancedAverage = True
                self._NumeratorTitle = field['advancedAverage']['numerator']
                self._DenominatorTitle = field['advancedAverage']['denominator']
                if 'order' in field['advancedAverage']:
                    self._AdvancedAverageOrder = field['advancedAverage']['order']

        # Диапазон дат
        all_dates = [x[self._PeriodFieldName] for x in self.Items]
        min_date = self.minDate if self.minDate else (min(all_dates) if all_dates else None)
        max_date = self.maxDate if self.maxDate else (max(all_dates) if all_dates else None)

        # Генерация массива дат (DatePeriod)
        if self._PeriodFieldName == "hour":
            self._DatePeriod = self._getHoursTrunkedData(min_date, max_date, self._PeriodFieldFormat)
        elif self._PeriodFieldName == "date":
            self._DatePeriod = self._getDaysTrunkedData(min_date, max_date, self._PeriodFieldFormat)
        elif self._PeriodFieldName == "month":
            self._DatePeriod = self._getMonthsTrunkedData(min_date, max_date, self._PeriodFieldFormat)
        else:
            # Если поле называется как-то иначе, можно адаптировать или просто оставить пустой массив
            self._DatePeriod = []

        # Список уникальных значений Rows
        vals = {x[self._RowFieldName] for x in self.Items if x.get(self._RowFieldName) is not None}
        self._Rows = sorted(vals, key=lambda v: str(v))

        # Определяем Min/Max
        vals = []
        for x in self.Items:
            v = x.get(self._ValueFieldName)
            if v is None:
                continue
            if isinstance(v, float) and math.isnan(v):
                continue
            vals.append(v)
        if vals:
            self._MinValue = min(vals)
            self._MaxValue = max(vals)
        else:
            self._MinValue = None
            self._MaxValue = None

        # Подсчёт итога по каждому столбцу (дню/часу/месяцу)
        if self.ShowSummaryXAxis and self._DatePeriod:
            for date_item in self._DatePeriod:
                s = 0.0
                for it in self.Items:
                    if self._datesEqual(date_item[self._PeriodFieldName], it[self._PeriodFieldName]):
                        val = it.get(self._ValueFieldName, 0)
                        if isinstance(val, float) and math.isnan(val):
                            pass
                        elif isinstance(val, (int, float)):
                            s += val
                self._Summary.append(s)

        # Если нужно отобразить «рассчитанные» значения (AllProjectsData -> CalculateItems)
        if self.ShowCalculated and self._DatePeriod:
            for date_item in self._DatePeriod:
                calc = None
                for prj_item in self._AllProjectItems:
                    if self._datesEqual(date_item[self._PeriodFieldName], prj_item[self._PeriodFieldName]):
                        calc = prj_item.get(self._ValueFieldName)
                if calc is not None:
                    self._CalculateItems.append(round(calc, 1))
                else:
                    self._CalculateItems.append('')

        # Расчёт «AdvancedAverage», если задано в Fields
        if self._AdvancedAverage:
            for row_value in self._Rows:
                numerator = 0.0
                denominator = 0.0
                for item in self.Items:
                    if item[self._RowFieldName] == row_value:
                        numerator += item.get(self._NumeratorTitle, 0)
                        denominator += item.get(self._DenominatorTitle, 0)
                if numerator == 0 and denominator == 0:
                    adv = None
                elif numerator == 0:
                    adv = 0
                elif denominator == 0:
                    adv = 100
                else:
                    adv = (numerator / denominator) * self.AdvancedAverageMultiplication

                self._AdvancedAverageArray.append({
                    'row': row_value,
                    'numerator': numerator,
                    'denominator': denominator,
                    'advancedAverage': adv,
                })

        # Сортировка строк по AdvancedAverage, если нужно
        if self._AdvancedAverageOrder:
            adv_col = [x['advancedAverage'] for x in self._AdvancedAverageArray]
            # делаем сортировку
            reverse = (self._AdvancedAverageOrder == 'desc')
            # Сортируем массив (advancedAverageArray) по столбцу advancedAverage
            self._AdvancedAverageArray.sort(key=lambda x: x['advancedAverage'] if x['advancedAverage'] is not None else -999999,  # None в конец
                                            reverse=reverse)
            # Обновляем _Rows в том же порядке
            self._Rows = [x['row'] for x in self._AdvancedAverageArray]

        # Сортировка по итогу (OrderByTotal)
        if self.OrderByTotal != 'none':
            self._aggregateByRow()
            # Сортируем _AggregatedByRow по 'sum'
            reverse = (self.OrderByTotal == 'desc')
            self._AggregatedByRow.sort(key=lambda x: x['sum'] if x['sum'] is not None else 0, reverse=reverse)
            self._Rows = [x['row'] for x in self._AggregatedByRow]

        # Сортировка по среднему (OrderByAverage)
        if self.OrderByAverage != 'none':
            if not self._AggregatedByRow:
                self._aggregateByRow()
            reverse = (self.OrderByAverage == 'desc')
            self._AggregatedByRow.sort(key=lambda x: x['avg'] if x['avg'] is not None else 0, reverse=reverse)
            self._Rows = [x['row'] for x in self._AggregatedByRow]
        from collections import defaultdict

        by_row_date = defaultdict(lambda: defaultdict(float))
        for it in self.Items:
            row = it.get(self._RowFieldName)
            d = it.get(self._PeriodFieldName)
            v = it.get(self._ValueFieldName)
            if row is None or d is None:
                continue
            dkey = str(d)[:10]  # 'YYYY-MM-DD' (работает и для 'YYYY-MM-DD HH:MM:SS')
            if isinstance(v, (int, float)):
                if isinstance(v, float) and math.isnan(v):
                    pass  # игнорируем
                else:
                    by_row_date[row][dkey] += v
            elif v == 0:
                by_row_date[row][dkey] += 0.0

        self._by_row_date = by_row_date

        agg_vals = []
        for rowmap in by_row_date.values():
            for val in rowmap.values():
                if isinstance(val, float) and math.isnan(val):
                    continue
                agg_vals.append(val)
        if agg_vals:
            self._MinValue = min(agg_vals)
            self._MaxValue = max(agg_vals)
        if self.OrderByLastDate != 'none' and self._DatePeriod and self._by_row_date is not None:
            # последняя дата периода в формате 'YYYY-MM-DD'
            last_key = max(d[self._PeriodFieldName][:10] for d in self._DatePeriod)

            def _score(row):
                v = self._by_row_date.get(row, {}).get(last_key, None)
                if v is None:
                    # либо считаем 0, либо отправляем в конец (см. ветку ниже)
                    return 0 if self.LastDateFallbackZero else float('-inf')
                return v

            reverse = (self.OrderByLastDate == 'desc')

            if self.LastDateFallbackZero:
                # обычная сортировка по числу (нет значения -> 0)
                self._Rows.sort(key=_score, reverse=reverse)
            else:
                # хотим, чтобы None улетал в конец и при asc, и при desc
                def _key2(row):
                    v = self._by_row_date.get(row, {}).get(last_key, None)
                    is_none = (v is None)
                    # для desc инвертируем знак, чтобы обходиться reverse=False
                    num = (-v if (v is not None and reverse) else (v if v is not None else 0))
                    return (is_none, num)

                self._Rows.sort(key=_key2, reverse=False)
    def _aggregateByRow(self) -> None:
        """
        Подсчитывает сумму, мин/макс и среднее по каждой «строке» (row),
        чтобы потом сортировать по ним.
        """
        self._AggregatedByRow = []
        for row_value in self._Rows:
            row_vals = []
            count_vals = 0
            for date_item in self._DatePeriod:
                for it in self.Items:
                    if (row_value == it[self._RowFieldName]
                            and self._datesEqual(date_item[self._PeriodFieldName],
                                                 it[self._PeriodFieldName])):
                        val = it.get(self._ValueFieldName)
                        if val is not None and not (isinstance(val, float) and math.isnan(val)):
                            row_vals.append(val)
                            count_vals += 1

            if not row_vals:
                row_min = row_max = row_sum = None
                row_avg = None
            else:
                row_min = min(row_vals)
                row_max = max(row_vals)
                row_sum = sum(row_vals)
                row_avg = round(row_sum / count_vals, 1)

            self._AggregatedByRow.append({
                'row': row_value,
                'min': row_min,
                'max': row_max,
                'avg': row_avg,
                'sum': row_sum
            })

    def _genCellTD(self, cellData: Any, style: Optional[str] = None) -> None:
        """
        Аналог PHP genCellTD для добавления <td>.
        """
        td_data = f'<td style="{self.TDStyle} {style if style else ""}">{cellData}</td>'
        self._RowData += td_data

    def _genCellTH(self, cellData: str, style: Optional[str] = None) -> None:
        """
        Аналог PHP genCellTH для добавления <th>.
        """
        th_data = f'<th style="{self.THStyle} {style if style else ""}">{cellData}</th>'
        self._RowData += th_data

    def _genRow(self, customRowData: Optional[str] = None) -> None:
        """
        Аналог PHP genRow для закрытия <tr> и добавления его в _TableBody.
        """
        if customRowData is None:
            customRowData = self._RowData
        self._RowIndex += 1
        # Подсветка чётной/нечётной строки
        backColour = "background-color: #f2f2f2;" if (self._RowIndex % 2) else ""
        row_html = f'<tr style="{backColour}{self.TRStyle}">{customRowData}</tr>'
        self._addDataToTableBody(row_html)
        self._RowData = ''

    def _getRows(self):
        return self._Rows

    def _addDataToTableBody(self, data: str) -> None:
        """
        Добавляет любой HTML-код (обычно <tr>...</tr>) в тело таблицы.
        """
        self._TableBody += data

    def _findFirstPair(self, valueParent: str, field: str) -> Any:
        """
        Поиск первого попавшегося элемента (row==valueParent) и возвращаем его field.
        Аналог PHP findFirstPair().
        """
        for it in self.Items:
            if it[self._RowFieldName] == valueParent:
                return it.get(field, 'N/F')
        return 'N/F'

    def _gradientColour(self, current: float, min_val: Optional[float], max_val: Optional[float],
                        ctype: str = 'desc') -> str:
        """
        Аналог PHP gradientColour (определение цвета ячейки на основе значения).
        """
        # Упрощённая таблица цветов. При желании можно усложнить/сделать динамической.
        colours = {
            0: '',
            10: '',
            20: '#FFEBEE',
            30: '#FFD7DE',
            40: '#FFC3CE',
            50: '#FFAFBE',
            60: '#FF9CAE',
            70: '#FF889E',
            80: '#FF748E',
            90: '#FF607E',
            100: '#FF4D6E',
        }
        if min_val is None or max_val is None or (max_val - min_val == 0):
            return ''

        # Рассчитываем процент
        percent = round((current - min_val) / (max_val - min_val), 1) * 100
        if ctype == 'desc':
            percent = 100 - percent
        # Приводим к ближайшему целому «шагу» в ключах словаря (кратно 10)
        keys_list = sorted(colours.keys())
        closest_key = min(keys_list, key=lambda x: abs(x - percent))
        return colours.get(closest_key, '')

    def _renderTable(self) -> str:
        """
        Аналог PHP renderTable
        """
        return f'<table style="{self.TableStyle}">{self._TableBody}</table>'

    def _datesEqual(self, dateA: str, dateB: str) -> bool:
        """
        Сравнение дат (строки) в формате ГГГГ-ММ-дд (или другом) — можно доработать.
        """
        # Для надёжности пытаемся привести к одному формату YYYY-MM-DD
        try:
            dA = datetime.strptime(dateA[:10], "%Y-%m-%d")
            dB = datetime.strptime(dateB[:10], "%Y-%m-%d")
            return dA.date() == dB.date()
        except ValueError:
            # Если парсинг не удался, сравним как есть
            return dateA == dateB

    def _coerce_date_only(self, x):
        if x is None:
            return None
        if isinstance(x, datetime):
            return datetime(x.year, x.month, x.day)
        if isinstance(x, date):
            return datetime(x.year, x.month, x.day)
        # строки: берём первые 10 символов 'YYYY-MM-DD'
        s = str(x)[:10]
        return datetime.strptime(s, "%Y-%m-%d")

    def _getDaysTrunkedData(self, min_date: str, max_date: str, fmt: Optional[str]) -> List[Dict[str, str]]:
        """
        Примерная реализация getDaysTrunkedData() из DateRange:
        Генерируем список дней в промежутке [min_date, max_date].
        """
        result = []
        try:
            start = self._coerce_date_only(min_date)
            end = self._coerce_date_only(max_date)
        except (ValueError, TypeError):
            return result  # если дата невалидная

        delta = end - start
        for i in range(delta.days + 1):
            curr = start + timedelta(days=i)
            # поле dateTime и поле formatted (по аналогии с PHP)
            date_str = curr.strftime("%Y-%m-%d")
            if fmt:
                formatted_str = curr.strftime(fmt)
            else:
                formatted_str = date_str
            result.append({
                'dateTime': date_str,
                'formatted': formatted_str,
                self._PeriodFieldName: date_str
            })
        return result

    def _getHoursTrunkedData(self, min_date: str, max_date: str, fmt: Optional[str]) -> List[Dict[str, str]]:
        """
        Примерная реализация getHoursTrunkedData().
        Генерируем диапазон по часам.
        """
        result = []
        try:
            start = datetime.strptime(min_date, "%Y-%m-%d %H:%M:%S")
            end = datetime.strptime(max_date, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            # Если не указаны часы, пытаемся добавить "00:00:00"
            try:
                start = datetime.strptime(min_date + " 00:00:00", "%Y-%m-%d %H:%M:%S")
                end = datetime.strptime(max_date + " 00:00:00", "%Y-%m-%d %H:%M:%S")
            except Exception:
                return result

        current = start
        while current <= end:
            date_str = current.strftime("%Y-%m-%d %H:%M:%S")
            if fmt:
                formatted_str = current.strftime(fmt)
            else:
                formatted_str = date_str
            result.append({
                'dateTime': date_str,
                'formatted': formatted_str,
                self._PeriodFieldName: date_str
            })
            current += timedelta(hours=1)

        return result

    def _getMonthsTrunkedData(self, min_date: str, max_date: str, fmt: Optional[str]) -> List[Dict[str, str]]:
        """
        Примерная реализация getMonthsTrunkedData().
        Генерируем список месяцев.
        """
        result = []
        try:
            start = datetime.strptime(min_date, "%Y-%m-%d")
            end = datetime.strptime(max_date, "%Y-%m-%d")
        except (ValueError, TypeError):
            return result

        current = datetime(start.year, start.month, 1)
        last = datetime(end.year, end.month, 1)

        while current <= last:
            date_str = current.strftime("%Y-%m-%d")
            if fmt:
                formatted_str = current.strftime(fmt)
            else:
                formatted_str = date_str
            result.append({
                'dateTime': date_str,
                'formatted': formatted_str,
                self._PeriodFieldName: date_str
            })
            # Переходим к первому числу следующего месяца
            year_inc = current.year + (current.month // 12)
            month_inc = (current.month % 12) + 1
            current = datetime(year_inc, month_inc, 1)

        return result

    def _showDiffRank(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Аналог PHP showDiffRank(). В оригинале работает с объектами (->id, ->date, ->count).
        Здесь предполагается, что в словаре должны быть 'id', 'date', 'count'.
        Можете адаптировать под вашу реальную структуру.
        """
        titleCounts = {}
        lastDate = None

        # Ищем максимальную дату и суммируем count по id
        for it in items:
            if 'id' not in it or 'count' not in it:
                # если структура иная, пропускаем
                continue

            if lastDate is None or it['date'] > lastDate:
                lastDate = it['date']

            item_id = it['id']
            titleCounts[item_id] = titleCounts.get(item_id, 0) + it['count']

        # Сортируем по убыванию
        sorted_ids = sorted(titleCounts.items(), key=lambda x: x[1], reverse=True)

        if self.SliceCount:
            # Обрезаем topCount
            sorted_ids = sorted_ids[:self.topCount]

        # Собираем только нужных
        allow_ids = [x[0] for x in sorted_ids]
        top20PerDay = []
        for it in items:
            if it.get('id') in allow_ids:
                it['dod'] = 0  # Инициализация
                top20PerDay.append(it)

        # Сортировка по убыванию даты/count
        top20PerDay = self._sortByLastColumn(top20PerDay)

        if not lastDate:
            return top20PerDay

        try:
            last_dt = datetime.strptime(lastDate, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            last_dt = datetime.strptime(lastDate, "%Y-%m-%d")
        prev_dt = last_dt - timedelta(days=1)

        lastDayDateTime = last_dt.strftime("%Y-%m-%d 00:00:00")
        prevLastDayDateTime = prev_dt.strftime("%Y-%m-%d 00:00:00")

        lastDay = []
        prevLastDay = []
        otherDaysItem = {}

        for it in top20PerDay:
            if it['date'] == lastDayDateTime:
                lastDay.append(it)
            elif it['date'] == prevLastDayDateTime:
                prevLastDay.append(it)
            else:
                otherDaysItem[it['id']] = it['id']

        lastDay = self._sortByCountDesc(lastDay)
        prevLastDay = self._sortByCountDesc(prevLastDay)

        prevLastDayById = {}
        for idx, v in enumerate(prevLastDay):
            prevLastDayById[v['id']] = idx

        for idx, v in enumerate(lastDay):
            if v['id'] in prevLastDayById:
                v['dod'] = prevLastDayById[v['id']] - idx
            elif v['id'] in otherDaysItem:
                v['dod'] = 0
            else:
                v['dod'] = 100  # новый элемент

        # Обновляем top20PerDay
        lastDayById = {x['id']: x for x in lastDay}
        for it in top20PerDay:
            if (it['id'] in lastDayById) and (it['date'] == lastDayDateTime):
                it['dod'] = lastDayById[it['id']]['dod']

        return top20PerDay

    def _sortByLastColumn(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Аналог PHP sortByLastColumn()
        Сортируем сначала по дате (убывание), потом по count (убывание).
        """
        def sort_func(x):
            # Пытаемся сравнить по дате
            dt_str = x.get('date', '')
            try:
                dt_val = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                try:
                    dt_val = datetime.strptime(dt_str, "%Y-%m-%d")
                except ValueError:
                    dt_val = datetime.min
            count_val = x.get('count', 0)
            # Минус перед timestamp/count, чтобы сортировать по убыванию
            return (-dt_val.timestamp(), -count_val)

        items.sort(key=sort_func)
        return items

    def _sortByCountDesc(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Аналог PHP sortByCountDesc().
        Сортируем по убыванию count.
        """
        items.sort(key=lambda x: x.get('count', 0), reverse=True)
        return items

    def _processResults(self, results: List[Dict[str, Any]], label: str, key: str) -> None:
        """
        Аналог PHP processResults(). Рисуем строку (Факт/План/Разница).
        """
        self._genCellTH(label)
        values = []
        for r in results:
            val = r.get(key, 0)
            self._genCellTH(val)
            try:
                values.append(round(float(val)))
            except (ValueError, TypeError):
                pass

        if values:
            avg_y = sum(values) / len(values)
            if self.ShowAverageYAxis:
                self._genCellTH(round(avg_y, 1))

            if self.ShowMinYAxis:
                self._genCellTH(min(values))

            if self.ShowMaxYAxis:
                self._genCellTH(max(values))

        self._genRow()