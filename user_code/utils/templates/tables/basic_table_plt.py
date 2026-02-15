import matplotlib.pyplot as plt
import datetime

class BasicTable:
    """
    Аналог PHP/Python BasicTable, но для вывода таблицы через matplotlib.
    Вместо HTML-таблицы формирует визуализацию таблицы с окраской ячеек,
    итоговой строкой и форматированием данных.
    """

    def __init__(self):
        # Исходные данные — список словарей с данными для строк
        self.Items = []

        # Описание полей (столбцов): title, ключ field, настройки (окрашивание, формат, итоги)
        self.Fields = []

        # Нужно ли показывать итоговую строку (суммы, максимумы и др.)
        self.ShowSummaryXAxis = False

        # Хранит агрегации (min, max, sum) по полям для окрашивания и итогов
        self._AggregatedData = {}

    def getTable(self):
        """
        Основной метод, который запускает подготовку данных и отрисовку.
        Возвращает объект matplotlib.figure.Figure с отрисованной таблицей.
        """
        self._constructBody()
        return self._renderTable()

    def _constructBody(self):
        """
        Подготовка тела таблицы:
        - Считаем агрегаты (min, max, sum) для окраски и итогов (_makePreparation)
        - Формируем список заголовков (self.headers) и валидных полей (self.valid_fields)
        - Формируем строки данных (self.rows), применяя форматирование значений
        - Формируем итоговую строку, если требуется
        """
        self._makePreparation()

        # Определяем заголовки и поля, по которым есть данные
        self.headers = []
        self.valid_fields = []
        for field in self.Fields:
            all_values = [x.get(field['field']) for x in self.Items]
            if any(v is not None for v in all_values):
                self.headers.append(field['title'])
                self.valid_fields.append(field)

        # Формируем строки данных — список списков, где каждый внутренний список — значения ячеек строки
        self.rows = []
        for item in self.Items:
            row = []
            for field in self.valid_fields:
                value = item.get(field['field'], None)
                if value is not None:
                    value = self._formatValue(value, field)  # форматируем (округляем, формат времени и т.д.)
                else:
                    if field.get('time_format'):
                        value = '00:00'  # если поле — время, а данных нет, пишем 00:00
                    else:
                        value = ''
                row.append(value)
            self.rows.append(row)

        # Формируем итоговую строку с агрегатами, если она нужна
        if self.ShowSummaryXAxis:
            summary_row = []
            for field in self.valid_fields:
                if 'summary' in field:
                    summ_val = self._AggregatedData[field['field']].get(field['summary'], '')
                    summ_val = self._formatValue(summ_val, field) if summ_val != '' else ''
                    summary_row.append(summ_val)
                elif 'advancedSummaryAvg' in field:
                    avg_adv = self._AggregatedData[field['field']].get('avgAdvanced', '')
                    if field.get('time_format') and avg_adv:
                        avg_adv = self._secondsToMinutes(self._toInt(avg_adv))
                    else:
                        avg_adv = self._formatValue(avg_adv, field) if avg_adv != '' else ''
                    summary_row.append(avg_adv)
                elif 'avgFor24' in field:
                    avg_adv = self._AggregatedData[field['field']].get('avgAdvanced', '')
                    avg_adv = self._formatValue(avg_adv, field) if avg_adv != '' else ''
                    summary_row.append(avg_adv)
                else:
                    summary_row.append('')
            self.rows.append(summary_row)  # итог добавляем как последнюю строку

    def _formatValue(self, value, field):
        """
        Форматирование значения по правилам:
        - округление (если указано 'round')
        - преобразование чисел с .0 в целые
        - формат времени (секунды -> MM:SS)
        - формат даты (если 'date_format')
        Возвращает строку для отображения.
        """
        try:
            if 'round' in field:
                rounded = round(float(value), field['round'])
            if rounded.is_integer():
                value = int(rounded)
            else:
                value = rounded
        except:
            pass

        if field.get('time_format'):
            value = self._secondsToMinutes(self._toInt(value))

        if 'date_format' in field:
            try:
                dt = datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
            except:
                try:
                    dt = datetime.datetime.strptime(value, "%Y-%m-%d")
                except:
                    dt = None
            if dt:
                value = dt.strftime(field['date_format'])

        return str(value)

    def _makePreparation(self):
        """
        Считает для каждого поля с 'paint' или 'summary' минимум, максимум и сумму,
        которые используются для окрашивания и итоговой строки.
        Также выставляет флаг ShowSummaryXAxis, если нужны итоги.
        """
        self._AggregatedData = {}
        self.ShowSummaryXAxis = False

        for field in self.Fields:
            has_paint = ('paint' in field) or ('summary' in field)
            if has_paint:
                arr = []
                for it in self.Items:
                    val = it.get(field['field'])
                    f_val = self._toFloat(val)
                    if f_val is not None:
                        arr.append(f_val)

                if not arr:
                    arr = [0.0]

                max_val = max(arr)
                min_val = min(arr)
                sum_val = sum(arr)

                self._AggregatedData[field['field']] = {
                    'min': min_val,
                    'max': max_val,
                    'sum': sum_val
                }

            if 'summary' in field or 'advancedSummaryAvg' in field or 'avgFor24' in field:
                self.ShowSummaryXAxis = True

            # Место для дополнительной логики advancedSummaryAvg и avgFor24

    def _toFloat(self, val):
        """Преобразует к float или возвращает None."""
        try:
            return float(val)
        except:
            return None

    def _toInt(self, val):
        """Преобразует к int или возвращает 0."""
        try:
            return int(float(val))
        except:
            return 0

    def _secondsToMinutes(self, seconds):
        """Преобразует секунды в строку формата MM:SS."""
        if seconds is None:
            return "00:00"
        minutes = seconds // 60
        remainder = seconds % 60
        return f"{minutes:02d}:{remainder:02d}"

    def _renderTable(self):
        """
        Отрисовывает таблицу с помощью matplotlib:
        - Создаёт figure и оси, выключает оси
        - Формирует цвета ячеек по градиенту (если paint задан)
        - Добавляет таблицу с данными и заголовками
        - Подсвечивает заголовки и итоговую строку (черный фон, белый текст)
        - Возвращает объект figure (можно показать plt.show() или сохранить)
        """
        fig, ax = plt.subplots(figsize=(len(self.headers), len(self.rows)+2))
        ax.axis('off')

        cell_colours = []

        for row_idx, row in enumerate(self.rows):
            row_colors = []
            for col_idx, cell in enumerate(row):
                field = self.valid_fields[col_idx]

                if 'paint' in field:
                    try:
                        val = float(cell)
                    except:
                        val = None

                    if val is not None:
                        min_val = self._AggregatedData[field['field']]['min']
                        max_val = self._AggregatedData[field['field']]['max']
                        ratio = (val - min_val) / (max_val - min_val) if max_val != min_val else 0

                        r = 1.0
                        g = 0.92 - 0.62 * ratio
                        b = 0.93 - 0.53 * ratio
                        colour = (r, max(min(g,1),0), max(min(b,1),0))
                    else:
                        colour = (1,1,1)
                else:
                    colour = (1,1,1)

                if row_idx % 2 == 1:
                    colour = tuple(min(1, c + 0.1) for c in colour)

                row_colors.append(colour)
            cell_colours.append(row_colors)

        col_width = [0.5] + [1.0] * (len(self.headers) - 1)
        table = ax.table(
            cellText=self.rows,
            colLabels=self.headers,
            cellColours=cell_colours,
            cellLoc='center',
            colWidths=col_width,
            loc='center'
        )
        table.auto_set_font_size(False)
        table.set_fontsize(12)
        table.scale(1, 1.5)

        n_cols = len(self.headers)
        n_rows = len(self.rows)

        black_bg = 'black'
        white_text = 'white'

        for col in range(n_cols):
            cell = table[(0, col)]
            cell.set_facecolor(black_bg)
            cell.get_text().set_color(white_text)

        if self.ShowSummaryXAxis:
            # В matplotlib table первая строка — заголовок (0),
            # данные идут с 1, итоговая строка индекс = n_rows (последняя)
            last_row = n_rows
            for col in range(n_cols):
                cell = table[(last_row, col)]
                cell.set_facecolor(black_bg)
                cell.get_text().set_color(white_text)

        plt.tight_layout()
        return fig