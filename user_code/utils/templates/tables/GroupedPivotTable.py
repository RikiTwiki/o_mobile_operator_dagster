from __future__ import annotations
from typing import Any, Dict, List
from datetime import datetime

class GroupedPivotTable:
    TableStyle: str = "border:1px solid #32383e; border-collapse:collapse; font-size:12; width:100%;"
    CellStyle: str  = "border:1px solid #32383e; padding:4px;"

    # Проценты статичных столбцов
    staticHeaderWidths: Dict[int, int] = {
        1: 10,  # Регион
        2: 10,  # Ответственный
        3: 10,  # Заявка
        4: 19,  # Подзадача
        5: 15,  # Статус
    }
    totalColumnWidth: int = 5  # Итого

    rawData: List[Dict[str, Any]]
    flatItems: List[Dict[str, Any]]
    months: List[str]
    uniqueTotals: Dict[str, int]
    grouped: Dict[str, Dict[str, Dict[str, Dict[str, Dict[str, int]]]]]

    regionResponsible: Dict[str, str] = {
        'Чуйская область':         'Асанбек уулу Мирлан<br>Алиев Исмаил<br>Жакишев Азамат<br>Баратбаев Азамат',
        'Нарынская область':       'Жолдошев Максат<br>Жакишев Азамат',
        'Джалал-Абадская область': 'Кадыралиев Руслан<br>Жакишев Азамат',
        'Иссык-Кульская область':  'Кадыров Айбек<br>Жакишев Азамат',
        'Таласская область':       'Сарбалиев Нурсултан<br>Жакишев Азамат',
        'Баткенская область':      'Турдуматов Дарман<br>Жакишев Азамат',
        'Ошская область':          'Шакиров Максат<br>Жакишев Азамат',
        'Все области КР':          'Алексей Сураев',
    }

    responsibleMap: Dict[str, Dict[str, str]] = {
        'Ошская область': {
            'Демонтаж в US': 'Шакиров Максат<br>Болот Абакиров',
            'Подключение нового абонента': 'Шакиров Максат<br>Асел Касымбекова<br>Мадина Шаршебаева<br>Исмаил Алиев',
            'Смена ТП 300/500': 'Шакиров Максат<br>Мадина Шаршебаева<br>Юлия Зарубина<br>Азамат Жакишев',
            'Подключение нового корпоративного абонента': 'Руслан Байсалов',
            'Перенос кабеля на другой адрес': 'Шакиров Максат<br>Алиев Исмаил',
            'Установка дополнительной O!TV приставки (выкуп)': 'Шакиров Максат<br>Алиев Исмаил',
            'Смена технологии': 'Шакиров Максат<br>Алиев Исмаил',
        },
        'Иссык-Кульская область': {
            'Демонтаж в US': 'Кадыров Айбек<br>Болот Абакиров',
            'Подключение нового корпоративного абонента': 'Руслан Байсалов',
            'Подключение нового абонента': 'Кадыров Айбек<br>Асел Касымбекова<br>Мадина Шаршебаева<br>Исмаил Алиев',
            'Смена ТП 300/500': 'Кадыров Айбек<br>Мадина Шаршебаева<br>Юлия Зарубина<br>Азамат Жакишев',
            'Перенос кабеля на другой адрес': 'Кадыров Айбек<br>Исмаил Алиев',
            'Установка дополнительной O!TV приставки (выкуп)': 'Кадыров Айбек<br>Исмаил Алиев',
        },
        'Джалал-Абадская область': {
            'Демонтаж в US': 'Кадыралиев Руслан<br>Болот Абакиров',
            'Аварийные работы': 'Кадыралиев Руслан',
            'Подключение нового корпоративного абонента': 'Руслан Байсалов',
            'Подключение нового абонента': 'Кадыралиев Руслан<br>Асел Касымбекова<br>Мадина Шаршебаева<br>Исмаил Алиев',
            'Смена ТП 300/500': 'Кадыралиев Руслан<br>Мадина Шаршебаева<br>Юлия Зарубина<br>Азамат Жакишев',
            'Добавление адреса': 'Кадыралиев Руслан<br>Азамат Жакишев',
            'Перенос кабеля в одном здании': 'Кадыралиев Руслан<br>Азамат Жакишев',
            'Перенос кабеля на другой адрес': 'Кадыралиев Руслан<br>Алиев Исмаил',
            'Установка дополнительной O!TV приставки (выкуп)': 'Кадыралиев Руслан<br>Алиев Исмаил',
        },
        'Баткенская область': {
            'Демонтаж в US': 'Турдуматов Дарман<br>Болот Абакиров',
            'Подключение нового корпоративного абонента': 'Руслан Байсалов',
            'Подключение нового абонента': 'Турдуматов Дарман<br>Асел Касымбекова<br>Мадина Шаршебаева<br>Исмаил Алиев',
            'Смена ТП 300/500': 'Турдуматов Дарман<br>Мадина Шаршебаева<br>Юлия Зарубина<br>Азамат Жакишев',
            'Добавление адреса': 'Турдуматов Дарман<br>Азамат Жакишев',
            'Перенос кабеля в одном здании': 'Турдуматов Дарман<br>Азамат Жакишев',
        },
        'Таласская область': {
            'Затруднение в работе O!TV': 'Сарбалиев Нурсултан<br>Азамат Жакишев',
            'Подключение нового корпоративного абонента': 'Руслан Байсалов',
            'Подключение нового абонента': 'Сарбалиев Нурсултан<br>Асел Касымбекова<br>Мадина Шаршебаева<br>Исмаил Алиев',
            'Смена ТП 300/500': 'Сарбалиев Нурсултан<br>Мадина Шаршебаева<br>Юлия Зарубина<br>Азамат Жакишев',
            'Добавление адреса': 'Сарбалиев Нурсултан<br>Азамат Жакишев',
            'Демонтаж в US': 'Сарбалиев Нурсултан<br>Болот Абакиров',
            'Перенос кабеля на другой адрес': 'Сарбалиев Нурсултан<br>Алиев Исмаил',
        },
        'Все области КР': {
            'ДС есть, но ТП не подключился': 'Юлия Зарубина',
            'Списалась АП, но ТП не подключился': 'Юлия Зарубина',
            'Физически подключили, но сервисы не назначались': 'Мадина Шаршебаева',
            'Подключение нового корпоративного абонента': 'Руслан Байсалов',
            'Подключение нового абонента': 'Рашитова Назира<br>Максатбекова Айназик<br>Шаршебаева Мадина',
            'Перерасчёт': 'Мадина Шаршебаева<br>Юлия Зарубина',
            'Обращения по аренде': 'Азамат Асипов',
            'Подключение без согласия жильцов': 'Азамат Асипов',
            'SUB: Перерасчёт': 'Мадина Шаршебаева',
            'Исправление данных в договоре': 'Шаршебаева Мадина',
        },
        'Чуйская область': {
            'Подключение нового абонента': 'Алиев Исмаил<br>Касымбекова Асел<br>Шаршебаева Мадина',
            'Добавление адреса': 'Жакишев Азамат',
            'Перенос кабеля на другой адрес': 'Исмаил Алиев',
            'Смена ТП 300/500': 'Жакишев Азамат<br>Мадина Шаршебаева<br>Юлия Зарубина',
            'Подключение нового корпоративного абонента': 'Руслан Байсалов',
            'Восстановление линии': 'Асанбек уулу Мирлан<br>Алиев Исмаил<br>Баратбаев Азамат<br>Мадина Шаршебаева',
            'Установка роутера (выкуп)': 'Азамат Жакишев<br>Асанбек уулу Мирлан<br>Баратбаев Азамат',
            '[SUB] Новый абонент: На уточнении': 'Контакт-Центр',
            'Перенос кабеля в одном здании': 'Асанбек уулу Мирлан<br>Жакишев Азамат<br>Баратбаев Азамат',
            'Установка дополнительной O!TV приставки (выкуп)': 'Асанбек уулу Мирлан<br>Алиев Исмаил<br>Баратбаев Азамат',
            '[SUB] Новый абонент - ОАП: Монтаж': 'Асанбек уулу Мирлан<br>Алиев Исмаил<br>Баратбаев Азамат',
            '[SUB] Новый абонент - ОАП: Монтаж кабеля': 'Асанбек уулу Мирлан<br>Алиев Исмаил<br>Баратбаев Азамат',
        },
        'Нарынская область': {
            'Подключение нового корпоративного абонента': 'Руслан Байсалов',
            'Подключение нового абонента': 'Жолдошев Максат<br>Асел Касымбекова<br>Мадина Шаршебаева<br>Исмаил Алиев',
            'Добавление адреса': 'Жолдошев Максат<br>Азамат Жакишев',
            'Смена ТП 300/500': 'Жолдошев Максат<br>Мадина Шаршебаева<br>Юлия Зарубина<br>Азамат Жакишев',
            'Демонтаж в US': 'Жолдошев Максат<br>Болот Абакиров',
            'Перенос кабеля на другой адрес': 'Жолдошев Максат<br>Алиев Исмаил',
        },
    }

    def __init__(self, data: List[Dict[str, Any]]):
        self.rawData = data
        self.flatItems = []
        self.months = []
        self.uniqueTotals = {}
        self.grouped = {}
        self.prepare()

    # === pipeline ===
    def prepare(self) -> None:
        self.flatten()
        self.collectMonths()
        self.collectUniqueTotals()
        self.groupData()

    def flatten(self) -> None:
        self.flatItems = []
        for entry in self.rawData:
            items = entry.get('items')
            if items and isinstance(items, list):
                region  = entry.get('область') or entry.get('local') or ''
                request = entry.get('title')   or entry.get('request') or ''
                for item in items:
                    self.flatItems.append({
                        'region':  region,
                        'request': request,
                        'subtask': item.get('subtask_title') or item.get('subtask') or 'Нет подзадач',
                        'status':  item.get('status', ''),
                        'month':   item.get('month', ''),
                        'count':   int(item.get('count', 0) or 0),
                    })

    def collectMonths(self) -> None:
        months_set = {it.get('month', '') for it in self.flatItems}
        months_set.discard('')  # как PHP: пустая строка попадёт, удалим
        self.months = sorted(months_set)

    def collectUniqueTotals(self) -> None:
        self.uniqueTotals = {m: 0 for m in self.months}
        for entry in self.rawData:
            m = entry.get('month')
            if m and 'unique_count' in entry:
                self.uniqueTotals[m] = int(entry.get('unique_count') or 0)

    def groupData(self) -> None:
        self.grouped = {}
        # инициализация и накопление
        for it in self.flatItems:
            reg     = it['region']
            req     = it['request']
            subtask = it['subtask']
            status  = it['status']
            month   = it['month']

            self.grouped.setdefault(reg, {}).setdefault(req, {}).setdefault(subtask, {})
            if status not in self.grouped[reg][req][subtask]:
                self.grouped[reg][req][subtask][status] = {m: 0 for m in self.months}
            if month in self.grouped[reg][req][subtask][status]:
                self.grouped[reg][req][subtask][status][month] += it['count']

        # сортировка статусов по сумме по убыванию (аналог uasort)
        for regKey, requests in list(self.grouped.items()):
            for reqKey, subtasks in list(requests.items()):
                for subKey, statusRows in list(subtasks.items()):
                    ordered = dict(
                        sorted(
                            statusRows.items(),
                            key=lambda kv: sum(kv[1].values()),
                            reverse=True,
                        )
                    )
                    self.grouped[regKey][reqKey][subKey] = ordered

    # === helpers ===
    def formatMonth(self, ymd: str) -> str:
        # ожидается 'YYYY-MM-DD' (как в PHP DateTime)
        try:
            d = datetime.strptime(ymd, "%Y-%m-%d")
        except ValueError:
            # fallback: если формат 'YYYY-MM'
            d = datetime.strptime(ymd + "-01", "%Y-%m-%d")
        m = {1:'янв',2:'фев',3:'мар',4:'апр',5:'май',6:'июн',7:'июл',8:'авг',9:'сен',10:'окт',11:'ноя',12:'дек'}[int(d.strftime("%m"))]
        return f"{m}.{d.strftime('%y')}"

    def getResponsible(self, region: str, request: str) -> str:
        # override по связке регион+заявка
        if region in self.responsibleMap and request in self.responsibleMap[region]:
            return self.responsibleMap[region][request]
        return self.regionResponsible.get(region, '')

    # === render ===
    def render(self) -> str:
        sumStatic   = sum(self.staticHeaderWidths.values())
        countMonths = len(self.months) or 1
        monthWidth  = f"{round((100 - sumStatic - self.totalColumnWidth) / countMonths, 2)}%"
        totalWidth  = f"{self.totalColumnWidth}%"

        # глобальные мин/макс по всем ячейкам > 0
        all_vals: List[int] = []
        def _walk(v: Any):
            if isinstance(v, dict):
                for _k, _v in v.items():
                    _walk(_v)
            elif isinstance(v, int):
                if v > 0:
                    all_vals.append(v)

        for requests in self.grouped.values():
            _walk(requests)

        globalMin = min(all_vals) if all_vals else 0
        globalMax = max(all_vals) if all_vals else 0

        html  = f"<table style='{self.TableStyle}'>"
        headerStyle = self.CellStyle + ' background-color:#32383e; color:#fff;'

        # Заголовок
        html += '<tr>'
        titles = ['Регион','Ответственный','Заявка','Подзадача','Статус']
        for i, title in enumerate(titles, start=1):
            width = f"{self.staticHeaderWidths.get(i, 0)}%"
            html += f'<th style="{(headerStyle + " width:" + width).strip()}">{title}</th>'
        for m in self.months:
            html += f'<th style="{(headerStyle + " width:" + monthWidth).strip()}">{self.formatMonth(m)}</th>'
        html += f'<th style="{(headerStyle + " width:" + totalWidth).strip()}">Итого</th>'
        html += '</tr>'

        # Тело
        for region, requests in self.grouped.items():
            # сортировка подзадач по сумме (как в PHP перед выводом)
            # (копируем структуру, сохраняя порядок)
            sorted_requests = {}
            for req, subtasks in requests.items():
                sorted_subtasks = dict(sorted(
                    subtasks.items(),
                    key=lambda kv: sum(sum(months.values()) for months in kv[1].values()),
                    reverse=True
                ))
                sorted_requests[req] = sorted_subtasks

            rowsInRegion = self.countRows(sorted_requests)
            firstRegion  = True

            for request, subtasks in sorted_requests.items():
                rowsInRequest = sum(len(statusRows) for statusRows in subtasks.values())
                firstRequest = True

                for subtask, statusRows in subtasks.items():
                    subtaskRowCount = len(statusRows)
                    firstSubtask    = True

                    for status, counts in statusRows.items():
                        html += '<tr>'

                        if firstRegion:
                            html += f'<td style="{(self.CellStyle + " width:" + str(self.staticHeaderWidths[1]) + "%").strip()}" rowspan="{rowsInRegion}">{region}</td>'
                            html += f'<td style="{(self.CellStyle + " width:" + str(self.staticHeaderWidths[2]) + "%").strip()}" rowspan="{rowsInRegion}">{self.getResponsible(region, request)}</td>'
                            firstRegion = False
                        if firstRequest:
                            html += f'<td style="{(self.CellStyle + " width:" + str(self.staticHeaderWidths[3]) + "%").strip()}" rowspan="{rowsInRequest}">{request}</td>'
                            firstRequest = False
                        if firstSubtask:
                            html += f'<td style="{(self.CellStyle + " width:" + str(self.staticHeaderWidths[4]) + "%").strip()}" rowspan="{subtaskRowCount}">{subtask}</td>'
                            firstSubtask = False

                        html += f'<td style="{(self.CellStyle + " width:" + str(self.staticHeaderWidths[5]) + "%").strip()}">{status}</td>'

                        total = 0
                        for m in self.months:
                            c = counts.get(m, 0)
                            if c > 0:
                                color = self.gradientColour(int(c), globalMin, globalMax, 'asc')
                            else:
                                color = ''
                            style = (self.CellStyle + f" width:{monthWidth};").strip() + (f"background-color:{color};" if color else '')
                            html += f'<td style="{style}">{c}</td>'
                            total += c

                        html += f'<td style="{(self.CellStyle + " width:" + totalWidth).strip()}">{total}</td>'
                        html += '</tr>'

        # Итоги уникальных
        html += '<tr>'
        html += f'<td colspan="5" style="{self.CellStyle} text-align:center;"><strong>Итого уникальных задач</strong></td>'
        for m in self.months:
            cnt = self.uniqueTotals.get(m, 0)
            html += f'<td style="{(self.CellStyle + " width:" + monthWidth).strip()}">{cnt}</td>'
        grand = sum(self.uniqueTotals.values()) if self.uniqueTotals else 0
        html += f'<td style="{(self.CellStyle + " width:" + totalWidth).strip()}">{grand}</td>'
        html += '</tr>'

        html += '</table>'
        return html

    def gradientColour(self, current: int, minv: int, maxv: int, type: str = 'asc') -> str:
        if maxv <= minv:
            return ''
        pct = round(((current - minv) / (maxv - minv)) * 100, 0)
        if type == 'desc':
            pct = 100 - pct
        steps = [0, 15, 60, 70, 90, 100]
        cmap = {
            0:   '#FFEBEE',
            15:  '#FFD7DE',
            60:  '#FFC3CE',
            70:  '#FFAFBE',
            90:  '#FF9CAE',
            100: '#FF4D6E',
        }
        key = max(s for s in steps if s <= pct)
        return cmap.get(key, '')

    def getTable(self) -> str:
        return self.render()

    def countRows(self, requests: Dict[str, Dict[str, Dict[str, Dict[str, int]]]]) -> int:
        s = 0
        for subtasks in requests.values():
            for statusRows in subtasks.values():
                s += len(statusRows)
        return s
