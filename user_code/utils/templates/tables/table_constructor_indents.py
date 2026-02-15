import html
import math
import re
from typing import Any, Dict, List, Optional

from utils.transformers.mp_report import smart_number


def round_half_up(x: float, ndigits: int = 0) -> float:
    """
    Эквивалент PHP round(..., PHP_ROUND_HALF_UP) для десятичных разрядов.
    Для ndigits=0 вернёт float целого значения (как в PHP).
    """
    m = 10 ** ndigits
    y = x * m
    # half-up: .5 всегда вверх по модулю
    if y >= 0:
        res = math.floor(y + 0.5)
    else:
        res = math.ceil(y - 0.5)
    return res / m


class TableConstructorIndents:
    # Публичные стили (как в PHP)
    TableStyle: str   = "border: 1px solid #32383e; border-collapse: collapse; font-size: 12px;"
    TDStyle: str      = "border: 1px solid #858585; text-align: center; padding: 3px;"
    THStyle: str      = "color: #fff; border: 1px solid #32383e; text-align: center; padding: 3px; background-color: #43484c;"
    TRStyle: str      = ""
    TRTitleStyle: str = "border: 1px solid #858585; font-weight: bold; text-align: center; background-color: #d9d9d9;"

    def __init__(self) -> None:
        # Приватные поля
        self.__TableBody: str = ""
        self.__RowData: str = ""
        self.__blockCounter: int = 0
        self.__currentBlockBackground: str = ""

        self.__currentColumnIndex: int = 0
        self.__columnWidths: Dict[int, str] = {
            1: "width: 10%;",
            2: "width: 10%;",
            3: "width: 5%;",
            4: "width: 5%;",
            5: "width: 5%;",
            6: "width: 5%;",
            7: "width: 5%;",
            8: "width: 5%;",
            9: "width: 5%;",
            10: "width: 5%;",
            11: "width: 30%;",
        }

        self.__colourMap: Dict[int, str] = {
            0:   "#FFEBEE",
            15:  "#FFD7DE",
            60:  "#FFC3CE",
            70:  "#FFAFBE",
            90:  "#FF9CAE",
            100: "#FF4D6E",
        }

    # ----------------- Вспомогательные -----------------

    def _build_attrs(self, props: Optional[Dict[str, Any]]) -> str:
        if not props:
            return ""
        out = []
        for k, v in props.items():
            out.append(f' {k}="{v}"')
        return "".join(out)

    def gradientColour(self, current: float, min_v: float, max_v: float, inverse: bool = False) -> str:
        if (max_v - min_v) == 0.0:
            percent = 0
        else:
            percent = round_half_up(((current - min_v) / (max_v - min_v)) * 100.0, 0)
            percent = max(0, min(100, int(percent)))
        if inverse:
            percent = 100 - percent

        selected = 0
        for threshold in self.__colourMap.keys():
            if percent >= threshold and threshold > selected:
                selected = threshold
        return f"background-color: {self.__colourMap[selected]};"

    def getBlockBackgroundColor(self) -> str:
        cycle = self.__blockCounter % 2
        return "background-color: #f0f0f0;" if cycle == 0 else ""

    def addSeparatorRow(self, colspan: int = 7) -> None:
        self.__TableBody += (
            f'<tr style="height: 10px;">'
            f'<td colspan="{colspan}" style="border: none; background-color: transparent; padding: 0;"></td>'
            f"</tr>"
        )

    # ----------------- Генераторы ячеек/строк -----------------

    def genCellTH(self, content: str, extraStyle: str = "", props: Optional[Dict[str, Any]] = None) -> None:
        self.__currentColumnIndex += 1
        width = self.__columnWidths.get(self.__currentColumnIndex, "")
        style = f"{self.THStyle} {width} {extraStyle}".strip()
        attrs = self._build_attrs(props)
        self.__RowData += f'<th style="{style}"{attrs}>{html.escape(content, quote=True)}</th>'

    def genCellTD(self, content: Any, extraStyle: str = "", props: Optional[Dict[str, Any]] = None) -> None:
        self.__currentColumnIndex += 1
        width = self.__columnWidths.get(self.__currentColumnIndex, "")
        style = f"{self.TDStyle} {width} {extraStyle}".strip()
        attrs = self._build_attrs(props)
        normalized = smart_number(content)  # 0.0 -> 0, "97.0" -> 97, "93.8" -> 93.8
        text = html.escape(str(normalized), quote=True)

        self.__RowData += f'<td style="{style}"{attrs}>{text}</td>'
    def genRow(self, hideBorders: bool = False) -> None:
        rowStyle = "border: none;" if hideBorders else self.TRStyle
        self.__TableBody += f'<tr style="{rowStyle.strip()}">{self.__RowData}</tr>'
        self.__RowData = ""
        self.__currentColumnIndex = 0

    def genRowTitle(self, title: str, colspan: Optional[int] = None) -> None:
        if colspan is None:
            matches = re.findall(r"<(td|th)\b", self.__RowData, flags=re.I)
            colspan = len(matches)
        self.__TableBody += (
            f'<tr style="{self.TRTitleStyle}"><td colspan="{colspan}">'
            f"{html.escape(title, quote=True)}</td></tr>"
        )
        self.__RowData = ""
        self.__currentColumnIndex = 0

    # ----------------- Блочные рендеры (как в PHP) -----------------

    def genDataRow(self, data: Dict[str, Any]) -> None:
        # разделитель между блоками (кроме первого)
        if self.__blockCounter > 0:
            self.addSeparatorRow()

        # фон блока
        self.__currentBlockBackground = self.getBlockBackgroundColor()

        items = data.get("new_items") or []
        count = max(1, len(items))

        attempt_num = int(data.get("attempt_number", 0))
        service_cnt = int(data.get("service", 0))
        conversion = data.get("conversion") if data.get("conversion") is not None else "–"

        for i in range(count):
            self.__RowData = ""
            if i == 0:
                rs = {"rowspan": str(count)}
                self.genCellTD(data.get("old_name", ""), "", rs)
                self.genCellTD(data.get("qwes_recomendet", "–") or "–", "", rs)

            # динамика new-name
            if i < len(items):
                self.genCellTD(items[i].get("new-name", ""))
            else:
                self.genCellTD("")

            if i == 0:
                self.genCellTD(str(attempt_num), "", rs)
                self.genCellTD(str(service_cnt), "", rs)

            # динамика connections
            if i < len(items):
                self.genCellTD(str(items[i].get("connections", "")))
            else:
                self.genCellTD("")

            if i == 0:
                self.genCellTD(conversion, "", rs)

            self.genRow()

        self.__blockCounter += 1

    def genDataRowNew(self, data: Dict[str, Any]) -> None:
        if self.__blockCounter > 0:
            self.addSeparatorRow(6)

        bg = self.getBlockBackgroundColor()

        title = data.get("title", "")
        items = data.get("subtasks") or []
        count = max(1, len(items))

        total_parent = sum(int(it.get("total_issues", 0)) for it in items)

        for i in range(count):
            self.__RowData = ""
            if i == 0:
                rs = {"rowspan": str(count)}
                self.genCellTD(title, bg, rs)

            sub_title = items[i].get("subtask_title", "") if i < len(items) else ""
            self.genCellTD(sub_title, bg)

            if i == 0:
                self.genCellTD(str(total_parent), bg, rs)

            child_cnt = items[i].get("daughter_issue_count", "") if i < len(items) else ""
            self.genCellTD(str(child_cnt), bg)

            open_ = items[i].get("open", "") if i < len(items) else ""
            self.genCellTD(str(open_), bg)

            closed = items[i].get("closed", "") if i < len(items) else ""
            self.genCellTD(str(closed), bg)

            self.genRow()

        self.__blockCounter += 1

    def genDataRowGrouped(self, data: Dict[str, Any]) -> None:
        if self.__blockCounter > 0:
            # теперь 11 колонок
            self.addSeparatorRow(11)

        bg = self.getBlockBackgroundColor()

        parent = data.get("parent_issue_type", "") or ""

        totalArr = data.get("total_count") or []
        subtypeArr = data.get("subtask_type") or []
        subcountArr = data.get("subtasks_count") or []
        openArr = data.get("open_subtasks_count") or []
        closedArr = data.get("closed_subtasks_count") or []
        ontimeArr = data.get("ontime_closed_subtasks_count") or []

        planArr = [int(round_half_up(float(v), 0)) for v in (data.get("sla_plan_days") or [])]
        factArr = [int(round_half_up(float(v), 0)) for v in (data.get("sla_fact_days") or [])]

        # SLA% по строкам
        SLAArr: List[float] = []
        for ontime, closed in zip(ontimeArr, closedArr):
            try:
                ontime_f = float(ontime)
                closed_f = float(closed)
                SLAArr.append(round_half_up((ontime_f / closed_f) * 100.0, 1) if closed_f > 0 else 0.0)
            except Exception:
                SLAArr.append(0.0)

        combined = list(subcountArr) + list(openArr) + list(closedArr) + list(ontimeArr)
        globalMin = min(combined) if combined else 0
        globalMax = max(combined) if combined else 0
        slaMin = min(SLAArr) if SLAArr else 0
        slaMax = max(SLAArr) if SLAArr else 0

        commentMap: Dict[str, str] = {
            'Подключение нового абонента': 'Многоэтажные дома -  от 5-ти рабочих дней, частный сектор - от 10ти рабочих дней.',
            'Добавление адреса': 'От 1-го до 3-х рабочих дней',
            'Перерасчёт': 'От 1-го до 3-х рабочих дней',
            'Нет покрытия': 'Сроки не сообщаем, так как фиксируем обращением для учёта в дальнейших планах строительства сети ',
            'Смена ТП 300/500': 'Как скоро абонент внесет ДС за ТП, оборудование и выезд. Как только заявка перейдет в статус “В работе у инженеров”, ее должны отработать в течение 10 рабочих дней',
            'Перенос кабеля на другой адрес': 'В течение 5-ти рабочих дней',
            'Перенос кабеля в одном здании': 'От 1-го до 3-х рабочих дней',
            'Установка роутера (выкуп)': 'В течение 5-ти рабочих дней',
            'Восстановление линии': 'В течение 5-ти рабочих дней',
            'Перевод в красную зону': 'Такое не создает КЦ',
            'Установка дополнительной O!TV приставки (выкуп)': 'В течение 5-ти рабочих дней',
            'Не работает проводной интернет': 'От 1-го до 3-х рабочих дней',
            'Затруднение в работе O!TV': 'От 1-го до 3-х рабочих дней',
            'Низкая скорость на проводном интернете': 'От 1-го до 3-х рабочих дней',
            'Проблема отключения услуг': 'В течение 1-го рабочего дня',
            'Списалась АП, но ТП не подключился': 'В течение 1-го рабочего дня',
            'ДС есть, но ТП не подключился': 'В течение 1-го рабочего дня',
            'Массовая проблема - Низкая скорость Интернета': 'По завершению восстановительных работ',
            'Затруднение в работе IPTV': 'От 1-го до 3-х рабочих дней',
            'Не работает телефонная линия': 'От 1-го до 3-х рабочих дней',
            'Платный выезд: Не работает проводной интернет': 'От 1-го до 3-х рабочих дней',
            'Физически подключили, но сервисы не назначались': 'В течение 1-го рабочего дня',
            'Версия TV-приставки': 'Не создаем такие заявки',
            'Подключение нового корпоративного абонента': 'В течение 5-ти рабочих дней',
            'Исправление данных в договоре': 'В течение 1-го рабочего дня',
            'Обращения по аренде': 'От 1-го до 3-х рабочих дней',
            'Массовая проблема - Не работает Интернет': 'Точных сроков не сообщаем. Применяем скрипт согласно созданному массовому инциденту',
            'ГУАА - Демонтаж оборудования ': 'КЦ не создает заявки на этот тип запроса. Данный тип запроса создается в запросе на расторжение договора специалистами офисов. Сроки - в порядке очереди.',
            'Демонтаж в US': 'КЦ в этом запрос не принимает заявку. Такие заявка создается после обращения абонента в офис с заявлением о расторжении договора.',
            'Обращения с сайта: Поддержка': 'Абонент сам оставляет заявку на сайте. ГТМ отрабатывает ежедневно в порядке очереди',
            'Нет исходящих вызовов': 'От 1-го до 3-х рабочих дней',
            'Проблема подключения услуг': 'В течение 1-го рабочего дня',
            # … остальные варианты по необходимости
        }
        commentText = commentMap.get(parent, "")

        count = max(1, len(subtypeArr))

        for i in range(count):
            self.__RowData = ""

            # A) основная заявка с rowspan
            if i == 0:
                rs = {"rowspan": str(count)}
                self.genCellTD(parent, bg, rs)

            # B) тип подзадачи
            val = subtypeArr[i] if i < len(subtypeArr) else None
            self.genCellTD(val or "Нет подзадач", bg)

            # C) общее число (total_count) — как в PHP: только из первого элемента
            if i == 0:
                self.genCellTD(str(totalArr[i] if i < len(totalArr) else ""), bg, rs)

            # D) поступило подзадач (градиент)
            val = int(subcountArr[i]) if i < len(subcountArr) and subcountArr[i] is not None else 0
            style = self.gradientColour(val, globalMin, globalMax)
            self.genCellTD(str(val), style)

            # E) незакрыто подзадач
            val = int(openArr[i]) if i < len(openArr) and openArr[i] is not None else 0
            style = self.gradientColour(val, globalMin, globalMax)
            self.genCellTD(str(val), style)

            # F) закрыто подзадач
            val = int(closedArr[i]) if i < len(closedArr) and closedArr[i] is not None else 0
            style = self.gradientColour(val, globalMin, globalMax)
            self.genCellTD(str(val), style)

            # G) закрыто в срок
            val = int(ontimeArr[i]) if i < len(ontimeArr) and ontimeArr[i] is not None else 0
            style = self.gradientColour(val, globalMin, globalMax)
            self.genCellTD(str(val), style)

            # H) плановый SLA (дни, округление half-up)
            self.genCellTD(str(planArr[i] if i < len(planArr) else ""), bg)

            # I) фактический SLA (дни, округление half-up)
            self.genCellTD(str(factArr[i] if i < len(factArr) else ""), bg)

            # J) SLA, % (градиент, инверсия)
            sla = SLAArr[i] if i < len(SLAArr) else 0.0
            if slaMax == slaMin:
                style = ""
            else:
                style = self.gradientColour(sla, slaMin, slaMax, True)
            self.genCellTD(str(sla), style)

            # K) Комментарий (rowspan только в первой строке блока)
            if i == 0:
                self.genCellTD(commentText, bg, rs)

            self.genRow()

        self.__blockCounter += 1

    # ----------------- Служебные -----------------

    def resetBlockCounter(self) -> None:
        self.__blockCounter = 0

    def renderTable(self) -> str:
        return f'<table style="{self.TableStyle}">{self.__TableBody}</table>'
