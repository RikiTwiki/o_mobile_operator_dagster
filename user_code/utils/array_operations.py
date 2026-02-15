from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Any, Hashable, Union, Optional, Sequence

import pandas as pd


def left_join_array(left, right, left_join_on, right_join_on=None):
    right_join_on = right_join_on or left_join_on
    if not right:
        return left

    if isinstance(right, dict):
        right = list(right.values()) if all(isinstance(v, dict) for v in right.values()) else [right]
    elif hasattr(right, "to_dict"):
        try:
            right = right.to_dict("records")
        except Exception:
            right = list(right)


    right_fields = set().union(*(r.keys() for r in right)) if right else set()
    empty_right = {k: None for k in right_fields if k != right_join_on}

    # Индекс правой части и сам join
    r_index = {r.get(right_join_on): r for r in right}
    out = []
    for l in (left or []):
        r = r_index.get(l.get(left_join_on))
        out.append({**l, **r} if r is not None else {**empty_right, **l})
    return out


def inner_join_array(left, right, left_join_on, right_join_on=None):
    """Выполняет INNER JOIN между двумя массивами"""
    if not right:
        return []

    right_join_on = right_join_on or left_join_on

    # Создаем хеш-таблицу для быстрого поиска
    right_hash = {item[right_join_on]: item for item in right}

    final = []
    for left_item in left:
        left_key = left_item[left_join_on]
        if left_key in right_hash:
            right_item = right_hash[left_key]
            final.append({**left_item, **right_item})

    return final


def left_join_and_sum_values(left, right, left_join_on, right_join_on=None):
    """Выполняет LEFT JOIN с суммированием числовых значений"""
    if not right:
        return left
    if not left:
        return right

    right_join_on = right_join_on or left_join_on

    # Создаем шаблон полей для суммирования
    empty_right_fields = {k: 0 for k in right[0] if k != right_join_on}

    final = []
    for left_item in left:
        item = left_item.copy()
        matched = False
        sum_fields = {k: 0 for k in empty_right_fields}

        # Суммируем все совпадающие записи
        for right_item in right:
            if left_item[left_join_on] == right_item[right_join_on]:
                matched = True
                for key, value in right_item.items():
                    if key != right_join_on and isinstance(value, (int, float)):
                        sum_fields[key] += value

        if not matched:
            # Если нет совпадений, добавляем пустые поля
            item = {**empty_right_fields, **left_item}
        else:
            # Добавляем суммированные значения
            for key, sum_value in sum_fields.items():
                if key in item and isinstance(item[key], (int, float)):
                    item[key] += sum_value
                else:
                    item[key] = sum_value

        final.append(item)

    return final

def get_difference(
    main: List[Dict[str, Any]],
    secondary: List[Dict[str, Any]],
    common_key: str = "hour",                 # ваши актуальные дефолты
    decreasing: str = "erlang",
    subtraction: str = "fact_operators",
    difference: str = "erlang_and_fact_operators",
    strict: bool = True,
) -> List[Dict[str, Union[str, int, int]]]:
    from typing import Hashable, Dict, Any, List, Union

    index: Dict[Hashable, Dict[str, Any]] = {}
    for row in secondary:
        k = row.get(common_key)
        if k not in index:
            index[k] = row  # first()

    out: List[Dict[str, Union[str, int, int]]] = []
    for item in main:
        if strict and common_key not in item:
            raise KeyError(f"Main row missing key '{common_key}': {item}")

        key = item.get(common_key)

        if strict and decreasing not in item:
            raise KeyError(f"Main row missing '{decreasing}' for {common_key}={key!r}")

        if key not in index:
            if strict:
                raise KeyError(f"No match for {common_key}={key!r} in secondary")
            dec = int(item.get(decreasing, 0) or 0)
            sub = 0.0
        else:
            if strict and subtraction not in index[key]:
                raise KeyError(f"Secondary row missing '{subtraction}' for {common_key}={key!r}")
            dec = int(item.get(decreasing, 0) or 0)
            sub = int(index[key].get(subtraction, 0) or 0)

        out.append({difference: dec - sub, common_key: key})

    return out

def df_select_one(df) -> Dict[str, Any]:
    """
    Эмулирует поведение PHP DB::selectOne:
    - если нет строк → пустой словарь {}
    - если есть строки → берем первую и превращаем в dict
    """
    if df is None or getattr(df, "empty", True):
        return {}
    # Берем первую строку и приводим к dict
    row = df.iloc[0]
    # Защита от NaN → 0 (как часто в agg)
    return {k: (0 if pd.isna(v) else v) for k, v in row.to_dict().items()}

def _parse_period(period: str):
    """Как strtotime в PHP: пробуем datetime, потом дату."""
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(period, fmt)
        except ValueError:
            continue
    return datetime.fromisoformat(period)

def _smart_number(v: Any) -> Any:
    if isinstance(v, bool) or v is None:
        return v
    if isinstance(v, (float, Decimal)):
        # Проверяем, является ли число целым (например, 3.0)
        if float(v).is_integer():
            return int(v)
        # Округляем до 1 знака
        return round(float(v), 1)
    return v

def array_merge(a, b):
    # если оба списка → конкатенация
    if isinstance(a, list) and isinstance(b, list):
        return a + b
    # если оба словари → объединение
    if isinstance(a, dict) and isinstance(b, dict):
        merged = a.copy()
        merged.update(b)
        return merged
    # иначе — ошибка
    raise TypeError("array_merge ожидает два списка или два словаря")


Row = Dict[str, Any]
Rows = List[Row]

def _to_rows(obj: Union[Dict[str, Any], List[Any], None]) -> Rows:
    """
    Приводит {"labels": [...], "items": [...]} к списку словарей.
    Поддерживает:
      - items = [dict, dict, ...]
      - items = [[...], [...], ...] при наличии labels
      - obj = уже список словарей
    Пустые/None -> [].
    """
    if not obj:
        return []

    # Уже список
    if isinstance(obj, list):
        if not obj:
            return []
        if isinstance(obj[0], dict):
            return obj
        # список списков без labels — нечем именовать колонки
        return []

    # Словарь с labels/items
    if isinstance(obj, dict):
        items = obj.get("items", [])
        labels = obj.get("labels")
        if not items:
            return []
        if isinstance(items[0], dict):
            return items
        if labels and isinstance(items[0], (list, tuple)):
            return [dict(zip(labels, row)) for row in items]
        return []

    return []

def left_join_array_dict(
    left: Union[List[Row], Dict[str, Any], None],
    right: Union[List[Row], Dict[str, Any], None],
    left_join_on: Union[str, Sequence[str]],
    right_join_on: Optional[Union[str, Sequence[str]]] = None,
    right_suffix: str = "_r",
    drop_on_from_right: bool = True,
) -> Rows:
    """
    LEFT JOIN по ключу(ям).
    - left/right: список словарей или {"labels":..., "items":...}
    - left_join_on: имя ключа или список имён (составной ключ)
    - right_join_on: если None — берём как left_join_on
    - right_suffix: суффикс для конфликтующих колонок из правой части
    - drop_on_from_right: не дублировать ключ(и) из правой части в результат
    """

    def _key_fn(on):
        if isinstance(on, str):
            return lambda row: row.get(on)
        cols = list(on)
        return lambda row: tuple(row.get(c) for c in cols)

    L = _to_rows(left)
    R = _to_rows(right)

    # Если справа пусто — просто вернём левую часть как есть
    if not R:
        return L.copy() if isinstance(L, list) else []

    if right_join_on is None:
        right_join_on = left_join_on

    lkey = _key_fn(left_join_on)
    rkey = _key_fn(right_join_on)

    # Соберём объединённый набор правых колонок
    right_keys_union = set()
    for r in R:
        if isinstance(r, dict):
            right_keys_union.update(r.keys())

    # По желанию не добавляем ключевые колонки справа
    def _discard_on(s: set, on):
        if isinstance(on, str):
            s.discard(on)
        else:
            for c in on:
                s.discard(c)

    if drop_on_from_right:
        _discard_on(right_keys_union, right_join_on)

    # Индекс правой части по ключу (последний победит; при необходимости можно хранить списки)
    r_index: Dict[Any, Row] = {}
    for r in R:
        if isinstance(r, dict):
            r_index[rkey(r)] = r

    # Соберём результат
    out: Rows = []
    for l in L:
        if not isinstance(l, dict):
            continue
        lk = lkey(l)
        r = r_index.get(lk)

        # Начинаем с копии левой строки
        row = dict(l)

        # Подтягиваем правые поля
        if r:
            for col in right_keys_union:
                if col in row:
                    row[col + right_suffix] = r.get(col)
                else:
                    row[col] = r.get(col)
        else:
            # Нет совпадения — подставляем None для всех правых колонок
            for col in right_keys_union:
                if col in row:
                    row[col + right_suffix] = None
                else:
                    row[col] = None

        out.append(row)

    return out