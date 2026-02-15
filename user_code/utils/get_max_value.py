def get_max_value_by_key(items, key):
    """
    Возвращает максимальное значение по ключу из списка словарей или одного словаря.
    """
    if isinstance(items, dict):
        items = [items]
    elif not isinstance(items, list):
        raise TypeError("items должен быть либо списком, либо словарём")

    if not items:
        raise ValueError("items пуст")

    max_val = None
    for idx, item in enumerate(items):
        if not isinstance(item, dict):
            raise TypeError(f"Элемент с индексом {idx} не является словарём")

        if key not in item:
            raise KeyError(f"Ключ '{key}' не найден в элементе с индексом {idx}")

        val = item[key]
        if max_val is None or val > max_val:
            max_val = val

    return max_val
