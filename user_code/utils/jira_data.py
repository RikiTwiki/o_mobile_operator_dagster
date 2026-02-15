import json

from sqlalchemy import null

get_created_issues_by_types_with_id_nur = [
    {
        "date": "2025-09-22 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10304",
        "title": "Основные услуги - Некорректная тарификация",
        "count": 2,
        "not_resolved": 0,
        "resolved": 2
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10305",
        "title": "Основные услуги - Нет входящих вызовов",
        "count": 3,
        "not_resolved": 0,
        "resolved": 3
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10306",
        "title": "Основные услуги - Нет исходящих вызовов",
        "count": 6,
        "not_resolved": 0,
        "resolved": 6
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10308",
        "title": "Основные услуги - Не работает интернет",
        "count": 20,
        "not_resolved": 0,
        "resolved": 20
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10309",
        "title": "Основные услуги - Не работает узел или приложение",
        "count": 4,
        "not_resolved": 0,
        "resolved": 4
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10311",
        "title": "Основные услуги - Проблема получения SMS",
        "count": 7,
        "not_resolved": 0,
        "resolved": 7
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10406",
        "title": "Основные услуги - Ekscliuzivnyi_FRAUD",
        "count": 4,
        "not_resolved": 0,
        "resolved": 4
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10302",
        "project_key": "CSA",
        "issuetype": "10325",
        "title": "Контент, дополнительные услуги и акции - Контент услуги",
        "count": 2,
        "not_resolved": 0,
        "resolved": 2
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10302",
        "project_key": "CSA",
        "issuetype": "10328",
        "title": "Контент, дополнительные услуги и акции - Акции",
        "count": 4,
        "not_resolved": 1,
        "resolved": 3
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10303",
        "project_key": "SC",
        "issuetype": "10319",
        "title": "Self Care - Проблема подключения",
        "count": 5,
        "not_resolved": 0,
        "resolved": 5
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10303",
        "project_key": "SC",
        "issuetype": "10320",
        "title": "Self Care - Проблема отключения",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10316",
        "title": "Платежи - Нет платежа",
        "count": 67,
        "not_resolved": 1,
        "resolved": 66
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10317",
        "title": "Платежи - Заблокирован номер",
        "count": 2,
        "not_resolved": 0,
        "resolved": 2
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10404",
        "title": "Платежи - Разблокировка электронного кошелька",
        "count": 49,
        "not_resolved": 0,
        "resolved": 49
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10405",
        "title": "Платежи - Заблокировать кошелек",
        "count": 4,
        "not_resolved": 0,
        "resolved": 4
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10701",
        "title": "Платежи - Корректировка платежа",
        "count": 33,
        "not_resolved": 1,
        "resolved": 32
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10801",
        "title": "Платежи - О! Деньги - удаление кошелька",
        "count": 6,
        "not_resolved": 0,
        "resolved": 6
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "11900",
        "title": "Платежи - О!Бонус",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10307",
        "project_key": "MP",
        "issuetype": "10100",
        "title": "Массовая проблема - Bug",
        "count": 36,
        "not_resolved": 0,
        "resolved": 36
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11301",
        "title": "TERMINALS - Аннуляция платежа",
        "count": 27,
        "not_resolved": 27,
        "resolved": 0
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11502",
        "title": "TERMINALS - Нет платежа - Терминал",
        "count": 147,
        "not_resolved": 1,
        "resolved": 146
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11600",
        "title": "TERMINALS - Корректировка платежа - Терминал",
        "count": 24,
        "not_resolved": 0,
        "resolved": 24
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11602",
        "title": "TERMINALS - Установка терминала - Терминал",
        "count": 3,
        "not_resolved": 0,
        "resolved": 3
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11603",
        "title": "TERMINALS - Оплата за аренду - Терминал",
        "count": 2,
        "not_resolved": 0,
        "resolved": 2
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11605",
        "title": "TERMINALS - Технические проблемы - Терминал",
        "count": 7,
        "not_resolved": 0,
        "resolved": 7
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11607",
        "title": "TERMINALS - Подзадача - Терминал",
        "count": 6,
        "not_resolved": 0,
        "resolved": 6
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "12707",
        "title": "TERMINALS - Требуется чек о платеже",
        "count": 8,
        "not_resolved": 0,
        "resolved": 8
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "11603",
        "project_key": "OFD",
        "issuetype": "12201",
        "title": "OFD - Проблема - ОФД",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10304",
        "title": "Основные услуги - Некорректная тарификация",
        "count": 5,
        "not_resolved": 1,
        "resolved": 4
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10305",
        "title": "Основные услуги - Нет входящих вызовов",
        "count": 2,
        "not_resolved": 0,
        "resolved": 2
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10306",
        "title": "Основные услуги - Нет исходящих вызовов",
        "count": 10,
        "not_resolved": 0,
        "resolved": 10
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10308",
        "title": "Основные услуги - Не работает интернет",
        "count": 13,
        "not_resolved": 0,
        "resolved": 13
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10311",
        "title": "Основные услуги - Проблема получения SMS",
        "count": 7,
        "not_resolved": 0,
        "resolved": 7
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10406",
        "title": "Основные услуги - Ekscliuzivnyi_FRAUD",
        "count": 4,
        "not_resolved": 0,
        "resolved": 4
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "11204",
        "title": "Основные услуги - Help",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10302",
        "project_key": "CSA",
        "issuetype": "10325",
        "title": "Контент, дополнительные услуги и акции - Контент услуги",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10302",
        "project_key": "CSA",
        "issuetype": "10328",
        "title": "Контент, дополнительные услуги и акции - Акции",
        "count": 2,
        "not_resolved": 0,
        "resolved": 2
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10303",
        "project_key": "SC",
        "issuetype": "10319",
        "title": "Self Care - Проблема подключения",
        "count": 9,
        "not_resolved": 0,
        "resolved": 9
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10303",
        "project_key": "SC",
        "issuetype": "10322",
        "title": "Self Care - Смена номера абонента",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10303",
        "project_key": "SC",
        "issuetype": "10323",
        "title": "Self Care - Смена ТП",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10316",
        "title": "Платежи - Нет платежа",
        "count": 63,
        "not_resolved": 1,
        "resolved": 62
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10317",
        "title": "Платежи - Заблокирован номер",
        "count": 2,
        "not_resolved": 0,
        "resolved": 2
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10404",
        "title": "Платежи - Разблокировка электронного кошелька",
        "count": 61,
        "not_resolved": 1,
        "resolved": 60
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10405",
        "title": "Платежи - Заблокировать кошелек",
        "count": 7,
        "not_resolved": 0,
        "resolved": 7
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10701",
        "title": "Платежи - Корректировка платежа",
        "count": 29,
        "not_resolved": 4,
        "resolved": 25
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10801",
        "title": "Платежи - О! Деньги - удаление кошелька",
        "count": 5,
        "not_resolved": 0,
        "resolved": 5
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "11900",
        "title": "Платежи - О!Бонус",
        "count": 5,
        "not_resolved": 0,
        "resolved": 5
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11301",
        "title": "TERMINALS - Аннуляция платежа",
        "count": 22,
        "not_resolved": 22,
        "resolved": 0
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11502",
        "title": "TERMINALS - Нет платежа - Терминал",
        "count": 101,
        "not_resolved": 2,
        "resolved": 99
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11600",
        "title": "TERMINALS - Корректировка платежа - Терминал",
        "count": 22,
        "not_resolved": 0,
        "resolved": 22
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11602",
        "title": "TERMINALS - Установка терминала - Терминал",
        "count": 4,
        "not_resolved": 0,
        "resolved": 4
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11603",
        "title": "TERMINALS - Оплата за аренду - Терминал",
        "count": 2,
        "not_resolved": 0,
        "resolved": 2
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11605",
        "title": "TERMINALS - Технические проблемы - Терминал",
        "count": 7,
        "not_resolved": 0,
        "resolved": 7
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11607",
        "title": "TERMINALS - Подзадача - Терминал",
        "count": 4,
        "not_resolved": 0,
        "resolved": 4
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "12707",
        "title": "TERMINALS - Требуется чек о платеже",
        "count": 6,
        "not_resolved": 0,
        "resolved": 6
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "11603",
        "project_key": "OFD",
        "issuetype": "12201",
        "title": "OFD - Проблема - ОФД",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10304",
        "title": "Основные услуги - Некорректная тарификация",
        "count": 6,
        "not_resolved": 0,
        "resolved": 6
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10305",
        "title": "Основные услуги - Нет входящих вызовов",
        "count": 10,
        "not_resolved": 0,
        "resolved": 10
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10306",
        "title": "Основные услуги - Нет исходящих вызовов",
        "count": 7,
        "not_resolved": 1,
        "resolved": 6
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10308",
        "title": "Основные услуги - Не работает интернет",
        "count": 21,
        "not_resolved": 0,
        "resolved": 21
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10311",
        "title": "Основные услуги - Проблема получения SMS",
        "count": 5,
        "not_resolved": 0,
        "resolved": 5
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10406",
        "title": "Основные услуги - Ekscliuzivnyi_FRAUD",
        "count": 3,
        "not_resolved": 0,
        "resolved": 3
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10302",
        "project_key": "CSA",
        "issuetype": "10328",
        "title": "Контент, дополнительные услуги и акции - Акции",
        "count": 5,
        "not_resolved": 0,
        "resolved": 5
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10303",
        "project_key": "SC",
        "issuetype": "10319",
        "title": "Self Care - Проблема подключения",
        "count": 25,
        "not_resolved": 0,
        "resolved": 25
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10303",
        "project_key": "SC",
        "issuetype": "10320",
        "title": "Self Care - Проблема отключения",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10303",
        "project_key": "SC",
        "issuetype": "10322",
        "title": "Self Care - Смена номера абонента",
        "count": 2,
        "not_resolved": 0,
        "resolved": 2
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10303",
        "project_key": "SC",
        "issuetype": "10323",
        "title": "Self Care - Смена ТП",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10316",
        "title": "Платежи - Нет платежа",
        "count": 80,
        "not_resolved": 1,
        "resolved": 79
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10404",
        "title": "Платежи - Разблокировка электронного кошелька",
        "count": 65,
        "not_resolved": 0,
        "resolved": 65
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10405",
        "title": "Платежи - Заблокировать кошелек",
        "count": 3,
        "not_resolved": 0,
        "resolved": 3
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10701",
        "title": "Платежи - Корректировка платежа",
        "count": 34,
        "not_resolved": 1,
        "resolved": 33
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10801",
        "title": "Платежи - О! Деньги - удаление кошелька",
        "count": 6,
        "not_resolved": 1,
        "resolved": 5
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "11900",
        "title": "Платежи - О!Бонус",
        "count": 6,
        "not_resolved": 0,
        "resolved": 6
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10307",
        "project_key": "MP",
        "issuetype": "10100",
        "title": "Массовая проблема - Bug",
        "count": 4,
        "not_resolved": 0,
        "resolved": 4
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11301",
        "title": "TERMINALS - Аннуляция платежа",
        "count": 16,
        "not_resolved": 16,
        "resolved": 0
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11502",
        "title": "TERMINALS - Нет платежа - Терминал",
        "count": 113,
        "not_resolved": 0,
        "resolved": 113
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11600",
        "title": "TERMINALS - Корректировка платежа - Терминал",
        "count": 14,
        "not_resolved": 0,
        "resolved": 14
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11602",
        "title": "TERMINALS - Установка терминала - Терминал",
        "count": 2,
        "not_resolved": 0,
        "resolved": 2
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11605",
        "title": "TERMINALS - Технические проблемы - Терминал",
        "count": 7,
        "not_resolved": 0,
        "resolved": 7
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11607",
        "title": "TERMINALS - Подзадача - Терминал",
        "count": 5,
        "not_resolved": 0,
        "resolved": 5
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "12707",
        "title": "TERMINALS - Требуется чек о платеже",
        "count": 5,
        "not_resolved": 0,
        "resolved": 5
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10304",
        "title": "Основные услуги - Некорректная тарификация",
        "count": 6,
        "not_resolved": 0,
        "resolved": 6
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10305",
        "title": "Основные услуги - Нет входящих вызовов",
        "count": 9,
        "not_resolved": 0,
        "resolved": 9
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10306",
        "title": "Основные услуги - Нет исходящих вызовов",
        "count": 18,
        "not_resolved": 2,
        "resolved": 16
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10308",
        "title": "Основные услуги - Не работает интернет",
        "count": 15,
        "not_resolved": 0,
        "resolved": 15
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10309",
        "title": "Основные услуги - Не работает узел или приложение",
        "count": 3,
        "not_resolved": 0,
        "resolved": 3
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10311",
        "title": "Основные услуги - Проблема получения SMS",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10312",
        "title": "Основные услуги - Проблема отправки SMS",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10406",
        "title": "Основные услуги - Ekscliuzivnyi_FRAUD",
        "count": 6,
        "not_resolved": 0,
        "resolved": 6
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10302",
        "project_key": "CSA",
        "issuetype": "10328",
        "title": "Контент, дополнительные услуги и акции - Акции",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10303",
        "project_key": "SC",
        "issuetype": "10319",
        "title": "Self Care - Проблема подключения",
        "count": 11,
        "not_resolved": 0,
        "resolved": 11
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10316",
        "title": "Платежи - Нет платежа",
        "count": 109,
        "not_resolved": 0,
        "resolved": 109
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10317",
        "title": "Платежи - Заблокирован номер",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10404",
        "title": "Платежи - Разблокировка электронного кошелька",
        "count": 56,
        "not_resolved": 1,
        "resolved": 55
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10405",
        "title": "Платежи - Заблокировать кошелек",
        "count": 4,
        "not_resolved": 0,
        "resolved": 4
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10701",
        "title": "Платежи - Корректировка платежа",
        "count": 29,
        "not_resolved": 2,
        "resolved": 27
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10801",
        "title": "Платежи - О! Деньги - удаление кошелька",
        "count": 2,
        "not_resolved": 0,
        "resolved": 2
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "11900",
        "title": "Платежи - О!Бонус",
        "count": 2,
        "not_resolved": 0,
        "resolved": 2
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11301",
        "title": "TERMINALS - Аннуляция платежа",
        "count": 24,
        "not_resolved": 24,
        "resolved": 0
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11502",
        "title": "TERMINALS - Нет платежа - Терминал",
        "count": 138,
        "not_resolved": 11,
        "resolved": 127
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11600",
        "title": "TERMINALS - Корректировка платежа - Терминал",
        "count": 18,
        "not_resolved": 0,
        "resolved": 18
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11602",
        "title": "TERMINALS - Установка терминала - Терминал",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11605",
        "title": "TERMINALS - Технические проблемы - Терминал",
        "count": 7,
        "not_resolved": 0,
        "resolved": 7
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11607",
        "title": "TERMINALS - Подзадача - Терминал",
        "count": 4,
        "not_resolved": 0,
        "resolved": 4
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "12707",
        "title": "TERMINALS - Требуется чек о платеже",
        "count": 12,
        "not_resolved": 0,
        "resolved": 12
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "11603",
        "project_key": "OFD",
        "issuetype": "12201",
        "title": "OFD - Проблема - ОФД",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "11603",
        "project_key": "OFD",
        "issuetype": "12500",
        "title": "OFD - Процедуры ОФД",
        "count": 2,
        "not_resolved": 1,
        "resolved": 1
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10304",
        "title": "Основные услуги - Некорректная тарификация",
        "count": 4,
        "not_resolved": 1,
        "resolved": 3
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10305",
        "title": "Основные услуги - Нет входящих вызовов",
        "count": 5,
        "not_resolved": 0,
        "resolved": 5
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10306",
        "title": "Основные услуги - Нет исходящих вызовов",
        "count": 5,
        "not_resolved": 0,
        "resolved": 5
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10308",
        "title": "Основные услуги - Не работает интернет",
        "count": 29,
        "not_resolved": 1,
        "resolved": 28
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10311",
        "title": "Основные услуги - Проблема получения SMS",
        "count": 5,
        "not_resolved": 0,
        "resolved": 5
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10312",
        "title": "Основные услуги - Проблема отправки SMS",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10406",
        "title": "Основные услуги - Ekscliuzivnyi_FRAUD",
        "count": 9,
        "not_resolved": 0,
        "resolved": 9
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10302",
        "project_key": "CSA",
        "issuetype": "10325",
        "title": "Контент, дополнительные услуги и акции - Контент услуги",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10302",
        "project_key": "CSA",
        "issuetype": "10328",
        "title": "Контент, дополнительные услуги и акции - Акции",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10303",
        "project_key": "SC",
        "issuetype": "10319",
        "title": "Self Care - Проблема подключения",
        "count": 7,
        "not_resolved": 0,
        "resolved": 7
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10303",
        "project_key": "SC",
        "issuetype": "10324",
        "title": "Self Care - Активация или восстановление SIM карты",
        "count": 1,
        "not_resolved": 1,
        "resolved": 0
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10316",
        "title": "Платежи - Нет платежа",
        "count": 70,
        "not_resolved": 0,
        "resolved": 70
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10317",
        "title": "Платежи - Заблокирован номер",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10404",
        "title": "Платежи - Разблокировка электронного кошелька",
        "count": 54,
        "not_resolved": 0,
        "resolved": 54
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10405",
        "title": "Платежи - Заблокировать кошелек",
        "count": 8,
        "not_resolved": 0,
        "resolved": 8
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10701",
        "title": "Платежи - Корректировка платежа",
        "count": 36,
        "not_resolved": 1,
        "resolved": 35
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10801",
        "title": "Платежи - О! Деньги - удаление кошелька",
        "count": 4,
        "not_resolved": 0,
        "resolved": 4
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "11404",
        "title": "Платежи - Сотрудничество",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "11900",
        "title": "Платежи - О!Бонус",
        "count": 3,
        "not_resolved": 0,
        "resolved": 3
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10307",
        "project_key": "MP",
        "issuetype": "10100",
        "title": "Массовая проблема - Bug",
        "count": 425,
        "not_resolved": 0,
        "resolved": 425
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11301",
        "title": "TERMINALS - Аннуляция платежа",
        "count": 20,
        "not_resolved": 20,
        "resolved": 0
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11502",
        "title": "TERMINALS - Нет платежа - Терминал",
        "count": 117,
        "not_resolved": 13,
        "resolved": 104
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11600",
        "title": "TERMINALS - Корректировка платежа - Терминал",
        "count": 20,
        "not_resolved": 0,
        "resolved": 20
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11602",
        "title": "TERMINALS - Установка терминала - Терминал",
        "count": 1,
        "not_resolved": 1,
        "resolved": 0
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11603",
        "title": "TERMINALS - Оплата за аренду - Терминал",
        "count": 2,
        "not_resolved": 0,
        "resolved": 2
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11605",
        "title": "TERMINALS - Технические проблемы - Терминал",
        "count": 10,
        "not_resolved": 0,
        "resolved": 10
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11607",
        "title": "TERMINALS - Подзадача - Терминал",
        "count": 6,
        "not_resolved": 0,
        "resolved": 6
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "12707",
        "title": "TERMINALS - Требуется чек о платеже",
        "count": 6,
        "not_resolved": 0,
        "resolved": 6
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "11603",
        "project_key": "OFD",
        "issuetype": "12500",
        "title": "OFD - Процедуры ОФД",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10304",
        "title": "Основные услуги - Некорректная тарификация",
        "count": 5,
        "not_resolved": 0,
        "resolved": 5
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10305",
        "title": "Основные услуги - Нет входящих вызовов",
        "count": 2,
        "not_resolved": 0,
        "resolved": 2
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10306",
        "title": "Основные услуги - Нет исходящих вызовов",
        "count": 9,
        "not_resolved": 0,
        "resolved": 9
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10308",
        "title": "Основные услуги - Не работает интернет",
        "count": 15,
        "not_resolved": 0,
        "resolved": 15
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10309",
        "title": "Основные услуги - Не работает узел или приложение",
        "count": 2,
        "not_resolved": 0,
        "resolved": 2
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10311",
        "title": "Основные услуги - Проблема получения SMS",
        "count": 6,
        "not_resolved": 0,
        "resolved": 6
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10312",
        "title": "Основные услуги - Проблема отправки SMS",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10406",
        "title": "Основные услуги - Ekscliuzivnyi_FRAUD",
        "count": 1,
        "not_resolved": 1,
        "resolved": 0
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10302",
        "project_key": "CSA",
        "issuetype": "10325",
        "title": "Контент, дополнительные услуги и акции - Контент услуги",
        "count": 3,
        "not_resolved": 0,
        "resolved": 3
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10302",
        "project_key": "CSA",
        "issuetype": "10328",
        "title": "Контент, дополнительные услуги и акции - Акции",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10303",
        "project_key": "SC",
        "issuetype": "10319",
        "title": "Self Care - Проблема подключения",
        "count": 5,
        "not_resolved": 0,
        "resolved": 5
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10303",
        "project_key": "SC",
        "issuetype": "10321",
        "title": "Self Care - Проверка статуса",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10316",
        "title": "Платежи - Нет платежа",
        "count": 36,
        "not_resolved": 3,
        "resolved": 33
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10317",
        "title": "Платежи - Заблокирован номер",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10404",
        "title": "Платежи - Разблокировка электронного кошелька",
        "count": 35,
        "not_resolved": 0,
        "resolved": 35
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10405",
        "title": "Платежи - Заблокировать кошелек",
        "count": 2,
        "not_resolved": 0,
        "resolved": 2
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10701",
        "title": "Платежи - Корректировка платежа",
        "count": 37,
        "not_resolved": 2,
        "resolved": 35
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10801",
        "title": "Платежи - О! Деньги - удаление кошелька",
        "count": 7,
        "not_resolved": 0,
        "resolved": 7
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "11404",
        "title": "Платежи - Сотрудничество",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "11900",
        "title": "Платежи - О!Бонус",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10307",
        "project_key": "MP",
        "issuetype": "10100",
        "title": "Массовая проблема - Bug",
        "count": 7,
        "not_resolved": 0,
        "resolved": 7
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11301",
        "title": "TERMINALS - Аннуляция платежа",
        "count": 23,
        "not_resolved": 23,
        "resolved": 0
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11502",
        "title": "TERMINALS - Нет платежа - Терминал",
        "count": 86,
        "not_resolved": 5,
        "resolved": 81
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11600",
        "title": "TERMINALS - Корректировка платежа - Терминал",
        "count": 25,
        "not_resolved": 0,
        "resolved": 25
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11602",
        "title": "TERMINALS - Установка терминала - Терминал",
        "count": 3,
        "not_resolved": 3,
        "resolved": 0
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11603",
        "title": "TERMINALS - Оплата за аренду - Терминал",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11605",
        "title": "TERMINALS - Технические проблемы - Терминал",
        "count": 5,
        "not_resolved": 0,
        "resolved": 5
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11607",
        "title": "TERMINALS - Подзадача - Терминал",
        "count": 3,
        "not_resolved": 0,
        "resolved": 3
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "12707",
        "title": "TERMINALS - Требуется чек о платеже",
        "count": 3,
        "not_resolved": 0,
        "resolved": 3
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "11503",
        "project_key": "AG",
        "issuetype": "12101",
        "title": "AGENT - Проблемы с платежами",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "11603",
        "project_key": "OFD",
        "issuetype": "12500",
        "title": "OFD - Процедуры ОФД",
        "count": 1,
        "not_resolved": 1,
        "resolved": 0
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10304",
        "title": "Основные услуги - Некорректная тарификация",
        "count": 2,
        "not_resolved": 0,
        "resolved": 2
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10305",
        "title": "Основные услуги - Нет входящих вызовов",
        "count": 8,
        "not_resolved": 0,
        "resolved": 8
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10306",
        "title": "Основные услуги - Нет исходящих вызовов",
        "count": 7,
        "not_resolved": 0,
        "resolved": 7
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10308",
        "title": "Основные услуги - Не работает интернет",
        "count": 11,
        "not_resolved": 0,
        "resolved": 11
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10311",
        "title": "Основные услуги - Проблема получения SMS",
        "count": 4,
        "not_resolved": 0,
        "resolved": 4
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10312",
        "title": "Основные услуги - Проблема отправки SMS",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10406",
        "title": "Основные услуги - Ekscliuzivnyi_FRAUD",
        "count": 2,
        "not_resolved": 0,
        "resolved": 2
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "10303",
        "project_key": "SC",
        "issuetype": "10319",
        "title": "Self Care - Проблема подключения",
        "count": 6,
        "not_resolved": 0,
        "resolved": 6
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "10303",
        "project_key": "SC",
        "issuetype": "10320",
        "title": "Self Care - Проблема отключения",
        "count": 2,
        "not_resolved": 0,
        "resolved": 2
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10316",
        "title": "Платежи - Нет платежа",
        "count": 62,
        "not_resolved": 1,
        "resolved": 61
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10404",
        "title": "Платежи - Разблокировка электронного кошелька",
        "count": 30,
        "not_resolved": 0,
        "resolved": 30
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10405",
        "title": "Платежи - Заблокировать кошелек",
        "count": 3,
        "not_resolved": 0,
        "resolved": 3
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10701",
        "title": "Платежи - Корректировка платежа",
        "count": 27,
        "not_resolved": 1,
        "resolved": 26
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10801",
        "title": "Платежи - О! Деньги - удаление кошелька",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "11900",
        "title": "Платежи - О!Бонус",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11301",
        "title": "TERMINALS - Аннуляция платежа",
        "count": 18,
        "not_resolved": 18,
        "resolved": 0
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11502",
        "title": "TERMINALS - Нет платежа - Терминал",
        "count": 84,
        "not_resolved": 7,
        "resolved": 77
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11600",
        "title": "TERMINALS - Корректировка платежа - Терминал",
        "count": 13,
        "not_resolved": 0,
        "resolved": 13
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11602",
        "title": "TERMINALS - Установка терминала - Терминал",
        "count": 3,
        "not_resolved": 3,
        "resolved": 0
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11605",
        "title": "TERMINALS - Технические проблемы - Терминал",
        "count": 5,
        "not_resolved": 0,
        "resolved": 5
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11607",
        "title": "TERMINALS - Подзадача - Терминал",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "12707",
        "title": "TERMINALS - Требуется чек о платеже",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "11603",
        "project_key": "OFD",
        "issuetype": "12500",
        "title": "OFD - Процедуры ОФД",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10304",
        "title": "Основные услуги - Некорректная тарификация",
        "count": 2,
        "not_resolved": 1,
        "resolved": 1
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10305",
        "title": "Основные услуги - Нет входящих вызовов",
        "count": 5,
        "not_resolved": 0,
        "resolved": 5
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10306",
        "title": "Основные услуги - Нет исходящих вызовов",
        "count": 13,
        "not_resolved": 0,
        "resolved": 13
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10308",
        "title": "Основные услуги - Не работает интернет",
        "count": 33,
        "not_resolved": 2,
        "resolved": 31
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10309",
        "title": "Основные услуги - Не работает узел или приложение",
        "count": 2,
        "not_resolved": 0,
        "resolved": 2
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10311",
        "title": "Основные услуги - Проблема получения SMS",
        "count": 4,
        "not_resolved": 0,
        "resolved": 4
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10312",
        "title": "Основные услуги - Проблема отправки SMS",
        "count": 1,
        "not_resolved": 1,
        "resolved": 0
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10301",
        "project_key": "MS",
        "issuetype": "10406",
        "title": "Основные услуги - Ekscliuzivnyi_FRAUD",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10302",
        "project_key": "CSA",
        "issuetype": "10325",
        "title": "Контент, дополнительные услуги и акции - Контент услуги",
        "count": 2,
        "not_resolved": 0,
        "resolved": 2
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10302",
        "project_key": "CSA",
        "issuetype": "10328",
        "title": "Контент, дополнительные услуги и акции - Акции",
        "count": 2,
        "not_resolved": 0,
        "resolved": 2
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10303",
        "project_key": "SC",
        "issuetype": "10319",
        "title": "Self Care - Проблема подключения",
        "count": 10,
        "not_resolved": 0,
        "resolved": 10
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10303",
        "project_key": "SC",
        "issuetype": "10323",
        "title": "Self Care - Смена ТП",
        "count": 2,
        "not_resolved": 0,
        "resolved": 2
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10316",
        "title": "Платежи - Нет платежа",
        "count": 60,
        "not_resolved": 0,
        "resolved": 60
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10317",
        "title": "Платежи - Заблокирован номер",
        "count": 2,
        "not_resolved": 0,
        "resolved": 2
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10404",
        "title": "Платежи - Разблокировка электронного кошелька",
        "count": 48,
        "not_resolved": 0,
        "resolved": 48
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10405",
        "title": "Платежи - Заблокировать кошелек",
        "count": 8,
        "not_resolved": 0,
        "resolved": 8
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "10701",
        "title": "Платежи - Корректировка платежа",
        "count": 50,
        "not_resolved": 4,
        "resolved": 46
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10304",
        "project_key": "PAYM",
        "issuetype": "11900",
        "title": "Платежи - О!Бонус",
        "count": 3,
        "not_resolved": 1,
        "resolved": 2
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10307",
        "project_key": "MP",
        "issuetype": "10100",
        "title": "Массовая проблема - Bug",
        "count": 132,
        "not_resolved": 0,
        "resolved": 132
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11301",
        "title": "TERMINALS - Аннуляция платежа",
        "count": 28,
        "not_resolved": 28,
        "resolved": 0
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11502",
        "title": "TERMINALS - Нет платежа - Терминал",
        "count": 156,
        "not_resolved": 13,
        "resolved": 143
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11600",
        "title": "TERMINALS - Корректировка платежа - Терминал",
        "count": 22,
        "not_resolved": 0,
        "resolved": 22
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11602",
        "title": "TERMINALS - Установка терминала - Терминал",
        "count": 1,
        "not_resolved": 1,
        "resolved": 0
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11603",
        "title": "TERMINALS - Оплата за аренду - Терминал",
        "count": 2,
        "not_resolved": 0,
        "resolved": 2
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11605",
        "title": "TERMINALS - Технические проблемы - Терминал",
        "count": 6,
        "not_resolved": 0,
        "resolved": 6
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "11607",
        "title": "TERMINALS - Подзадача - Терминал",
        "count": 6,
        "not_resolved": 0,
        "resolved": 6
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "11309",
        "project_key": "TERM",
        "issuetype": "12707",
        "title": "TERMINALS - Требуется чек о платеже",
        "count": 9,
        "not_resolved": 0,
        "resolved": 9
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "11603",
        "project_key": "OFD",
        "issuetype": "12500",
        "title": "OFD - Процедуры ОФД",
        "count": 2,
        "not_resolved": 0,
        "resolved": 2
    }
]

get_created_issues_by_types_with_id_saima = [
    {
        "date": "2025-09-22 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10108",
        "title": "MAIN SERVICES - [SUB] SAIMA: На уточнении",
        "count": 8,
        "not_resolved": 1,
        "resolved": 7
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10209",
        "title": "MAIN SERVICES - Списалась АП, но ТП не подключился",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10212",
        "title": "MAIN SERVICES - ДС есть, но ТП не подключился",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10306",
        "title": "MAIN SERVICES - [SUB] Монтажники",
        "count": 242,
        "not_resolved": 10,
        "resolved": 232
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10307",
        "title": "MAIN SERVICES - [SUB] Инженеры",
        "count": 110,
        "not_resolved": 9,
        "resolved": 101
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10308",
        "title": "MAIN SERVICES - Не работает проводной интернет",
        "count": 294,
        "not_resolved": 250,
        "resolved": 44
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10309",
        "title": "MAIN SERVICES - Низкая скорость на проводном интернете",
        "count": 12,
        "not_resolved": 12,
        "resolved": 0
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10310",
        "title": "MAIN SERVICES - Не работает телефонная линия",
        "count": 1,
        "not_resolved": 1,
        "resolved": 0
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10311",
        "title": "MAIN SERVICES - Нет исходящих вызовов",
        "count": 1,
        "not_resolved": 1,
        "resolved": 0
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10317",
        "title": "MAIN SERVICES - Затруднение в работе O!TV",
        "count": 53,
        "not_resolved": 46,
        "resolved": 7
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10901",
        "title": "MAIN SERVICES - ГУАА - Демонтаж оборудования ",
        "count": 48,
        "not_resolved": 48,
        "resolved": 0
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "11505",
        "title": "MAIN SERVICES - Обращения с сайта: Поддержка",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "11606",
        "title": "MAIN SERVICES - Аварийные работы",
        "count": 26,
        "not_resolved": 0,
        "resolved": 26
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "13900",
        "title": "MAIN SERVICES - [SUB] Замена оборудования",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10101",
        "title": "CUSTOMER SERVICES - Подключение нового абонента",
        "count": 350,
        "not_resolved": 204,
        "resolved": 146
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10102",
        "title": "CUSTOMER SERVICES - [SUB] Новый абонент - ОАП: Просчёт",
        "count": 3,
        "not_resolved": 0,
        "resolved": 3
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10104",
        "title": "CUSTOMER SERVICES - [SUB] Новый абонент - ОАП: Монтаж",
        "count": 211,
        "not_resolved": 33,
        "resolved": 178
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10108",
        "title": "CUSTOMER SERVICES - [SUB] SAIMA: На уточнении",
        "count": 3,
        "not_resolved": 0,
        "resolved": 3
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10214",
        "title": "CUSTOMER SERVICES - Перенос кабеля в одном здании",
        "count": 10,
        "not_resolved": 4,
        "resolved": 6
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10314",
        "title": "CUSTOMER SERVICES - Перерасчёт",
        "count": 9,
        "not_resolved": 0,
        "resolved": 9
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10401",
        "title": "CUSTOMER SERVICES - [SUB] Новый абонент: На уточнении",
        "count": 11,
        "not_resolved": 0,
        "resolved": 11
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10501",
        "title": "CUSTOMER SERVICES - Восстановление линии",
        "count": 3,
        "not_resolved": 1,
        "resolved": 2
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10502",
        "title": "CUSTOMER SERVICES - Перенос кабеля на другой адрес",
        "count": 22,
        "not_resolved": 3,
        "resolved": 19
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10513",
        "title": "CUSTOMER SERVICES - Подключение нового корпоративного абонента",
        "count": 13,
        "not_resolved": 10,
        "resolved": 3
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10802",
        "title": "CUSTOMER SERVICES - Нет покрытия",
        "count": 4,
        "not_resolved": 4,
        "resolved": 0
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10813",
        "title": "CUSTOMER SERVICES - [SUB] Новый абонент - ГТМ",
        "count": 132,
        "not_resolved": 1,
        "resolved": 131
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "11102",
        "title": "CUSTOMER SERVICES - Исправление данных в договоре",
        "count": 2,
        "not_resolved": 2,
        "resolved": 0
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "11707",
        "title": "CUSTOMER SERVICES - SUB: Перерасчёт",
        "count": 1,
        "not_resolved": 1,
        "resolved": 0
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "11718",
        "title": "CUSTOMER SERVICES - ТП Сотрудник 550 + О!TV от Сайма Телеком",
        "count": 3,
        "not_resolved": 1,
        "resolved": 2
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "11722",
        "title": "CUSTOMER SERVICES - [SUB] Восстановление линии",
        "count": 2,
        "not_resolved": 2,
        "resolved": 0
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "12000",
        "title": "CUSTOMER SERVICES - ОКП: Добавление адреса в справочник",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "12200",
        "title": "CUSTOMER SERVICES - Тех лист: На исправление",
        "count": 19,
        "not_resolved": 0,
        "resolved": 19
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "12208",
        "title": "CUSTOMER SERVICES - Установка роутера (выкуп)",
        "count": 1,
        "not_resolved": 1,
        "resolved": 0
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "12300",
        "title": "CUSTOMER SERVICES - Добавление адреса",
        "count": 24,
        "not_resolved": 2,
        "resolved": 22
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "12313",
        "title": "CUSTOMER SERVICES - Установка дополнительной O!TV приставки (выкуп)",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "13207",
        "title": "CUSTOMER SERVICES - Смена ТП 300/500",
        "count": 5,
        "not_resolved": 3,
        "resolved": 2
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "13614",
        "title": "CUSTOMER SERVICES - На включение xDSL",
        "count": 2,
        "not_resolved": 2,
        "resolved": 0
    },
    {
        "date": "2025-09-22 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "14800",
        "title": "CUSTOMER SERVICES - Изменение адреса",
        "count": 14,
        "not_resolved": 14,
        "resolved": 0
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10108",
        "title": "MAIN SERVICES - [SUB] SAIMA: На уточнении",
        "count": 14,
        "not_resolved": 1,
        "resolved": 13
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10209",
        "title": "MAIN SERVICES - Списалась АП, но ТП не подключился",
        "count": 5,
        "not_resolved": 0,
        "resolved": 5
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10211",
        "title": "MAIN SERVICES - Физически подключили, но сервисы не назначались",
        "count": 2,
        "not_resolved": 0,
        "resolved": 2
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10306",
        "title": "MAIN SERVICES - [SUB] Монтажники",
        "count": 196,
        "not_resolved": 10,
        "resolved": 186
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10307",
        "title": "MAIN SERVICES - [SUB] Инженеры",
        "count": 157,
        "not_resolved": 8,
        "resolved": 149
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10308",
        "title": "MAIN SERVICES - Не работает проводной интернет",
        "count": 316,
        "not_resolved": 241,
        "resolved": 75
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10309",
        "title": "MAIN SERVICES - Низкая скорость на проводном интернете",
        "count": 7,
        "not_resolved": 6,
        "resolved": 1
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10317",
        "title": "MAIN SERVICES - Затруднение в работе O!TV",
        "count": 48,
        "not_resolved": 44,
        "resolved": 4
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10901",
        "title": "MAIN SERVICES - ГУАА - Демонтаж оборудования ",
        "count": 8,
        "not_resolved": 8,
        "resolved": 0
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "11000",
        "title": "MAIN SERVICES - Проблема подключения услуг",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "11302",
        "title": "MAIN SERVICES - [SUB] Монтажники - ОАП",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "11505",
        "title": "MAIN SERVICES - Обращения с сайта: Поддержка",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "11606",
        "title": "MAIN SERVICES - Аварийные работы",
        "count": 11,
        "not_resolved": 0,
        "resolved": 11
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "13100",
        "title": "MAIN SERVICES - Демонтаж в US",
        "count": 2,
        "not_resolved": 1,
        "resolved": 1
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "13900",
        "title": "MAIN SERVICES - [SUB] Замена оборудования",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10101",
        "title": "CUSTOMER SERVICES - Подключение нового абонента",
        "count": 339,
        "not_resolved": 172,
        "resolved": 167
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10102",
        "title": "CUSTOMER SERVICES - [SUB] Новый абонент - ОАП: Просчёт",
        "count": 2,
        "not_resolved": 0,
        "resolved": 2
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10104",
        "title": "CUSTOMER SERVICES - [SUB] Новый абонент - ОАП: Монтаж",
        "count": 177,
        "not_resolved": 18,
        "resolved": 159
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10108",
        "title": "CUSTOMER SERVICES - [SUB] SAIMA: На уточнении",
        "count": 14,
        "not_resolved": 0,
        "resolved": 14
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10214",
        "title": "CUSTOMER SERVICES - Перенос кабеля в одном здании",
        "count": 4,
        "not_resolved": 3,
        "resolved": 1
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10314",
        "title": "CUSTOMER SERVICES - Перерасчёт",
        "count": 16,
        "not_resolved": 0,
        "resolved": 16
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10401",
        "title": "CUSTOMER SERVICES - [SUB] Новый абонент: На уточнении",
        "count": 5,
        "not_resolved": 0,
        "resolved": 5
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10501",
        "title": "CUSTOMER SERVICES - Восстановление линии",
        "count": 4,
        "not_resolved": 2,
        "resolved": 2
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10502",
        "title": "CUSTOMER SERVICES - Перенос кабеля на другой адрес",
        "count": 17,
        "not_resolved": 2,
        "resolved": 15
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10513",
        "title": "CUSTOMER SERVICES - Подключение нового корпоративного абонента",
        "count": 22,
        "not_resolved": 18,
        "resolved": 4
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10802",
        "title": "CUSTOMER SERVICES - Нет покрытия",
        "count": 4,
        "not_resolved": 4,
        "resolved": 0
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10813",
        "title": "CUSTOMER SERVICES - [SUB] Новый абонент - ГТМ",
        "count": 139,
        "not_resolved": 0,
        "resolved": 139
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "11102",
        "title": "CUSTOMER SERVICES - Исправление данных в договоре",
        "count": 3,
        "not_resolved": 3,
        "resolved": 0
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "11718",
        "title": "CUSTOMER SERVICES - ТП Сотрудник 550 + О!TV от Сайма Телеком",
        "count": 1,
        "not_resolved": 1,
        "resolved": 0
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "11722",
        "title": "CUSTOMER SERVICES - [SUB] Восстановление линии",
        "count": 1,
        "not_resolved": 1,
        "resolved": 0
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "12200",
        "title": "CUSTOMER SERVICES - Тех лист: На исправление",
        "count": 19,
        "not_resolved": 0,
        "resolved": 19
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "12208",
        "title": "CUSTOMER SERVICES - Установка роутера (выкуп)",
        "count": 3,
        "not_resolved": 3,
        "resolved": 0
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "12300",
        "title": "CUSTOMER SERVICES - Добавление адреса",
        "count": 21,
        "not_resolved": 4,
        "resolved": 17
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "12313",
        "title": "CUSTOMER SERVICES - Установка дополнительной O!TV приставки (выкуп)",
        "count": 3,
        "not_resolved": 1,
        "resolved": 2
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "13207",
        "title": "CUSTOMER SERVICES - Смена ТП 300/500",
        "count": 9,
        "not_resolved": 6,
        "resolved": 3
    },
    {
        "date": "2025-09-23 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "14800",
        "title": "CUSTOMER SERVICES - Изменение адреса",
        "count": 15,
        "not_resolved": 15,
        "resolved": 0
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10108",
        "title": "MAIN SERVICES - [SUB] SAIMA: На уточнении",
        "count": 13,
        "not_resolved": 0,
        "resolved": 13
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10209",
        "title": "MAIN SERVICES - Списалась АП, но ТП не подключился",
        "count": 2,
        "not_resolved": 0,
        "resolved": 2
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10306",
        "title": "MAIN SERVICES - [SUB] Монтажники",
        "count": 266,
        "not_resolved": 10,
        "resolved": 256
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10307",
        "title": "MAIN SERVICES - [SUB] Инженеры",
        "count": 129,
        "not_resolved": 10,
        "resolved": 119
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10308",
        "title": "MAIN SERVICES - Не работает проводной интернет",
        "count": 369,
        "not_resolved": 278,
        "resolved": 91
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10309",
        "title": "MAIN SERVICES - Низкая скорость на проводном интернете",
        "count": 7,
        "not_resolved": 7,
        "resolved": 0
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10310",
        "title": "MAIN SERVICES - Не работает телефонная линия",
        "count": 1,
        "not_resolved": 1,
        "resolved": 0
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10317",
        "title": "MAIN SERVICES - Затруднение в работе O!TV",
        "count": 71,
        "not_resolved": 54,
        "resolved": 17
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10901",
        "title": "MAIN SERVICES - ГУАА - Демонтаж оборудования ",
        "count": 6,
        "not_resolved": 6,
        "resolved": 0
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "11000",
        "title": "MAIN SERVICES - Проблема подключения услуг",
        "count": 2,
        "not_resolved": 0,
        "resolved": 2
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "11606",
        "title": "MAIN SERVICES - Аварийные работы",
        "count": 29,
        "not_resolved": 0,
        "resolved": 29
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "11708",
        "title": "MAIN SERVICES - Платный выезд: Не работает проводной интернет",
        "count": 1,
        "not_resolved": 1,
        "resolved": 0
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "13900",
        "title": "MAIN SERVICES - [SUB] Замена оборудования",
        "count": 2,
        "not_resolved": 1,
        "resolved": 1
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10101",
        "title": "CUSTOMER SERVICES - Подключение нового абонента",
        "count": 338,
        "not_resolved": 203,
        "resolved": 135
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10102",
        "title": "CUSTOMER SERVICES - [SUB] Новый абонент - ОАП: Просчёт",
        "count": 4,
        "not_resolved": 0,
        "resolved": 4
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10104",
        "title": "CUSTOMER SERVICES - [SUB] Новый абонент - ОАП: Монтаж",
        "count": 209,
        "not_resolved": 31,
        "resolved": 178
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10108",
        "title": "CUSTOMER SERVICES - [SUB] SAIMA: На уточнении",
        "count": 19,
        "not_resolved": 0,
        "resolved": 19
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10214",
        "title": "CUSTOMER SERVICES - Перенос кабеля в одном здании",
        "count": 5,
        "not_resolved": 2,
        "resolved": 3
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10314",
        "title": "CUSTOMER SERVICES - Перерасчёт",
        "count": 13,
        "not_resolved": 2,
        "resolved": 11
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10401",
        "title": "CUSTOMER SERVICES - [SUB] Новый абонент: На уточнении",
        "count": 4,
        "not_resolved": 0,
        "resolved": 4
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10502",
        "title": "CUSTOMER SERVICES - Перенос кабеля на другой адрес",
        "count": 21,
        "not_resolved": 7,
        "resolved": 14
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10503",
        "title": "CUSTOMER SERVICES - Смена технологии",
        "count": 1,
        "not_resolved": 1,
        "resolved": 0
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10513",
        "title": "CUSTOMER SERVICES - Подключение нового корпоративного абонента",
        "count": 15,
        "not_resolved": 15,
        "resolved": 0
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10802",
        "title": "CUSTOMER SERVICES - Нет покрытия",
        "count": 4,
        "not_resolved": 4,
        "resolved": 0
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10813",
        "title": "CUSTOMER SERVICES - [SUB] Новый абонент - ГТМ",
        "count": 119,
        "not_resolved": 0,
        "resolved": 119
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "11102",
        "title": "CUSTOMER SERVICES - Исправление данных в договоре",
        "count": 4,
        "not_resolved": 4,
        "resolved": 0
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "11718",
        "title": "CUSTOMER SERVICES - ТП Сотрудник 550 + О!TV от Сайма Телеком",
        "count": 3,
        "not_resolved": 3,
        "resolved": 0
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "12200",
        "title": "CUSTOMER SERVICES - Тех лист: На исправление",
        "count": 29,
        "not_resolved": 5,
        "resolved": 24
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "12208",
        "title": "CUSTOMER SERVICES - Установка роутера (выкуп)",
        "count": 1,
        "not_resolved": 1,
        "resolved": 0
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "12300",
        "title": "CUSTOMER SERVICES - Добавление адреса",
        "count": 12,
        "not_resolved": 1,
        "resolved": 11
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "12302",
        "title": "CUSTOMER SERVICES - Перерасчет в биллинге",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "13207",
        "title": "CUSTOMER SERVICES - Смена ТП 300/500",
        "count": 8,
        "not_resolved": 8,
        "resolved": 0
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "13614",
        "title": "CUSTOMER SERVICES - На включение xDSL",
        "count": 1,
        "not_resolved": 1,
        "resolved": 0
    },
    {
        "date": "2025-09-24 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "14800",
        "title": "CUSTOMER SERVICES - Изменение адреса",
        "count": 19,
        "not_resolved": 19,
        "resolved": 0
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10108",
        "title": "MAIN SERVICES - [SUB] SAIMA: На уточнении",
        "count": 9,
        "not_resolved": 0,
        "resolved": 9
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10209",
        "title": "MAIN SERVICES - Списалась АП, но ТП не подключился",
        "count": 2,
        "not_resolved": 0,
        "resolved": 2
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10211",
        "title": "MAIN SERVICES - Физически подключили, но сервисы не назначались",
        "count": 3,
        "not_resolved": 0,
        "resolved": 3
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10212",
        "title": "MAIN SERVICES - ДС есть, но ТП не подключился",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10306",
        "title": "MAIN SERVICES - [SUB] Монтажники",
        "count": 210,
        "not_resolved": 11,
        "resolved": 199
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10307",
        "title": "MAIN SERVICES - [SUB] Инженеры",
        "count": 122,
        "not_resolved": 5,
        "resolved": 117
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10308",
        "title": "MAIN SERVICES - Не работает проводной интернет",
        "count": 322,
        "not_resolved": 231,
        "resolved": 91
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10309",
        "title": "MAIN SERVICES - Низкая скорость на проводном интернете",
        "count": 7,
        "not_resolved": 7,
        "resolved": 0
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10317",
        "title": "MAIN SERVICES - Затруднение в работе O!TV",
        "count": 68,
        "not_resolved": 59,
        "resolved": 9
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10901",
        "title": "MAIN SERVICES - ГУАА - Демонтаж оборудования ",
        "count": 199,
        "not_resolved": 198,
        "resolved": 1
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "11302",
        "title": "MAIN SERVICES - [SUB] Монтажники - ОАП",
        "count": 2,
        "not_resolved": 1,
        "resolved": 1
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "11505",
        "title": "MAIN SERVICES - Обращения с сайта: Поддержка",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "11606",
        "title": "MAIN SERVICES - Аварийные работы",
        "count": 15,
        "not_resolved": 0,
        "resolved": 15
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10101",
        "title": "CUSTOMER SERVICES - Подключение нового абонента",
        "count": 327,
        "not_resolved": 160,
        "resolved": 167
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10102",
        "title": "CUSTOMER SERVICES - [SUB] Новый абонент - ОАП: Просчёт",
        "count": 3,
        "not_resolved": 0,
        "resolved": 3
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10104",
        "title": "CUSTOMER SERVICES - [SUB] Новый абонент - ОАП: Монтаж",
        "count": 182,
        "not_resolved": 32,
        "resolved": 150
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10108",
        "title": "CUSTOMER SERVICES - [SUB] SAIMA: На уточнении",
        "count": 2,
        "not_resolved": 0,
        "resolved": 2
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10214",
        "title": "CUSTOMER SERVICES - Перенос кабеля в одном здании",
        "count": 3,
        "not_resolved": 0,
        "resolved": 3
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10314",
        "title": "CUSTOMER SERVICES - Перерасчёт",
        "count": 11,
        "not_resolved": 3,
        "resolved": 8
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10401",
        "title": "CUSTOMER SERVICES - [SUB] Новый абонент: На уточнении",
        "count": 9,
        "not_resolved": 0,
        "resolved": 9
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10501",
        "title": "CUSTOMER SERVICES - Восстановление линии",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10502",
        "title": "CUSTOMER SERVICES - Перенос кабеля на другой адрес",
        "count": 14,
        "not_resolved": 2,
        "resolved": 12
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10513",
        "title": "CUSTOMER SERVICES - Подключение нового корпоративного абонента",
        "count": 12,
        "not_resolved": 10,
        "resolved": 2
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10802",
        "title": "CUSTOMER SERVICES - Нет покрытия",
        "count": 2,
        "not_resolved": 2,
        "resolved": 0
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10813",
        "title": "CUSTOMER SERVICES - [SUB] Новый абонент - ГТМ",
        "count": 160,
        "not_resolved": 0,
        "resolved": 160
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "11102",
        "title": "CUSTOMER SERVICES - Исправление данных в договоре",
        "count": 2,
        "not_resolved": 2,
        "resolved": 0
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "12000",
        "title": "CUSTOMER SERVICES - ОКП: Добавление адреса в справочник",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "12200",
        "title": "CUSTOMER SERVICES - Тех лист: На исправление",
        "count": 32,
        "not_resolved": 7,
        "resolved": 25
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "12208",
        "title": "CUSTOMER SERVICES - Установка роутера (выкуп)",
        "count": 1,
        "not_resolved": 1,
        "resolved": 0
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "12300",
        "title": "CUSTOMER SERVICES - Добавление адреса",
        "count": 14,
        "not_resolved": 2,
        "resolved": 12
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "12908",
        "title": "CUSTOMER SERVICES - Перевод в красную зону",
        "count": 1,
        "not_resolved": 1,
        "resolved": 0
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "13207",
        "title": "CUSTOMER SERVICES - Смена ТП 300/500",
        "count": 6,
        "not_resolved": 5,
        "resolved": 1
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "13614",
        "title": "CUSTOMER SERVICES - На включение xDSL",
        "count": 2,
        "not_resolved": 2,
        "resolved": 0
    },
    {
        "date": "2025-09-25 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "14800",
        "title": "CUSTOMER SERVICES - Изменение адреса",
        "count": 18,
        "not_resolved": 18,
        "resolved": 0
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10108",
        "title": "MAIN SERVICES - [SUB] SAIMA: На уточнении",
        "count": 16,
        "not_resolved": 1,
        "resolved": 15
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10209",
        "title": "MAIN SERVICES - Списалась АП, но ТП не подключился",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10306",
        "title": "MAIN SERVICES - [SUB] Монтажники",
        "count": 262,
        "not_resolved": 12,
        "resolved": 250
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10307",
        "title": "MAIN SERVICES - [SUB] Инженеры",
        "count": 126,
        "not_resolved": 5,
        "resolved": 121
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10308",
        "title": "MAIN SERVICES - Не работает проводной интернет",
        "count": 358,
        "not_resolved": 268,
        "resolved": 90
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10309",
        "title": "MAIN SERVICES - Низкая скорость на проводном интернете",
        "count": 5,
        "not_resolved": 5,
        "resolved": 0
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10317",
        "title": "MAIN SERVICES - Затруднение в работе O!TV",
        "count": 61,
        "not_resolved": 50,
        "resolved": 11
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10404",
        "title": "MAIN SERVICES - [SUB] IT SUPPORT",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10901",
        "title": "MAIN SERVICES - ГУАА - Демонтаж оборудования ",
        "count": 4,
        "not_resolved": 4,
        "resolved": 0
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10904",
        "title": "MAIN SERVICES - Проблема отключения услуг",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "11000",
        "title": "MAIN SERVICES - Проблема подключения услуг",
        "count": 2,
        "not_resolved": 0,
        "resolved": 2
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "11302",
        "title": "MAIN SERVICES - [SUB] Монтажники - ОАП",
        "count": 2,
        "not_resolved": 2,
        "resolved": 0
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "11505",
        "title": "MAIN SERVICES - Обращения с сайта: Поддержка",
        "count": 2,
        "not_resolved": 0,
        "resolved": 2
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "11606",
        "title": "MAIN SERVICES - Аварийные работы",
        "count": 18,
        "not_resolved": 0,
        "resolved": 18
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "13100",
        "title": "MAIN SERVICES - Демонтаж в US",
        "count": 28,
        "not_resolved": 0,
        "resolved": 28
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10101",
        "title": "CUSTOMER SERVICES - Подключение нового абонента",
        "count": 291,
        "not_resolved": 152,
        "resolved": 139
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10102",
        "title": "CUSTOMER SERVICES - [SUB] Новый абонент - ОАП: Просчёт",
        "count": 2,
        "not_resolved": 0,
        "resolved": 2
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10104",
        "title": "CUSTOMER SERVICES - [SUB] Новый абонент - ОАП: Монтаж",
        "count": 150,
        "not_resolved": 21,
        "resolved": 129
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10108",
        "title": "CUSTOMER SERVICES - [SUB] SAIMA: На уточнении",
        "count": 4,
        "not_resolved": 0,
        "resolved": 4
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10214",
        "title": "CUSTOMER SERVICES - Перенос кабеля в одном здании",
        "count": 2,
        "not_resolved": 0,
        "resolved": 2
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10314",
        "title": "CUSTOMER SERVICES - Перерасчёт",
        "count": 17,
        "not_resolved": 1,
        "resolved": 16
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10401",
        "title": "CUSTOMER SERVICES - [SUB] Новый абонент: На уточнении",
        "count": 7,
        "not_resolved": 0,
        "resolved": 7
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10501",
        "title": "CUSTOMER SERVICES - Восстановление линии",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10502",
        "title": "CUSTOMER SERVICES - Перенос кабеля на другой адрес",
        "count": 13,
        "not_resolved": 2,
        "resolved": 11
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10513",
        "title": "CUSTOMER SERVICES - Подключение нового корпоративного абонента",
        "count": 12,
        "not_resolved": 10,
        "resolved": 2
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10802",
        "title": "CUSTOMER SERVICES - Нет покрытия",
        "count": 2,
        "not_resolved": 2,
        "resolved": 0
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10813",
        "title": "CUSTOMER SERVICES - [SUB] Новый абонент - ГТМ",
        "count": 128,
        "not_resolved": 0,
        "resolved": 128
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "11102",
        "title": "CUSTOMER SERVICES - Исправление данных в договоре",
        "count": 3,
        "not_resolved": 3,
        "resolved": 0
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "11718",
        "title": "CUSTOMER SERVICES - ТП Сотрудник 550 + О!TV от Сайма Телеком",
        "count": 1,
        "not_resolved": 1,
        "resolved": 0
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "12200",
        "title": "CUSTOMER SERVICES - Тех лист: На исправление",
        "count": 13,
        "not_resolved": 3,
        "resolved": 10
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "12208",
        "title": "CUSTOMER SERVICES - Установка роутера (выкуп)",
        "count": 2,
        "not_resolved": 2,
        "resolved": 0
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "12300",
        "title": "CUSTOMER SERVICES - Добавление адреса",
        "count": 19,
        "not_resolved": 5,
        "resolved": 14
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "12313",
        "title": "CUSTOMER SERVICES - Установка дополнительной O!TV приставки (выкуп)",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "13207",
        "title": "CUSTOMER SERVICES - Смена ТП 300/500",
        "count": 8,
        "not_resolved": 8,
        "resolved": 0
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "13614",
        "title": "CUSTOMER SERVICES - На включение xDSL",
        "count": 1,
        "not_resolved": 1,
        "resolved": 0
    },
    {
        "date": "2025-09-26 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "14800",
        "title": "CUSTOMER SERVICES - Изменение адреса",
        "count": 15,
        "not_resolved": 15,
        "resolved": 0
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10108",
        "title": "MAIN SERVICES - [SUB] SAIMA: На уточнении",
        "count": 13,
        "not_resolved": 0,
        "resolved": 13
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10209",
        "title": "MAIN SERVICES - Списалась АП, но ТП не подключился",
        "count": 2,
        "not_resolved": 0,
        "resolved": 2
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10306",
        "title": "MAIN SERVICES - [SUB] Монтажники",
        "count": 201,
        "not_resolved": 12,
        "resolved": 189
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10307",
        "title": "MAIN SERVICES - [SUB] Инженеры",
        "count": 121,
        "not_resolved": 3,
        "resolved": 118
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10308",
        "title": "MAIN SERVICES - Не работает проводной интернет",
        "count": 262,
        "not_resolved": 210,
        "resolved": 52
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10309",
        "title": "MAIN SERVICES - Низкая скорость на проводном интернете",
        "count": 12,
        "not_resolved": 11,
        "resolved": 1
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10312",
        "title": "MAIN SERVICES - Затруднение в работе IPTV",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10317",
        "title": "MAIN SERVICES - Затруднение в работе O!TV",
        "count": 71,
        "not_resolved": 63,
        "resolved": 8
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10901",
        "title": "MAIN SERVICES - ГУАА - Демонтаж оборудования ",
        "count": 1,
        "not_resolved": 1,
        "resolved": 0
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "11000",
        "title": "MAIN SERVICES - Проблема подключения услуг",
        "count": 3,
        "not_resolved": 0,
        "resolved": 3
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "11606",
        "title": "MAIN SERVICES - Аварийные работы",
        "count": 4,
        "not_resolved": 0,
        "resolved": 4
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "11708",
        "title": "MAIN SERVICES - Платный выезд: Не работает проводной интернет",
        "count": 2,
        "not_resolved": 2,
        "resolved": 0
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "13900",
        "title": "MAIN SERVICES - [SUB] Замена оборудования",
        "count": 1,
        "not_resolved": 1,
        "resolved": 0
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10101",
        "title": "CUSTOMER SERVICES - Подключение нового абонента",
        "count": 265,
        "not_resolved": 128,
        "resolved": 137
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10104",
        "title": "CUSTOMER SERVICES - [SUB] Новый абонент - ОАП: Монтаж",
        "count": 122,
        "not_resolved": 14,
        "resolved": 108
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10214",
        "title": "CUSTOMER SERVICES - Перенос кабеля в одном здании",
        "count": 5,
        "not_resolved": 0,
        "resolved": 5
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10314",
        "title": "CUSTOMER SERVICES - Перерасчёт",
        "count": 9,
        "not_resolved": 1,
        "resolved": 8
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10320",
        "title": "CUSTOMER SERVICES - [SUB] Новый абонент - ОДД",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10401",
        "title": "CUSTOMER SERVICES - [SUB] Новый абонент: На уточнении",
        "count": 6,
        "not_resolved": 0,
        "resolved": 6
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10501",
        "title": "CUSTOMER SERVICES - Восстановление линии",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10502",
        "title": "CUSTOMER SERVICES - Перенос кабеля на другой адрес",
        "count": 10,
        "not_resolved": 3,
        "resolved": 7
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10513",
        "title": "CUSTOMER SERVICES - Подключение нового корпоративного абонента",
        "count": 4,
        "not_resolved": 4,
        "resolved": 0
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10802",
        "title": "CUSTOMER SERVICES - Нет покрытия",
        "count": 3,
        "not_resolved": 3,
        "resolved": 0
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10813",
        "title": "CUSTOMER SERVICES - [SUB] Новый абонент - ГТМ",
        "count": 117,
        "not_resolved": 1,
        "resolved": 116
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "11102",
        "title": "CUSTOMER SERVICES - Исправление данных в договоре",
        "count": 4,
        "not_resolved": 4,
        "resolved": 0
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "12200",
        "title": "CUSTOMER SERVICES - Тех лист: На исправление",
        "count": 14,
        "not_resolved": 4,
        "resolved": 10
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "12208",
        "title": "CUSTOMER SERVICES - Установка роутера (выкуп)",
        "count": 1,
        "not_resolved": 1,
        "resolved": 0
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "12300",
        "title": "CUSTOMER SERVICES - Добавление адреса",
        "count": 12,
        "not_resolved": 0,
        "resolved": 12
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "12313",
        "title": "CUSTOMER SERVICES - Установка дополнительной O!TV приставки (выкуп)",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "13207",
        "title": "CUSTOMER SERVICES - Смена ТП 300/500",
        "count": 7,
        "not_resolved": 6,
        "resolved": 1
    },
    {
        "date": "2025-09-27 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "14800",
        "title": "CUSTOMER SERVICES - Изменение адреса",
        "count": 5,
        "not_resolved": 5,
        "resolved": 0
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10108",
        "title": "MAIN SERVICES - [SUB] SAIMA: На уточнении",
        "count": 10,
        "not_resolved": 0,
        "resolved": 10
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10209",
        "title": "MAIN SERVICES - Списалась АП, но ТП не подключился",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10306",
        "title": "MAIN SERVICES - [SUB] Монтажники",
        "count": 151,
        "not_resolved": 7,
        "resolved": 144
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10307",
        "title": "MAIN SERVICES - [SUB] Инженеры",
        "count": 91,
        "not_resolved": 5,
        "resolved": 86
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10308",
        "title": "MAIN SERVICES - Не работает проводной интернет",
        "count": 200,
        "not_resolved": 168,
        "resolved": 32
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10309",
        "title": "MAIN SERVICES - Низкая скорость на проводном интернете",
        "count": 6,
        "not_resolved": 6,
        "resolved": 0
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10317",
        "title": "MAIN SERVICES - Затруднение в работе O!TV",
        "count": 48,
        "not_resolved": 44,
        "resolved": 4
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "11000",
        "title": "MAIN SERVICES - Проблема подключения услуг",
        "count": 2,
        "not_resolved": 0,
        "resolved": 2
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "11505",
        "title": "MAIN SERVICES - Обращения с сайта: Поддержка",
        "count": 3,
        "not_resolved": 0,
        "resolved": 3
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "11606",
        "title": "MAIN SERVICES - Аварийные работы",
        "count": 2,
        "not_resolved": 0,
        "resolved": 2
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "13900",
        "title": "MAIN SERVICES - [SUB] Замена оборудования",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10101",
        "title": "CUSTOMER SERVICES - Подключение нового абонента",
        "count": 241,
        "not_resolved": 113,
        "resolved": 128
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10104",
        "title": "CUSTOMER SERVICES - [SUB] Новый абонент - ОАП: Монтаж",
        "count": 118,
        "not_resolved": 17,
        "resolved": 101
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10202",
        "title": "CUSTOMER SERVICES - Согласование особых условий",
        "count": 1,
        "not_resolved": 1,
        "resolved": 0
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10214",
        "title": "CUSTOMER SERVICES - Перенос кабеля в одном здании",
        "count": 3,
        "not_resolved": 1,
        "resolved": 2
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10314",
        "title": "CUSTOMER SERVICES - Перерасчёт",
        "count": 11,
        "not_resolved": 1,
        "resolved": 10
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10401",
        "title": "CUSTOMER SERVICES - [SUB] Новый абонент: На уточнении",
        "count": 9,
        "not_resolved": 0,
        "resolved": 9
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10502",
        "title": "CUSTOMER SERVICES - Перенос кабеля на другой адрес",
        "count": 13,
        "not_resolved": 2,
        "resolved": 11
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10513",
        "title": "CUSTOMER SERVICES - Подключение нового корпоративного абонента",
        "count": 7,
        "not_resolved": 7,
        "resolved": 0
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10802",
        "title": "CUSTOMER SERVICES - Нет покрытия",
        "count": 4,
        "not_resolved": 4,
        "resolved": 0
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10813",
        "title": "CUSTOMER SERVICES - [SUB] Новый абонент - ГТМ",
        "count": 117,
        "not_resolved": 0,
        "resolved": 117
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "11102",
        "title": "CUSTOMER SERVICES - Исправление данных в договоре",
        "count": 1,
        "not_resolved": 1,
        "resolved": 0
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "12200",
        "title": "CUSTOMER SERVICES - Тех лист: На исправление",
        "count": 12,
        "not_resolved": 2,
        "resolved": 10
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "12300",
        "title": "CUSTOMER SERVICES - Добавление адреса",
        "count": 8,
        "not_resolved": 1,
        "resolved": 7
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "12313",
        "title": "CUSTOMER SERVICES - Установка дополнительной O!TV приставки (выкуп)",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "13207",
        "title": "CUSTOMER SERVICES - Смена ТП 300/500",
        "count": 6,
        "not_resolved": 3,
        "resolved": 3
    },
    {
        "date": "2025-09-28 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "14800",
        "title": "CUSTOMER SERVICES - Изменение адреса",
        "count": 8,
        "not_resolved": 8,
        "resolved": 0
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10108",
        "title": "MAIN SERVICES - [SUB] SAIMA: На уточнении",
        "count": 17,
        "not_resolved": 1,
        "resolved": 16
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10209",
        "title": "MAIN SERVICES - Списалась АП, но ТП не подключился",
        "count": 2,
        "not_resolved": 0,
        "resolved": 2
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10212",
        "title": "MAIN SERVICES - ДС есть, но ТП не подключился",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10306",
        "title": "MAIN SERVICES - [SUB] Монтажники",
        "count": 236,
        "not_resolved": 13,
        "resolved": 223
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10307",
        "title": "MAIN SERVICES - [SUB] Инженеры",
        "count": 145,
        "not_resolved": 7,
        "resolved": 138
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10308",
        "title": "MAIN SERVICES - Не работает проводной интернет",
        "count": 311,
        "not_resolved": 267,
        "resolved": 44
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10309",
        "title": "MAIN SERVICES - Низкая скорость на проводном интернете",
        "count": 14,
        "not_resolved": 13,
        "resolved": 1
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10310",
        "title": "MAIN SERVICES - Не работает телефонная линия",
        "count": 1,
        "not_resolved": 1,
        "resolved": 0
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10317",
        "title": "MAIN SERVICES - Затруднение в работе O!TV",
        "count": 51,
        "not_resolved": 41,
        "resolved": 10
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "10901",
        "title": "MAIN SERVICES - ГУАА - Демонтаж оборудования ",
        "count": 451,
        "not_resolved": 451,
        "resolved": 0
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "11000",
        "title": "MAIN SERVICES - Проблема подключения услуг",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "11505",
        "title": "MAIN SERVICES - Обращения с сайта: Поддержка",
        "count": 1,
        "not_resolved": 1,
        "resolved": 0
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "11606",
        "title": "MAIN SERVICES - Аварийные работы",
        "count": 24,
        "not_resolved": 0,
        "resolved": 24
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "13100",
        "title": "MAIN SERVICES - Демонтаж в US",
        "count": 28,
        "not_resolved": 27,
        "resolved": 1
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10100",
        "project_key": "MS",
        "issuetype": "13900",
        "title": "MAIN SERVICES - [SUB] Замена оборудования",
        "count": 5,
        "not_resolved": 1,
        "resolved": 4
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10101",
        "title": "CUSTOMER SERVICES - Подключение нового абонента",
        "count": 407,
        "not_resolved": 225,
        "resolved": 182
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10102",
        "title": "CUSTOMER SERVICES - [SUB] Новый абонент - ОАП: Просчёт",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10104",
        "title": "CUSTOMER SERVICES - [SUB] Новый абонент - ОАП: Монтаж",
        "count": 227,
        "not_resolved": 57,
        "resolved": 170
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10108",
        "title": "CUSTOMER SERVICES - [SUB] SAIMA: На уточнении",
        "count": 13,
        "not_resolved": 0,
        "resolved": 13
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10214",
        "title": "CUSTOMER SERVICES - Перенос кабеля в одном здании",
        "count": 5,
        "not_resolved": 2,
        "resolved": 3
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10303",
        "title": "CUSTOMER SERVICES - Подключение без согласия жильцов",
        "count": 1,
        "not_resolved": 1,
        "resolved": 0
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10314",
        "title": "CUSTOMER SERVICES - Перерасчёт",
        "count": 16,
        "not_resolved": 0,
        "resolved": 16
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10401",
        "title": "CUSTOMER SERVICES - [SUB] Новый абонент: На уточнении",
        "count": 4,
        "not_resolved": 0,
        "resolved": 4
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10501",
        "title": "CUSTOMER SERVICES - Восстановление линии",
        "count": 2,
        "not_resolved": 1,
        "resolved": 1
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10502",
        "title": "CUSTOMER SERVICES - Перенос кабеля на другой адрес",
        "count": 20,
        "not_resolved": 9,
        "resolved": 11
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10513",
        "title": "CUSTOMER SERVICES - Подключение нового корпоративного абонента",
        "count": 13,
        "not_resolved": 10,
        "resolved": 3
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10802",
        "title": "CUSTOMER SERVICES - Нет покрытия",
        "count": 2,
        "not_resolved": 2,
        "resolved": 0
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "10813",
        "title": "CUSTOMER SERVICES - [SUB] Новый абонент - ГТМ",
        "count": 159,
        "not_resolved": 0,
        "resolved": 159
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "11102",
        "title": "CUSTOMER SERVICES - Исправление данных в договоре",
        "count": 13,
        "not_resolved": 13,
        "resolved": 0
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "11718",
        "title": "CUSTOMER SERVICES - ТП Сотрудник 550 + О!TV от Сайма Телеком",
        "count": 4,
        "not_resolved": 0,
        "resolved": 4
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "12000",
        "title": "CUSTOMER SERVICES - ОКП: Добавление адреса в справочник",
        "count": 1,
        "not_resolved": 0,
        "resolved": 1
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "12200",
        "title": "CUSTOMER SERVICES - Тех лист: На исправление",
        "count": 17,
        "not_resolved": 2,
        "resolved": 15
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "12208",
        "title": "CUSTOMER SERVICES - Установка роутера (выкуп)",
        "count": 2,
        "not_resolved": 2,
        "resolved": 0
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "12300",
        "title": "CUSTOMER SERVICES - Добавление адреса",
        "count": 9,
        "not_resolved": 0,
        "resolved": 9
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "13207",
        "title": "CUSTOMER SERVICES - Смена ТП 300/500",
        "count": 8,
        "not_resolved": 1,
        "resolved": 7
    },
    {
        "date": "2025-09-29 00:00:00",
        "id": "10101",
        "project_key": "CS",
        "issuetype": "14800",
        "title": "CUSTOMER SERVICES - Изменение адреса",
        "count": 6,
        "not_resolved": 6,
        "resolved": 0
    }
]

SLA_MOCK_DATA   = json.loads(r'''[
    {
        "parent_issue_type": "Восстановление линии",
        "total_count": 128,
        "subtask_type": null,
        "subtasks_count": 112,
        "open_subtasks_count": 76,
        "closed_subtasks_count": 36,
        "ontime_closed_subtasks_count": 10,
        "sla_plan_days": "8.00",
        "sla_fact_days": "2.79"
    },
    {
        "parent_issue_type": "Восстановление линии",
        "total_count": 128,
        "subtask_type": "[SUB] Восстановление линии",
        "subtasks_count": 10,
        "open_subtasks_count": 10,
        "closed_subtasks_count": 0,
        "ontime_closed_subtasks_count": 0,
        "sla_plan_days": "0",
        "sla_fact_days": "0"
    },
    {
        "parent_issue_type": "Восстановление линии",
        "total_count": 128,
        "subtask_type": "[SUB] SAIMA: На уточнении",
        "subtasks_count": 8,
        "open_subtasks_count": 0,
        "closed_subtasks_count": 8,
        "ontime_closed_subtasks_count": 5,
        "sla_plan_days": "8.00",
        "sla_fact_days": "2.37"
    },
    {
        "parent_issue_type": "Добавление адреса",
        "total_count": 433,
        "subtask_type": null,
        "subtasks_count": 433,
        "open_subtasks_count": 34,
        "closed_subtasks_count": 399,
        "ontime_closed_subtasks_count": 283,
        "sla_plan_days": "8.00",
        "sla_fact_days": "1.57"
    },
    {
        "parent_issue_type": "Добавление проекта в Биллинг",
        "total_count": 1,
        "subtask_type": null,
        "subtasks_count": 1,
        "open_subtasks_count": 1,
        "closed_subtasks_count": 0,
        "ontime_closed_subtasks_count": 0,
        "sla_plan_days": "0",
        "sla_fact_days": "0"
    },
    {
        "parent_issue_type": "Изменение адреса",
        "total_count": 369,
        "subtask_type": null,
        "subtasks_count": 369,
        "open_subtasks_count": 368,
        "closed_subtasks_count": 1,
        "ontime_closed_subtasks_count": 1,
        "sla_plan_days": "8.00",
        "sla_fact_days": "0.75"
    },
    {
        "parent_issue_type": "Исправление данных в договоре",
        "total_count": 122,
        "subtask_type": null,
        "subtasks_count": 122,
        "open_subtasks_count": 120,
        "closed_subtasks_count": 2,
        "ontime_closed_subtasks_count": 2,
        "sla_plan_days": "8.00",
        "sla_fact_days": "0.29"
    },
    {
        "parent_issue_type": "На включение xDSL",
        "total_count": 24,
        "subtask_type": null,
        "subtasks_count": 24,
        "open_subtasks_count": 24,
        "closed_subtasks_count": 0,
        "ontime_closed_subtasks_count": 0,
        "sla_plan_days": "0",
        "sla_fact_days": "0"
    },
    {
        "parent_issue_type": "Нет покрытия",
        "total_count": 111,
        "subtask_type": null,
        "subtasks_count": 111,
        "open_subtasks_count": 111,
        "closed_subtasks_count": 0,
        "ontime_closed_subtasks_count": 0,
        "sla_plan_days": "0",
        "sla_fact_days": "0"
    },
    {
        "parent_issue_type": "ОКП: Добавление адреса в справочник",
        "total_count": 18,
        "subtask_type": null,
        "subtasks_count": 18,
        "open_subtasks_count": 0,
        "closed_subtasks_count": 18,
        "ontime_closed_subtasks_count": 17,
        "sla_plan_days": "8.00",
        "sla_fact_days": "0.50"
    },
    {
        "parent_issue_type": "Обращения по аренде",
        "total_count": 1,
        "subtask_type": null,
        "subtasks_count": 1,
        "open_subtasks_count": 1,
        "closed_subtasks_count": 0,
        "ontime_closed_subtasks_count": 0,
        "sla_plan_days": "0",
        "sla_fact_days": "0"
    },
    {
        "parent_issue_type": "Перевод в красную зону",
        "total_count": 1,
        "subtask_type": null,
        "subtasks_count": 1,
        "open_subtasks_count": 1,
        "closed_subtasks_count": 0,
        "ontime_closed_subtasks_count": 0,
        "sla_plan_days": "0",
        "sla_fact_days": "0"
    },
    {
        "parent_issue_type": "Перенос кабеля в одном здании",
        "total_count": 138,
        "subtask_type": null,
        "subtasks_count": 135,
        "open_subtasks_count": 26,
        "closed_subtasks_count": 109,
        "ontime_closed_subtasks_count": 57,
        "sla_plan_days": "8.00",
        "sla_fact_days": "4.58"
    },
    {
        "parent_issue_type": "Перенос кабеля в одном здании",
        "total_count": 138,
        "subtask_type": "[SUB] SAIMA: На уточнении",
        "subtasks_count": 3,
        "open_subtasks_count": 0,
        "closed_subtasks_count": 3,
        "ontime_closed_subtasks_count": 2,
        "sla_plan_days": "8.00",
        "sla_fact_days": "4.82"
    },
    {
        "parent_issue_type": "Перенос кабеля на другой адрес",
        "total_count": 483,
        "subtask_type": null,
        "subtasks_count": 382,
        "open_subtasks_count": 96,
        "closed_subtasks_count": 288,
        "ontime_closed_subtasks_count": 57,
        "sla_plan_days": "8.00",
        "sla_fact_days": "2.90"
    },
    {
        "parent_issue_type": "Перенос кабеля на другой адрес",
        "total_count": 483,
        "subtask_type": "[SUB] SAIMA: На уточнении",
        "subtasks_count": 145,
        "open_subtasks_count": 0,
        "closed_subtasks_count": 145,
        "ontime_closed_subtasks_count": 71,
        "sla_plan_days": "8.00",
        "sla_fact_days": "4.16"
    },
    {
        "parent_issue_type": "Перерасчет в биллинге",
        "total_count": 16,
        "subtask_type": null,
        "subtasks_count": 16,
        "open_subtasks_count": 0,
        "closed_subtasks_count": 16,
        "ontime_closed_subtasks_count": 15,
        "sla_plan_days": "8.00",
        "sla_fact_days": "0.14"
    },
    {
        "parent_issue_type": "Перерасчёт",
        "total_count": 679,
        "subtask_type": null,
        "subtasks_count": 661,
        "open_subtasks_count": 75,
        "closed_subtasks_count": 586,
        "ontime_closed_subtasks_count": 360,
        "sla_plan_days": "8.00",
        "sla_fact_days": "3.45"
    },
    {
        "parent_issue_type": "Перерасчёт",
        "total_count": 679,
        "subtask_type": "Перерасчет в биллинге",
        "subtasks_count": 16,
        "open_subtasks_count": 0,
        "closed_subtasks_count": 16,
        "ontime_closed_subtasks_count": 15,
        "sla_plan_days": "8.00",
        "sla_fact_days": "0.14"
    },
    {
        "parent_issue_type": "Перерасчёт",
        "total_count": 679,
        "subtask_type": "SUB: Перерасчёт",
        "subtasks_count": 2,
        "open_subtasks_count": 2,
        "closed_subtasks_count": 0,
        "ontime_closed_subtasks_count": 0,
        "sla_plan_days": "0",
        "sla_fact_days": "0"
    },
    {
        "parent_issue_type": "Подключение без согласия жильцов",
        "total_count": 2,
        "subtask_type": null,
        "subtasks_count": 2,
        "open_subtasks_count": 2,
        "closed_subtasks_count": 0,
        "ontime_closed_subtasks_count": 0,
        "sla_plan_days": "0",
        "sla_fact_days": "0"
    },
    {
        "parent_issue_type": "Подключение нового абонента",
        "total_count": 10247,
        "subtask_type": "[SUB] Новый абонент - ОАП: Монтаж",
        "subtasks_count": 4969,
        "open_subtasks_count": 666,
        "closed_subtasks_count": 4303,
        "ontime_closed_subtasks_count": 29,
        "sla_plan_days": "8.00",
        "sla_fact_days": "1.42"
    },
    {
        "parent_issue_type": "Подключение нового абонента",
        "total_count": 10247,
        "subtask_type": "[SUB] Новый абонент - ГТМ",
        "subtasks_count": 4579,
        "open_subtasks_count": 5,
        "closed_subtasks_count": 4574,
        "ontime_closed_subtasks_count": 3178,
        "sla_plan_days": "8.00",
        "sla_fact_days": "1.03"
    },
    {
        "parent_issue_type": "Подключение нового абонента",
        "total_count": 10247,
        "subtask_type": null,
        "subtasks_count": 761,
        "open_subtasks_count": 310,
        "closed_subtasks_count": 454,
        "ontime_closed_subtasks_count": 323,
        "sla_plan_days": "8.00",
        "sla_fact_days": "1.25"
    },
    {
        "parent_issue_type": "Подключение нового абонента",
        "total_count": 10247,
        "subtask_type": "[SUB] Новый абонент: На уточнении",
        "subtasks_count": 193,
        "open_subtasks_count": 2,
        "closed_subtasks_count": 191,
        "ontime_closed_subtasks_count": 117,
        "sla_plan_days": "8.00",
        "sla_fact_days": "3.08"
    },
    {
        "parent_issue_type": "Подключение нового абонента",
        "total_count": 10247,
        "subtask_type": "[SUB] Новый абонент - ОАП: Просчёт",
        "subtasks_count": 43,
        "open_subtasks_count": 0,
        "closed_subtasks_count": 43,
        "ontime_closed_subtasks_count": 42,
        "sla_plan_days": "8.00",
        "sla_fact_days": "0.10"
    },
    {
        "parent_issue_type": "Подключение нового корпоративного абонента",
        "total_count": 395,
        "subtask_type": null,
        "subtasks_count": 395,
        "open_subtasks_count": 314,
        "closed_subtasks_count": 81,
        "ontime_closed_subtasks_count": 36,
        "sla_plan_days": "8.00",
        "sla_fact_days": "3.65"
    },
    {
        "parent_issue_type": "Смена ТП 300/500",
        "total_count": 149,
        "subtask_type": null,
        "subtasks_count": 75,
        "open_subtasks_count": 31,
        "closed_subtasks_count": 55,
        "ontime_closed_subtasks_count": 9,
        "sla_plan_days": "8.00",
        "sla_fact_days": "1.20"
    },
    {
        "parent_issue_type": "Смена ТП 300/500",
        "total_count": 149,
        "subtask_type": "[SUB] Замена оборудования",
        "subtasks_count": 50,
        "open_subtasks_count": 9,
        "closed_subtasks_count": 41,
        "ontime_closed_subtasks_count": 29,
        "sla_plan_days": "8.00",
        "sla_fact_days": "3.07"
    },
    {
        "parent_issue_type": "Смена ТП 300/500",
        "total_count": 149,
        "subtask_type": "[SUB] SAIMA: На уточнении",
        "subtasks_count": 2,
        "open_subtasks_count": 1,
        "closed_subtasks_count": 1,
        "ontime_closed_subtasks_count": 0,
        "sla_plan_days": "0",
        "sla_fact_days": "0"
    },
    {
        "parent_issue_type": "Смена технологии",
        "total_count": 2,
        "subtask_type": null,
        "subtasks_count": 2,
        "open_subtasks_count": 2,
        "closed_subtasks_count": 0,
        "ontime_closed_subtasks_count": 0,
        "sla_plan_days": "0",
        "sla_fact_days": "0"
    },
    {
        "parent_issue_type": "Согласование особых условий",
        "total_count": 2,
        "subtask_type": null,
        "subtasks_count": 2,
        "open_subtasks_count": 2,
        "closed_subtasks_count": 0,
        "ontime_closed_subtasks_count": 0,
        "sla_plan_days": "0",
        "sla_fact_days": "0"
    },
    {
        "parent_issue_type": "ТП Сотрудник 550 + О!TV от Сайма Телеком",
        "total_count": 29,
        "subtask_type": null,
        "subtasks_count": 27,
        "open_subtasks_count": 6,
        "closed_subtasks_count": 21,
        "ontime_closed_subtasks_count": 19,
        "sla_plan_days": "8.00",
        "sla_fact_days": "1.64"
    },
    {
        "parent_issue_type": "ТП Сотрудник 550 + О!TV от Сайма Телеком",
        "total_count": 29,
        "subtask_type": "Добавление адреса",
        "subtasks_count": 2,
        "open_subtasks_count": 1,
        "closed_subtasks_count": 1,
        "ontime_closed_subtasks_count": 1,
        "sla_plan_days": "8.00",
        "sla_fact_days": "0.02"
    },
    {
        "parent_issue_type": "Тех лист: На исправление",
        "total_count": 496,
        "subtask_type": null,
        "subtasks_count": 496,
        "open_subtasks_count": 66,
        "closed_subtasks_count": 430,
        "ontime_closed_subtasks_count": 429,
        "sla_plan_days": "8.00",
        "sla_fact_days": "0.05"
    },
    {
        "parent_issue_type": "Установка дополнительной O!TV приставки (выкуп)",
        "total_count": 28,
        "subtask_type": null,
        "subtasks_count": 3,
        "open_subtasks_count": 0,
        "closed_subtasks_count": 3,
        "ontime_closed_subtasks_count": 0,
        "sla_plan_days": "0",
        "sla_fact_days": "0"
    },
    {
        "parent_issue_type": "Установка роутера (выкуп)",
        "total_count": 64,
        "subtask_type": "[SUB] Инженеры",
        "subtasks_count": 64,
        "open_subtasks_count": 16,
        "closed_subtasks_count": 48,
        "ontime_closed_subtasks_count": 19,
        "sla_plan_days": "8.00",
        "sla_fact_days": "3.47"
    },
    {
        "parent_issue_type": "Установка роутера (выкуп)",
        "total_count": 64,
        "subtask_type": "[SUB] SAIMA: На уточнении",
        "subtasks_count": 3,
        "open_subtasks_count": 0,
        "closed_subtasks_count": 3,
        "ontime_closed_subtasks_count": 3,
        "sla_plan_days": "8.00",
        "sla_fact_days": "3.06"
    }
]''')

SLA_MS_DATA = json.loads(r'''[
    {
        "parent_issue_type": "Аварийные работы",
        "total_count": 389,
        "subtask_type": null,
        "subtasks_count": 389,
        "open_subtasks_count": 0,
        "closed_subtasks_count": 389,
        "ontime_closed_subtasks_count": 359,
        "sla_plan_days": "8.00",
        "sla_fact_days": "2.10"
    },
    {
        "parent_issue_type": "Версия TV-приставки",
        "total_count": 7,
        "subtask_type": "[SUB] SAIMA: На уточнении",
        "subtasks_count": 6,
        "open_subtasks_count": 5,
        "closed_subtasks_count": 1,
        "ontime_closed_subtasks_count": 0,
        "sla_plan_days": "0",
        "sla_fact_days": "0"
    },
    {
        "parent_issue_type": "Версия TV-приставки",
        "total_count": 7,
        "subtask_type": null,
        "subtasks_count": 1,
        "open_subtasks_count": 1,
        "closed_subtasks_count": 0,
        "ontime_closed_subtasks_count": 0,
        "sla_plan_days": "0",
        "sla_fact_days": "0"
    },
    {
        "parent_issue_type": "ГУАА - Демонтаж оборудования ",
        "total_count": 9574,
        "subtask_type": null,
        "subtasks_count": 9567,
        "open_subtasks_count": 9562,
        "closed_subtasks_count": 5,
        "ontime_closed_subtasks_count": 0,
        "sla_plan_days": "0",
        "sla_fact_days": "0"
    },
    {
        "parent_issue_type": "ГУАА - Демонтаж оборудования ",
        "total_count": 9574,
        "subtask_type": "Восстановление линии",
        "subtasks_count": 7,
        "open_subtasks_count": 2,
        "closed_subtasks_count": 5,
        "ontime_closed_subtasks_count": 5,
        "sla_plan_days": "8.00",
        "sla_fact_days": "3.63"
    },
    {
        "parent_issue_type": "ДС есть, но ТП не подключился",
        "total_count": 21,
        "subtask_type": null,
        "subtasks_count": 13,
        "open_subtasks_count": 1,
        "closed_subtasks_count": 12,
        "ontime_closed_subtasks_count": 9,
        "sla_plan_days": "8.00",
        "sla_fact_days": "0.98"
    },
    {
        "parent_issue_type": "ДС есть, но ТП не подключился",
        "total_count": 21,
        "subtask_type": "[SUB] SAIMA: На уточнении",
        "subtasks_count": 8,
        "open_subtasks_count": 0,
        "closed_subtasks_count": 8,
        "ontime_closed_subtasks_count": 5,
        "sla_plan_days": "8.00",
        "sla_fact_days": "1.65"
    },
    {
        "parent_issue_type": "ДС есть, но ТП не подключился",
        "total_count": 21,
        "subtask_type": "[SUB] SAIMA: Активация сервисов - Backoffice",
        "subtasks_count": 1,
        "open_subtasks_count": 0,
        "closed_subtasks_count": 1,
        "ontime_closed_subtasks_count": 0,
        "sla_plan_days": "0",
        "sla_fact_days": "0"
    },
    {
        "parent_issue_type": "Демонтаж в US",
        "total_count": 542,
        "subtask_type": null,
        "subtasks_count": 383,
        "open_subtasks_count": 227,
        "closed_subtasks_count": 156,
        "ontime_closed_subtasks_count": 52,
        "sla_plan_days": "8.00",
        "sla_fact_days": "2.86"
    },
    {
        "parent_issue_type": "Демонтаж в US",
        "total_count": 542,
        "subtask_type": "ГУАА - Демонтаж оборудования ",
        "subtasks_count": 159,
        "open_subtasks_count": 159,
        "closed_subtasks_count": 0,
        "ontime_closed_subtasks_count": 0,
        "sla_plan_days": "0",
        "sla_fact_days": "0"
    },
    {
        "parent_issue_type": "Затруднение в работе IPTV",
        "total_count": 5,
        "subtask_type": "[SUB] SAIMA: На уточнении",
        "subtasks_count": 3,
        "open_subtasks_count": 0,
        "closed_subtasks_count": 3,
        "ontime_closed_subtasks_count": 2,
        "sla_plan_days": "8.00",
        "sla_fact_days": "2.03"
    },
    {
        "parent_issue_type": "Затруднение в работе IPTV",
        "total_count": 5,
        "subtask_type": "[SUB] Инженеры",
        "subtasks_count": 2,
        "open_subtasks_count": 0,
        "closed_subtasks_count": 2,
        "ontime_closed_subtasks_count": 1,
        "sla_plan_days": "8.00",
        "sla_fact_days": "7.63"
    },
    {
        "parent_issue_type": "Затруднение в работе IPTV",
        "total_count": 5,
        "subtask_type": null,
        "subtasks_count": 1,
        "open_subtasks_count": 0,
        "closed_subtasks_count": 1,
        "ontime_closed_subtasks_count": 0,
        "sla_plan_days": "0",
        "sla_fact_days": "0"
    },
    {
        "parent_issue_type": "Затруднение в работе O!TV",
        "total_count": 2035,
        "subtask_type": "[SUB] Инженеры",
        "subtasks_count": 1841,
        "open_subtasks_count": 92,
        "closed_subtasks_count": 1749,
        "ontime_closed_subtasks_count": 1188,
        "sla_plan_days": "8.00",
        "sla_fact_days": "3.63"
    },
    {
        "parent_issue_type": "Затруднение в работе O!TV",
        "total_count": 2035,
        "subtask_type": "[SUB] SAIMA: На уточнении",
        "subtasks_count": 116,
        "open_subtasks_count": 0,
        "closed_subtasks_count": 116,
        "ontime_closed_subtasks_count": 90,
        "sla_plan_days": "8.00",
        "sla_fact_days": "2.71"
    },
    {
        "parent_issue_type": "Затруднение в работе O!TV",
        "total_count": 2035,
        "subtask_type": null,
        "subtasks_count": 83,
        "open_subtasks_count": 6,
        "closed_subtasks_count": 77,
        "ontime_closed_subtasks_count": 66,
        "sla_plan_days": "8.00",
        "sla_fact_days": "0.67"
    },
    {
        "parent_issue_type": "Затруднение в работе O!TV",
        "total_count": 2035,
        "subtask_type": "[SUB] Монтажники",
        "subtasks_count": 61,
        "open_subtasks_count": 5,
        "closed_subtasks_count": 56,
        "ontime_closed_subtasks_count": 41,
        "sla_plan_days": "8.00",
        "sla_fact_days": "1.95"
    },
    {
        "parent_issue_type": "Затруднение в работе O!TV",
        "total_count": 2035,
        "subtask_type": "[SUB] Монтажники - ОАП",
        "subtasks_count": 2,
        "open_subtasks_count": 1,
        "closed_subtasks_count": 1,
        "ontime_closed_subtasks_count": 0,
        "sla_plan_days": "0",
        "sla_fact_days": "0"
    },
    {
        "parent_issue_type": "Не работает проводной интернет",
        "total_count": 9476,
        "subtask_type": "[SUB] Монтажники",
        "subtasks_count": 6590,
        "open_subtasks_count": 398,
        "closed_subtasks_count": 6192,
        "ontime_closed_subtasks_count": 3691,
        "sla_plan_days": "8.00",
        "sla_fact_days": "4.08"
    },
    {
        "parent_issue_type": "Не работает проводной интернет",
        "total_count": 9476,
        "subtask_type": "[SUB] Инженеры",
        "subtasks_count": 2035,
        "open_subtasks_count": 96,
        "closed_subtasks_count": 1939,
        "ontime_closed_subtasks_count": 1248,
        "sla_plan_days": "8.00",
        "sla_fact_days": "3.77"
    },
    {
        "parent_issue_type": "Не работает проводной интернет",
        "total_count": 9476,
        "subtask_type": null,
        "subtasks_count": 790,
        "open_subtasks_count": 34,
        "closed_subtasks_count": 759,
        "ontime_closed_subtasks_count": 729,
        "sla_plan_days": "8.00",
        "sla_fact_days": "0.34"
    },
    {
        "parent_issue_type": "Не работает проводной интернет",
        "total_count": 9476,
        "subtask_type": "[SUB] SAIMA: На уточнении",
        "subtasks_count": 289,
        "open_subtasks_count": 6,
        "closed_subtasks_count": 283,
        "ontime_closed_subtasks_count": 241,
        "sla_plan_days": "8.00",
        "sla_fact_days": "2.61"
    },
    {
        "parent_issue_type": "Не работает проводной интернет",
        "total_count": 9476,
        "subtask_type": "[SUB] Монтажники - ОАП",
        "subtasks_count": 17,
        "open_subtasks_count": 6,
        "closed_subtasks_count": 11,
        "ontime_closed_subtasks_count": 4,
        "sla_plan_days": "8.00",
        "sla_fact_days": "3.76"
    },
    {
        "parent_issue_type": "Не работает проводной интернет",
        "total_count": 9476,
        "subtask_type": "Не работает проводной интернет",
        "subtasks_count": 3,
        "open_subtasks_count": 0,
        "closed_subtasks_count": 3,
        "ontime_closed_subtasks_count": 2,
        "sla_plan_days": "8.00",
        "sla_fact_days": "0.62"
    },
    {
        "parent_issue_type": "Не работает телефонная линия",
        "total_count": 15,
        "subtask_type": "[SUB] Монтажники",
        "subtasks_count": 8,
        "open_subtasks_count": 0,
        "closed_subtasks_count": 8,
        "ontime_closed_subtasks_count": 1,
        "sla_plan_days": "8.00",
        "sla_fact_days": "0.18"
    },
    {
        "parent_issue_type": "Не работает телефонная линия",
        "total_count": 15,
        "subtask_type": "[SUB] Инженеры",
        "subtasks_count": 4,
        "open_subtasks_count": 0,
        "closed_subtasks_count": 4,
        "ontime_closed_subtasks_count": 1,
        "sla_plan_days": "8.00",
        "sla_fact_days": "7.87"
    },
    {
        "parent_issue_type": "Не работает телефонная линия",
        "total_count": 15,
        "subtask_type": null,
        "subtasks_count": 3,
        "open_subtasks_count": 0,
        "closed_subtasks_count": 3,
        "ontime_closed_subtasks_count": 3,
        "sla_plan_days": "8.00",
        "sla_fact_days": "2.40"
    },
    {
        "parent_issue_type": "Не работает телефонная линия",
        "total_count": 15,
        "subtask_type": "[SUB] SAIMA: На уточнении",
        "subtasks_count": 2,
        "open_subtasks_count": 0,
        "closed_subtasks_count": 2,
        "ontime_closed_subtasks_count": 2,
        "sla_plan_days": "8.00",
        "sla_fact_days": "0.84"
    },
    {
        "parent_issue_type": "Нет исходящих вызовов",
        "total_count": 3,
        "subtask_type": "[SUB] Монтажники",
        "subtasks_count": 2,
        "open_subtasks_count": 0,
        "closed_subtasks_count": 2,
        "ontime_closed_subtasks_count": 1,
        "sla_plan_days": "8.00",
        "sla_fact_days": "0.15"
    },
    {
        "parent_issue_type": "Нет исходящих вызовов",
        "total_count": 3,
        "subtask_type": null,
        "subtasks_count": 1,
        "open_subtasks_count": 0,
        "closed_subtasks_count": 1,
        "ontime_closed_subtasks_count": 1,
        "sla_plan_days": "8.00",
        "sla_fact_days": "0.00"
    },
    {
        "parent_issue_type": "Низкая скорость на проводном интернете",
        "total_count": 329,
        "subtask_type": "[SUB] Инженеры",
        "subtasks_count": 287,
        "open_subtasks_count": 12,
        "closed_subtasks_count": 275,
        "ontime_closed_subtasks_count": 151,
        "sla_plan_days": "8.00",
        "sla_fact_days": "4.17"
    },
    {
        "parent_issue_type": "Низкая скорость на проводном интернете",
        "total_count": 329,
        "subtask_type": "[SUB] Монтажники",
        "subtasks_count": 47,
        "open_subtasks_count": 0,
        "closed_subtasks_count": 47,
        "ontime_closed_subtasks_count": 24,
        "sla_plan_days": "8.00",
        "sla_fact_days": "3.61"
    },
    {
        "parent_issue_type": "Низкая скорость на проводном интернете",
        "total_count": 329,
        "subtask_type": "[SUB] SAIMA: На уточнении",
        "subtasks_count": 21,
        "open_subtasks_count": 3,
        "closed_subtasks_count": 18,
        "ontime_closed_subtasks_count": 16,
        "sla_plan_days": "8.00",
        "sla_fact_days": "1.68"
    },
    {
        "parent_issue_type": "Низкая скорость на проводном интернете",
        "total_count": 329,
        "subtask_type": null,
        "subtasks_count": 7,
        "open_subtasks_count": 6,
        "closed_subtasks_count": 3,
        "ontime_closed_subtasks_count": 3,
        "sla_plan_days": "8.00",
        "sla_fact_days": "0.17"
    },
    {
        "parent_issue_type": "Низкая скорость на проводном интернете",
        "total_count": 329,
        "subtask_type": "[SUB] Монтажники - ОАП",
        "subtasks_count": 1,
        "open_subtasks_count": 0,
        "closed_subtasks_count": 1,
        "ontime_closed_subtasks_count": 0,
        "sla_plan_days": "0",
        "sla_fact_days": "0"
    },
    {
        "parent_issue_type": "Обращения с сайта: Поддержка",
        "total_count": 49,
        "subtask_type": null,
        "subtasks_count": 49,
        "open_subtasks_count": 0,
        "closed_subtasks_count": 49,
        "ontime_closed_subtasks_count": 15,
        "sla_plan_days": "8.00",
        "sla_fact_days": "5.69"
    },
    {
        "parent_issue_type": "Платный выезд: Не работает проводной интернет",
        "total_count": 35,
        "subtask_type": "[SUB] Монтажники",
        "subtasks_count": 19,
        "open_subtasks_count": 1,
        "closed_subtasks_count": 18,
        "ontime_closed_subtasks_count": 12,
        "sla_plan_days": "8.00",
        "sla_fact_days": "3.58"
    },
    {
        "parent_issue_type": "Платный выезд: Не работает проводной интернет",
        "total_count": 35,
        "subtask_type": "[SUB] Инженеры",
        "subtasks_count": 14,
        "open_subtasks_count": 0,
        "closed_subtasks_count": 14,
        "ontime_closed_subtasks_count": 11,
        "sla_plan_days": "8.00",
        "sla_fact_days": "3.15"
    },
    {
        "parent_issue_type": "Платный выезд: Не работает проводной интернет",
        "total_count": 35,
        "subtask_type": null,
        "subtasks_count": 3,
        "open_subtasks_count": 0,
        "closed_subtasks_count": 3,
        "ontime_closed_subtasks_count": 3,
        "sla_plan_days": "8.00",
        "sla_fact_days": "0.22"
    },
    {
        "parent_issue_type": "Проблема отключения услуг",
        "total_count": 26,
        "subtask_type": null,
        "subtasks_count": 26,
        "open_subtasks_count": 6,
        "closed_subtasks_count": 20,
        "ontime_closed_subtasks_count": 13,
        "sla_plan_days": "8.00",
        "sla_fact_days": "0.22"
    },
    {
        "parent_issue_type": "Проблема подключения услуг",
        "total_count": 108,
        "subtask_type": null,
        "subtasks_count": 104,
        "open_subtasks_count": 2,
        "closed_subtasks_count": 102,
        "ontime_closed_subtasks_count": 84,
        "sla_plan_days": "8.00",
        "sla_fact_days": "0.91"
    },
    {
        "parent_issue_type": "Проблема подключения услуг",
        "total_count": 108,
        "subtask_type": "[SUB] SAIMA: На уточнении",
        "subtasks_count": 4,
        "open_subtasks_count": 0,
        "closed_subtasks_count": 4,
        "ontime_closed_subtasks_count": 3,
        "sla_plan_days": "8.00",
        "sla_fact_days": "2.50"
    },
    {
        "parent_issue_type": "Списалась АП, но ТП не подключился",
        "total_count": 288,
        "subtask_type": null,
        "subtasks_count": 271,
        "open_subtasks_count": 8,
        "closed_subtasks_count": 263,
        "ontime_closed_subtasks_count": 236,
        "sla_plan_days": "8.00",
        "sla_fact_days": "0.75"
    },
    {
        "parent_issue_type": "Списалась АП, но ТП не подключился",
        "total_count": 288,
        "subtask_type": "[SUB] SAIMA: На уточнении",
        "subtasks_count": 22,
        "open_subtasks_count": 1,
        "closed_subtasks_count": 21,
        "ontime_closed_subtasks_count": 13,
        "sla_plan_days": "8.00",
        "sla_fact_days": "2.91"
    },
    {
        "parent_issue_type": "Физически подключили, но сервисы не назначались",
        "total_count": 31,
        "subtask_type": null,
        "subtasks_count": 17,
        "open_subtasks_count": 7,
        "closed_subtasks_count": 10,
        "ontime_closed_subtasks_count": 7,
        "sla_plan_days": "8.00",
        "sla_fact_days": "2.39"
    },
    {
        "parent_issue_type": "Физически подключили, но сервисы не назначались",
        "total_count": 31,
        "subtask_type": "[SUB] SAIMA: На уточнении",
        "subtasks_count": 14,
        "open_subtasks_count": 0,
        "closed_subtasks_count": 14,
        "ontime_closed_subtasks_count": 10,
        "sla_plan_days": "8.00",
        "sla_fact_days": "4.23"
    }
]''')

get_jira_subtask_time_series_ms = [
    {
        "result": "{\"parent_issue_type\" : \"Аварийные работы\", \"subtitles\" : [{\"date\" : null, \"subtask_type\" : null, \"count\" : null}]}"
    },
    {
        "result": "{\"parent_issue_type\" : \"Версия TV-приставки\", \"subtitles\" : [{\"date\" : \"2025-09-22\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 1}, {\"date\" : \"2025-09-11\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 1}, {\"date\" : \"2025-09-05\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 1}, {\"date\" : \"2025-09-04\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 2}, {\"date\" : \"2025-09-02\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 1}]}"
    },
    {
        "result": "{\"parent_issue_type\" : \"ГУАА - Демонтаж оборудования \", \"subtitles\" : [{\"date\" : \"2025-09-16\", \"subtask_type\" : \"Восстановление линии\", \"count\" : 1}, {\"date\" : \"2025-09-11\", \"subtask_type\" : \"Восстановление линии\", \"count\" : 1}, {\"date\" : \"2025-09-03\", \"subtask_type\" : \"Восстановление линии\", \"count\" : 4}, {\"date\" : \"2025-09-02\", \"subtask_type\" : \"Восстановление линии\", \"count\" : 1}]}"
    },
    {
        "result": "{\"parent_issue_type\" : \"ДС есть, но ТП не подключился\", \"subtitles\" : [{\"date\" : \"2025-09-10\", \"subtask_type\" : \"[SUB] SAIMA: Активация сервисов - Backoffice\", \"count\" : 1}, {\"date\" : \"2025-09-03\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 2}, {\"date\" : \"2025-09-02\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 3}, {\"date\" : \"2025-09-01\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 3}]}"
    },
    {
        "result": "{\"parent_issue_type\" : \"Демонтаж в US\", \"subtitles\" : [{\"date\" : \"2025-09-25\", \"subtask_type\" : \"ГУАА - Демонтаж оборудования \", \"count\" : 1}, {\"date\" : \"2025-09-16\", \"subtask_type\" : \"ГУАА - Демонтаж оборудования \", \"count\" : 41}, {\"date\" : \"2025-09-12\", \"subtask_type\" : \"ГУАА - Демонтаж оборудования \", \"count\" : 1}, {\"date\" : \"2025-09-11\", \"subtask_type\" : \"ГУАА - Демонтаж оборудования \", \"count\" : 21}, {\"date\" : \"2025-09-09\", \"subtask_type\" : \"ГУАА - Демонтаж оборудования \", \"count\" : 14}, {\"date\" : \"2025-09-08\", \"subtask_type\" : \"ГУАА - Демонтаж оборудования \", \"count\" : 2}, {\"date\" : \"2025-09-05\", \"subtask_type\" : \"ГУАА - Демонтаж оборудования \", \"count\" : 4}, {\"date\" : \"2025-09-04\", \"subtask_type\" : \"ГУАА - Демонтаж оборудования \", \"count\" : 17}, {\"date\" : \"2025-09-02\", \"subtask_type\" : \"ГУАА - Демонтаж оборудования \", \"count\" : 12}, {\"date\" : \"2025-09-01\", \"subtask_type\" : \"ГУАА - Демонтаж оборудования \", \"count\" : 32}, {\"date\" : \"2025-08-29\", \"subtask_type\" : \"ГУАА - Демонтаж оборудования \", \"count\" : 14}]}"
    },
    {
        "result": "{\"parent_issue_type\" : \"Затруднение в работе IPTV\", \"subtitles\" : [{\"date\" : \"2025-09-27\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 2}, {\"date\" : \"2025-09-13\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 1}, {\"date\" : \"2025-09-12\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 1}, {\"date\" : \"2025-09-11\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 1}]}"
    },
    {
        "result": "{\"parent_issue_type\" : \"Затруднение в работе O!TV\", \"subtitles\" : [{\"date\" : \"2025-09-29\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 47}, {\"date\" : \"2025-09-29\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 3}, {\"date\" : \"2025-09-29\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 3}, {\"date\" : \"2025-09-28\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 56}, {\"date\" : \"2025-09-28\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 2}, {\"date\" : \"2025-09-27\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 66}, {\"date\" : \"2025-09-27\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 3}, {\"date\" : \"2025-09-27\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 1}, {\"date\" : \"2025-09-26\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 58}, {\"date\" : \"2025-09-26\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 4}, {\"date\" : \"2025-09-26\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 3}, {\"date\" : \"2025-09-26\", \"subtask_type\" : \"[SUB] Монтажники - ОАП\", \"count\" : 1}, {\"date\" : \"2025-09-25\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 57}, {\"date\" : \"2025-09-25\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 3}, {\"date\" : \"2025-09-25\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 3}, {\"date\" : \"2025-09-24\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 57}, {\"date\" : \"2025-09-24\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 3}, {\"date\" : \"2025-09-24\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 2}, {\"date\" : \"2025-09-23\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 42}, {\"date\" : \"2025-09-23\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 2}, {\"date\" : \"2025-09-22\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 42}, {\"date\" : \"2025-09-22\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 2}, {\"date\" : \"2025-09-22\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 1}, {\"date\" : \"2025-09-21\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 44}, {\"date\" : \"2025-09-21\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 2}, {\"date\" : \"2025-09-21\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 2}, {\"date\" : \"2025-09-20\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 60}, {\"date\" : \"2025-09-20\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 4}, {\"date\" : \"2025-09-20\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 2}, {\"date\" : \"2025-09-19\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 57}, {\"date\" : \"2025-09-19\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 4}, {\"date\" : \"2025-09-19\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 2}, {\"date\" : \"2025-09-18\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 44}, {\"date\" : \"2025-09-18\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 5}, {\"date\" : \"2025-09-18\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 3}, {\"date\" : \"2025-09-17\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 67}, {\"date\" : \"2025-09-17\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 9}, {\"date\" : \"2025-09-17\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 4}, {\"date\" : \"2025-09-16\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 54}, {\"date\" : \"2025-09-16\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 4}, {\"date\" : \"2025-09-15\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 49}, {\"date\" : \"2025-09-15\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 3}, {\"date\" : \"2025-09-15\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 1}, {\"date\" : \"2025-09-14\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 54}, {\"date\" : \"2025-09-14\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 2}, {\"date\" : \"2025-09-14\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 2}, {\"date\" : \"2025-09-13\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 60}, {\"date\" : \"2025-09-13\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 5}, {\"date\" : \"2025-09-12\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 57}, {\"date\" : \"2025-09-12\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 3}, {\"date\" : \"2025-09-12\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 2}, {\"date\" : \"2025-09-11\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 68}, {\"date\" : \"2025-09-11\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 2}, {\"date\" : \"2025-09-11\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 1}, {\"date\" : \"2025-09-10\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 64}, {\"date\" : \"2025-09-10\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 2}, {\"date\" : \"2025-09-10\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 1}, {\"date\" : \"2025-09-09\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 68}, {\"date\" : \"2025-09-09\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 4}, {\"date\" : \"2025-09-09\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 2}, {\"date\" : \"2025-09-08\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 64}, {\"date\" : \"2025-09-08\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 8}, {\"date\" : \"2025-09-08\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 2}, {\"date\" : \"2025-09-07\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 58}, {\"date\" : \"2025-09-07\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 1}, {\"date\" : \"2025-09-07\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 1}, {\"date\" : \"2025-09-06\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 69}, {\"date\" : \"2025-09-06\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 1}, {\"date\" : \"2025-09-05\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 59}, {\"date\" : \"2025-09-05\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 8}, {\"date\" : \"2025-09-05\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 3}, {\"date\" : \"2025-09-05\", \"subtask_type\" : \"[SUB] Монтажники - ОАП\", \"count\" : 1}, {\"date\" : \"2025-09-04\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 85}, {\"date\" : \"2025-09-04\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 5}, {\"date\" : \"2025-09-03\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 102}, {\"date\" : \"2025-09-03\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 6}, {\"date\" : \"2025-09-03\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 5}, {\"date\" : \"2025-09-02\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 76}, {\"date\" : \"2025-09-02\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 10}, {\"date\" : \"2025-09-02\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 1}, {\"date\" : \"2025-09-01\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 38}, {\"date\" : \"2025-09-01\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 12}, {\"date\" : \"2025-09-01\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 1}, {\"date\" : \"2025-08-31\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 33}, {\"date\" : \"2025-08-31\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 2}, {\"date\" : \"2025-08-30\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 50}, {\"date\" : \"2025-08-30\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 3}, {\"date\" : \"2025-08-30\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 1}, {\"date\" : \"2025-08-29\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 36}, {\"date\" : \"2025-08-29\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 1}]}"
    },
    {
        "result": "{\"parent_issue_type\" : \"Не работает проводной интернет\", \"subtitles\" : [{\"date\" : \"2025-09-29\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 230}, {\"date\" : \"2025-09-29\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 71}, {\"date\" : \"2025-09-29\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 14}, {\"date\" : \"2025-09-28\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 149}, {\"date\" : \"2025-09-28\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 30}, {\"date\" : \"2025-09-28\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 6}, {\"date\" : \"2025-09-27\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 193}, {\"date\" : \"2025-09-27\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 47}, {\"date\" : \"2025-09-27\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 6}, {\"date\" : \"2025-09-26\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 259}, {\"date\" : \"2025-09-26\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 54}, {\"date\" : \"2025-09-26\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 9}, {\"date\" : \"2025-09-26\", \"subtask_type\" : \"[SUB] Монтажники - ОАП\", \"count\" : 1}, {\"date\" : \"2025-09-25\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 205}, {\"date\" : \"2025-09-25\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 51}, {\"date\" : \"2025-09-25\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 5}, {\"date\" : \"2025-09-25\", \"subtask_type\" : \"[SUB] Монтажники - ОАП\", \"count\" : 2}, {\"date\" : \"2025-09-24\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 261}, {\"date\" : \"2025-09-24\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 59}, {\"date\" : \"2025-09-24\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 9}, {\"date\" : \"2025-09-23\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 194}, {\"date\" : \"2025-09-23\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 89}, {\"date\" : \"2025-09-23\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 10}, {\"date\" : \"2025-09-23\", \"subtask_type\" : \"[SUB] Монтажники - ОАП\", \"count\" : 1}, {\"date\" : \"2025-09-22\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 235}, {\"date\" : \"2025-09-22\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 53}, {\"date\" : \"2025-09-22\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 5}, {\"date\" : \"2025-09-21\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 171}, {\"date\" : \"2025-09-21\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 49}, {\"date\" : \"2025-09-21\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 5}, {\"date\" : \"2025-09-20\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 145}, {\"date\" : \"2025-09-20\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 47}, {\"date\" : \"2025-09-20\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 8}, {\"date\" : \"2025-09-19\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 200}, {\"date\" : \"2025-09-19\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 56}, {\"date\" : \"2025-09-19\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 13}, {\"date\" : \"2025-09-19\", \"subtask_type\" : \"[SUB] Монтажники - ОАП\", \"count\" : 2}, {\"date\" : \"2025-09-18\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 204}, {\"date\" : \"2025-09-18\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 62}, {\"date\" : \"2025-09-18\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 17}, {\"date\" : \"2025-09-18\", \"subtask_type\" : \"[SUB] Монтажники - ОАП\", \"count\" : 1}, {\"date\" : \"2025-09-17\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 209}, {\"date\" : \"2025-09-17\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 52}, {\"date\" : \"2025-09-17\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 25}, {\"date\" : \"2025-09-16\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 198}, {\"date\" : \"2025-09-16\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 50}, {\"date\" : \"2025-09-16\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 14}, {\"date\" : \"2025-09-16\", \"subtask_type\" : \"[SUB] Монтажники - ОАП\", \"count\" : 1}, {\"date\" : \"2025-09-15\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 299}, {\"date\" : \"2025-09-15\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 69}, {\"date\" : \"2025-09-15\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 10}, {\"date\" : \"2025-09-14\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 179}, {\"date\" : \"2025-09-14\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 42}, {\"date\" : \"2025-09-14\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 4}, {\"date\" : \"2025-09-14\", \"subtask_type\" : \"[SUB] Монтажники - ОАП\", \"count\" : 1}, {\"date\" : \"2025-09-13\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 182}, {\"date\" : \"2025-09-13\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 42}, {\"date\" : \"2025-09-13\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 4}, {\"date\" : \"2025-09-12\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 201}, {\"date\" : \"2025-09-12\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 61}, {\"date\" : \"2025-09-12\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 3}, {\"date\" : \"2025-09-11\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 215}, {\"date\" : \"2025-09-11\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 61}, {\"date\" : \"2025-09-11\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 9}, {\"date\" : \"2025-09-10\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 195}, {\"date\" : \"2025-09-10\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 73}, {\"date\" : \"2025-09-10\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 13}, {\"date\" : \"2025-09-10\", \"subtask_type\" : \"[SUB] Монтажники - ОАП\", \"count\" : 1}, {\"date\" : \"2025-09-09\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 288}, {\"date\" : \"2025-09-09\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 88}, {\"date\" : \"2025-09-09\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 11}, {\"date\" : \"2025-09-09\", \"subtask_type\" : \"[SUB] Монтажники - ОАП\", \"count\" : 2}, {\"date\" : \"2025-09-08\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 243}, {\"date\" : \"2025-09-08\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 76}, {\"date\" : \"2025-09-08\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 5}, {\"date\" : \"2025-09-08\", \"subtask_type\" : \"Не работает проводной интернет\", \"count\" : 1}, {\"date\" : \"2025-09-07\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 161}, {\"date\" : \"2025-09-07\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 52}, {\"date\" : \"2025-09-07\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 24}, {\"date\" : \"2025-09-07\", \"subtask_type\" : \"[SUB] Монтажники - ОАП\", \"count\" : 1}, {\"date\" : \"2025-09-06\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 206}, {\"date\" : \"2025-09-06\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 63}, {\"date\" : \"2025-09-06\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 4}, {\"date\" : \"2025-09-05\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 217}, {\"date\" : \"2025-09-05\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 83}, {\"date\" : \"2025-09-05\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 12}, {\"date\" : \"2025-09-05\", \"subtask_type\" : \"[SUB] Монтажники - ОАП\", \"count\" : 1}, {\"date\" : \"2025-09-04\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 212}, {\"date\" : \"2025-09-04\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 79}, {\"date\" : \"2025-09-04\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 8}, {\"date\" : \"2025-09-03\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 235}, {\"date\" : \"2025-09-03\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 104}, {\"date\" : \"2025-09-03\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 8}, {\"date\" : \"2025-09-03\", \"subtask_type\" : \"[SUB] Монтажники - ОАП\", \"count\" : 2}, {\"date\" : \"2025-09-02\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 212}, {\"date\" : \"2025-09-02\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 93}, {\"date\" : \"2025-09-02\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 10}, {\"date\" : \"2025-09-02\", \"subtask_type\" : \"Не работает проводной интернет\", \"count\" : 1}, {\"date\" : \"2025-09-01\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 192}, {\"date\" : \"2025-09-01\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 117}, {\"date\" : \"2025-09-01\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 8}, {\"date\" : \"2025-09-01\", \"subtask_type\" : \"Не работает проводной интернет\", \"count\" : 1}, {\"date\" : \"2025-08-31\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 115}, {\"date\" : \"2025-08-31\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 44}, {\"date\" : \"2025-08-31\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 3}, {\"date\" : \"2025-08-30\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 170}, {\"date\" : \"2025-08-30\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 48}, {\"date\" : \"2025-08-30\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 3}, {\"date\" : \"2025-08-30\", \"subtask_type\" : \"[SUB] Монтажники - ОАП\", \"count\" : 1}, {\"date\" : \"2025-08-29\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 215}, {\"date\" : \"2025-08-29\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 70}, {\"date\" : \"2025-08-29\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 4}]}"
    },
    {
        "result": "{\"parent_issue_type\" : \"Не работает телефонная линия\", \"subtitles\" : [{\"date\" : \"2025-09-29\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 1}, {\"date\" : \"2025-09-24\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 1}, {\"date\" : \"2025-09-22\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 1}, {\"date\" : \"2025-09-19\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 1}, {\"date\" : \"2025-09-16\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 1}, {\"date\" : \"2025-09-15\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 1}, {\"date\" : \"2025-09-11\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 1}, {\"date\" : \"2025-09-11\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 1}, {\"date\" : \"2025-09-10\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 1}, {\"date\" : \"2025-09-09\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 1}, {\"date\" : \"2025-09-08\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 1}, {\"date\" : \"2025-09-08\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 1}, {\"date\" : \"2025-09-06\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 1}, {\"date\" : \"2025-08-29\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 1}]}"
    },
    {
        "result": "{\"parent_issue_type\" : \"Нет исходящих вызовов\", \"subtitles\" : [{\"date\" : \"2025-09-22\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 1}, {\"date\" : \"2025-09-08\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 1}]}"
    },
    {
        "result": "{\"parent_issue_type\" : \"Низкая скорость на проводном интернете\", \"subtitles\" : [{\"date\" : \"2025-09-29\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 11}, {\"date\" : \"2025-09-29\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 2}, {\"date\" : \"2025-09-28\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 5}, {\"date\" : \"2025-09-28\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 1}, {\"date\" : \"2025-09-28\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 1}, {\"date\" : \"2025-09-27\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 7}, {\"date\" : \"2025-09-27\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 5}, {\"date\" : \"2025-09-27\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 1}, {\"date\" : \"2025-09-26\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 5}, {\"date\" : \"2025-09-25\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 5}, {\"date\" : \"2025-09-25\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 2}, {\"date\" : \"2025-09-24\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 6}, {\"date\" : \"2025-09-24\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 2}, {\"date\" : \"2025-09-24\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 2}, {\"date\" : \"2025-09-23\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 10}, {\"date\" : \"2025-09-23\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 2}, {\"date\" : \"2025-09-22\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 8}, {\"date\" : \"2025-09-22\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 3}, {\"date\" : \"2025-09-21\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 4}, {\"date\" : \"2025-09-20\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 13}, {\"date\" : \"2025-09-20\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 1}, {\"date\" : \"2025-09-19\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 9}, {\"date\" : \"2025-09-19\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 2}, {\"date\" : \"2025-09-19\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 1}, {\"date\" : \"2025-09-18\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 15}, {\"date\" : \"2025-09-17\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 4}, {\"date\" : \"2025-09-17\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 3}, {\"date\" : \"2025-09-17\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 1}, {\"date\" : \"2025-09-16\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 11}, {\"date\" : \"2025-09-16\", \"subtask_type\" : \"[SUB] Монтажники - ОАП\", \"count\" : 1}, {\"date\" : \"2025-09-16\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 1}, {\"date\" : \"2025-09-16\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 1}, {\"date\" : \"2025-09-15\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 14}, {\"date\" : \"2025-09-15\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 2}, {\"date\" : \"2025-09-14\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 3}, {\"date\" : \"2025-09-14\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 2}, {\"date\" : \"2025-09-14\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 1}, {\"date\" : \"2025-09-13\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 6}, {\"date\" : \"2025-09-13\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 5}, {\"date\" : \"2025-09-12\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 10}, {\"date\" : \"2025-09-12\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 1}, {\"date\" : \"2025-09-11\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 6}, {\"date\" : \"2025-09-11\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 3}, {\"date\" : \"2025-09-11\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 1}, {\"date\" : \"2025-09-10\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 5}, {\"date\" : \"2025-09-10\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 4}, {\"date\" : \"2025-09-09\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 20}, {\"date\" : \"2025-09-09\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 2}, {\"date\" : \"2025-09-09\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 1}, {\"date\" : \"2025-09-08\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 19}, {\"date\" : \"2025-09-08\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 3}, {\"date\" : \"2025-09-08\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 1}, {\"date\" : \"2025-09-07\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 17}, {\"date\" : \"2025-09-06\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 11}, {\"date\" : \"2025-09-05\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 2}, {\"date\" : \"2025-09-05\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 1}, {\"date\" : \"2025-09-04\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 15}, {\"date\" : \"2025-09-03\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 10}, {\"date\" : \"2025-09-03\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 3}, {\"date\" : \"2025-09-03\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 1}, {\"date\" : \"2025-09-02\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 7}, {\"date\" : \"2025-09-02\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 5}, {\"date\" : \"2025-09-01\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 5}, {\"date\" : \"2025-08-31\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 5}, {\"date\" : \"2025-08-30\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 8}, {\"date\" : \"2025-08-30\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 1}, {\"date\" : \"2025-08-29\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 11}]}"
    },
    {
        "result": "{\"parent_issue_type\" : \"Обращения с сайта: Поддержка\", \"subtitles\" : [{\"date\" : null, \"subtask_type\" : null, \"count\" : null}]}"
    },
    {
        "result": "{\"parent_issue_type\" : \"Платный выезд: Не работает проводной интернет\", \"subtitles\" : [{\"date\" : \"2025-09-27\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 2}, {\"date\" : \"2025-09-24\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 1}, {\"date\" : \"2025-09-21\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 1}, {\"date\" : \"2025-09-20\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 1}, {\"date\" : \"2025-09-19\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 2}, {\"date\" : \"2025-09-19\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 1}, {\"date\" : \"2025-09-18\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 1}, {\"date\" : \"2025-09-16\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 1}, {\"date\" : \"2025-09-15\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 2}, {\"date\" : \"2025-09-15\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 1}, {\"date\" : \"2025-09-12\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 1}, {\"date\" : \"2025-09-11\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 2}, {\"date\" : \"2025-09-11\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 1}, {\"date\" : \"2025-09-10\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 1}, {\"date\" : \"2025-09-09\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 1}, {\"date\" : \"2025-09-08\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 1}, {\"date\" : \"2025-09-06\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 1}, {\"date\" : \"2025-09-04\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 2}, {\"date\" : \"2025-09-03\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 1}, {\"date\" : \"2025-09-03\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 1}, {\"date\" : \"2025-09-02\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 2}, {\"date\" : \"2025-08-31\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 2}, {\"date\" : \"2025-08-31\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 1}, {\"date\" : \"2025-08-30\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 1}, {\"date\" : \"2025-08-29\", \"subtask_type\" : \"[SUB] Монтажники\", \"count\" : 2}]}"
    },
    {
        "result": "{\"parent_issue_type\" : \"Проблема отключения услуг\", \"subtitles\" : [{\"date\" : null, \"subtask_type\" : null, \"count\" : null}]}"
    },
    {
        "result": "{\"parent_issue_type\" : \"Проблема подключения услуг\", \"subtitles\" : [{\"date\" : \"2025-09-16\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 1}, {\"date\" : \"2025-09-09\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 1}, {\"date\" : \"2025-09-02\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 2}]}"
    },
    {
        "result": "{\"parent_issue_type\" : \"Списалась АП, но ТП не подключился\", \"subtitles\" : [{\"date\" : \"2025-09-22\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 1}, {\"date\" : \"2025-09-19\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 2}, {\"date\" : \"2025-09-16\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 1}, {\"date\" : \"2025-09-10\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 1}, {\"date\" : \"2025-09-08\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 2}, {\"date\" : \"2025-09-04\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 3}, {\"date\" : \"2025-09-03\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 5}, {\"date\" : \"2025-09-02\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 1}, {\"date\" : \"2025-09-01\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 6}]}"
    },
    {
        "result": "{\"parent_issue_type\" : \"Физически подключили, но сервисы не назначались\", \"subtitles\" : [{\"date\" : \"2025-09-26\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 2}, {\"date\" : \"2025-09-25\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 1}, {\"date\" : \"2025-09-23\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 1}, {\"date\" : \"2025-09-17\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 1}, {\"date\" : \"2025-09-15\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 1}, {\"date\" : \"2025-09-09\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 2}, {\"date\" : \"2025-09-08\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 3}, {\"date\" : \"2025-09-05\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 2}, {\"date\" : \"2025-09-04\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 1}]}"
    }
]

get_jira_subtask_time_series_cs = [
    {
        "result": "{\"parent_issue_type\" : \"Восстановление линии\", \"subtitles\" : [{\"date\" : \"2025-09-29\", \"subtask_type\" : null, \"count\" : 2}, {\"date\" : \"2025-09-27\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-26\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-26\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 1}, {\"date\" : \"2025-09-23\", \"subtask_type\" : null, \"count\" : 3}, {\"date\" : \"2025-09-23\", \"subtask_type\" : \"[SUB] Восстановление линии\", \"count\" : 1}, {\"date\" : \"2025-09-22\", \"subtask_type\" : \"[SUB] Восстановление линии\", \"count\" : 2}, {\"date\" : \"2025-09-22\", \"subtask_type\" : null, \"count\" : 2}, {\"date\" : \"2025-09-21\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-20\", \"subtask_type\" : null, \"count\" : 2}, {\"date\" : \"2025-09-19\", \"subtask_type\" : null, \"count\" : 2}, {\"date\" : \"2025-09-18\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-18\", \"subtask_type\" : \"[SUB] Восстановление линии\", \"count\" : 1}, {\"date\" : \"2025-09-17\", \"subtask_type\" : null, \"count\" : 5}, {\"date\" : \"2025-09-17\", \"subtask_type\" : \"[SUB] Восстановление линии\", \"count\" : 1}, {\"date\" : \"2025-09-16\", \"subtask_type\" : null, \"count\" : 3}, {\"date\" : \"2025-09-15\", \"subtask_type\" : null, \"count\" : 3}, {\"date\" : \"2025-09-14\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-12\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-11\", \"subtask_type\" : \"[SUB] Восстановление линии\", \"count\" : 1}, {\"date\" : \"2025-09-11\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-10\", \"subtask_type\" : null, \"count\" : 2}, {\"date\" : \"2025-09-09\", \"subtask_type\" : null, \"count\" : 3}, {\"date\" : \"2025-09-08\", \"subtask_type\" : \"[SUB] Восстановление линии\", \"count\" : 2}, {\"date\" : \"2025-09-08\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-08\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 1}, {\"date\" : \"2025-09-07\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-06\", \"subtask_type\" : null, \"count\" : 3}, {\"date\" : \"2025-09-05\", \"subtask_type\" : null, \"count\" : 3}, {\"date\" : \"2025-09-05\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 2}, {\"date\" : \"2025-09-04\", \"subtask_type\" : null, \"count\" : 5}, {\"date\" : \"2025-09-04\", \"subtask_type\" : \"[SUB] Восстановление линии\", \"count\" : 1}, {\"date\" : \"2025-09-04\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 1}, {\"date\" : \"2025-09-03\", \"subtask_type\" : null, \"count\" : 7}, {\"date\" : \"2025-09-03\", \"subtask_type\" : \"[SUB] Восстановление линии\", \"count\" : 1}, {\"date\" : \"2025-09-02\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 3}, {\"date\" : \"2025-09-02\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-01\", \"subtask_type\" : null, \"count\" : 50}, {\"date\" : \"2025-08-31\", \"subtask_type\" : null, \"count\" : 2}, {\"date\" : \"2025-08-30\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-08-29\", \"subtask_type\" : null, \"count\" : 4}]}"
    },
    {
        "result": "{\"parent_issue_type\" : \"Добавление адреса\", \"subtitles\" : [{\"date\" : \"2025-09-29\", \"subtask_type\" : null, \"count\" : 9}, {\"date\" : \"2025-09-28\", \"subtask_type\" : null, \"count\" : 8}, {\"date\" : \"2025-09-27\", \"subtask_type\" : null, \"count\" : 12}, {\"date\" : \"2025-09-26\", \"subtask_type\" : null, \"count\" : 19}, {\"date\" : \"2025-09-25\", \"subtask_type\" : null, \"count\" : 14}, {\"date\" : \"2025-09-24\", \"subtask_type\" : null, \"count\" : 12}, {\"date\" : \"2025-09-23\", \"subtask_type\" : null, \"count\" : 21}, {\"date\" : \"2025-09-22\", \"subtask_type\" : null, \"count\" : 24}, {\"date\" : \"2025-09-21\", \"subtask_type\" : null, \"count\" : 6}, {\"date\" : \"2025-09-20\", \"subtask_type\" : null, \"count\" : 13}, {\"date\" : \"2025-09-19\", \"subtask_type\" : null, \"count\" : 18}, {\"date\" : \"2025-09-18\", \"subtask_type\" : null, \"count\" : 10}, {\"date\" : \"2025-09-17\", \"subtask_type\" : null, \"count\" : 23}, {\"date\" : \"2025-09-16\", \"subtask_type\" : null, \"count\" : 14}, {\"date\" : \"2025-09-15\", \"subtask_type\" : null, \"count\" : 20}, {\"date\" : \"2025-09-14\", \"subtask_type\" : null, \"count\" : 7}, {\"date\" : \"2025-09-13\", \"subtask_type\" : null, \"count\" : 13}, {\"date\" : \"2025-09-12\", \"subtask_type\" : null, \"count\" : 10}, {\"date\" : \"2025-09-11\", \"subtask_type\" : null, \"count\" : 9}, {\"date\" : \"2025-09-10\", \"subtask_type\" : null, \"count\" : 11}, {\"date\" : \"2025-09-09\", \"subtask_type\" : null, \"count\" : 10}, {\"date\" : \"2025-09-08\", \"subtask_type\" : null, \"count\" : 16}, {\"date\" : \"2025-09-07\", \"subtask_type\" : null, \"count\" : 9}, {\"date\" : \"2025-09-06\", \"subtask_type\" : null, \"count\" : 7}, {\"date\" : \"2025-09-05\", \"subtask_type\" : null, \"count\" : 19}, {\"date\" : \"2025-09-04\", \"subtask_type\" : null, \"count\" : 12}, {\"date\" : \"2025-09-03\", \"subtask_type\" : null, \"count\" : 9}, {\"date\" : \"2025-09-02\", \"subtask_type\" : null, \"count\" : 15}, {\"date\" : \"2025-09-01\", \"subtask_type\" : null, \"count\" : 14}, {\"date\" : \"2025-08-31\", \"subtask_type\" : null, \"count\" : 9}, {\"date\" : \"2025-08-30\", \"subtask_type\" : null, \"count\" : 22}, {\"date\" : \"2025-08-29\", \"subtask_type\" : null, \"count\" : 18}]}"
    },
    {
        "result": "{\"parent_issue_type\" : \"Добавление проекта в Биллинг\", \"subtitles\" : [{\"date\" : \"2025-09-05\", \"subtask_type\" : null, \"count\" : 1}]}"
    },
    {
        "result": "{\"parent_issue_type\" : \"Изменение адреса\", \"subtitles\" : [{\"date\" : \"2025-09-29\", \"subtask_type\" : null, \"count\" : 6}, {\"date\" : \"2025-09-28\", \"subtask_type\" : null, \"count\" : 8}, {\"date\" : \"2025-09-27\", \"subtask_type\" : null, \"count\" : 5}, {\"date\" : \"2025-09-26\", \"subtask_type\" : null, \"count\" : 15}, {\"date\" : \"2025-09-25\", \"subtask_type\" : null, \"count\" : 18}, {\"date\" : \"2025-09-24\", \"subtask_type\" : null, \"count\" : 19}, {\"date\" : \"2025-09-23\", \"subtask_type\" : null, \"count\" : 15}, {\"date\" : \"2025-09-22\", \"subtask_type\" : null, \"count\" : 14}, {\"date\" : \"2025-09-21\", \"subtask_type\" : null, \"count\" : 6}, {\"date\" : \"2025-09-20\", \"subtask_type\" : null, \"count\" : 4}, {\"date\" : \"2025-09-19\", \"subtask_type\" : null, \"count\" : 8}, {\"date\" : \"2025-09-18\", \"subtask_type\" : null, \"count\" : 4}, {\"date\" : \"2025-09-17\", \"subtask_type\" : null, \"count\" : 18}, {\"date\" : \"2025-09-16\", \"subtask_type\" : null, \"count\" : 9}, {\"date\" : \"2025-09-15\", \"subtask_type\" : null, \"count\" : 13}, {\"date\" : \"2025-09-14\", \"subtask_type\" : null, \"count\" : 8}, {\"date\" : \"2025-09-13\", \"subtask_type\" : null, \"count\" : 3}, {\"date\" : \"2025-09-12\", \"subtask_type\" : null, \"count\" : 13}, {\"date\" : \"2025-09-11\", \"subtask_type\" : null, \"count\" : 16}, {\"date\" : \"2025-09-10\", \"subtask_type\" : null, \"count\" : 15}, {\"date\" : \"2025-09-09\", \"subtask_type\" : null, \"count\" : 10}, {\"date\" : \"2025-09-08\", \"subtask_type\" : null, \"count\" : 18}, {\"date\" : \"2025-09-07\", \"subtask_type\" : null, \"count\" : 11}, {\"date\" : \"2025-09-06\", \"subtask_type\" : null, \"count\" : 5}, {\"date\" : \"2025-09-05\", \"subtask_type\" : null, \"count\" : 12}, {\"date\" : \"2025-09-04\", \"subtask_type\" : null, \"count\" : 21}, {\"date\" : \"2025-09-03\", \"subtask_type\" : null, \"count\" : 25}, {\"date\" : \"2025-09-02\", \"subtask_type\" : null, \"count\" : 14}, {\"date\" : \"2025-09-01\", \"subtask_type\" : null, \"count\" : 10}, {\"date\" : \"2025-08-31\", \"subtask_type\" : null, \"count\" : 4}, {\"date\" : \"2025-08-30\", \"subtask_type\" : null, \"count\" : 9}, {\"date\" : \"2025-08-29\", \"subtask_type\" : null, \"count\" : 13}]}"
    },
    {
        "result": "{\"parent_issue_type\" : \"Исправление данных в договоре\", \"subtitles\" : [{\"date\" : \"2025-09-29\", \"subtask_type\" : null, \"count\" : 13}, {\"date\" : \"2025-09-28\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-27\", \"subtask_type\" : null, \"count\" : 4}, {\"date\" : \"2025-09-26\", \"subtask_type\" : null, \"count\" : 3}, {\"date\" : \"2025-09-25\", \"subtask_type\" : null, \"count\" : 2}, {\"date\" : \"2025-09-24\", \"subtask_type\" : null, \"count\" : 4}, {\"date\" : \"2025-09-23\", \"subtask_type\" : null, \"count\" : 3}, {\"date\" : \"2025-09-22\", \"subtask_type\" : null, \"count\" : 2}, {\"date\" : \"2025-09-21\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-20\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-19\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-18\", \"subtask_type\" : null, \"count\" : 3}, {\"date\" : \"2025-09-17\", \"subtask_type\" : null, \"count\" : 4}, {\"date\" : \"2025-09-16\", \"subtask_type\" : null, \"count\" : 4}, {\"date\" : \"2025-09-15\", \"subtask_type\" : null, \"count\" : 3}, {\"date\" : \"2025-09-14\", \"subtask_type\" : null, \"count\" : 2}, {\"date\" : \"2025-09-13\", \"subtask_type\" : null, \"count\" : 4}, {\"date\" : \"2025-09-12\", \"subtask_type\" : null, \"count\" : 2}, {\"date\" : \"2025-09-11\", \"subtask_type\" : null, \"count\" : 3}, {\"date\" : \"2025-09-10\", \"subtask_type\" : null, \"count\" : 6}, {\"date\" : \"2025-09-09\", \"subtask_type\" : null, \"count\" : 7}, {\"date\" : \"2025-09-08\", \"subtask_type\" : null, \"count\" : 3}, {\"date\" : \"2025-09-07\", \"subtask_type\" : null, \"count\" : 6}, {\"date\" : \"2025-09-06\", \"subtask_type\" : null, \"count\" : 2}, {\"date\" : \"2025-09-05\", \"subtask_type\" : null, \"count\" : 2}, {\"date\" : \"2025-09-04\", \"subtask_type\" : null, \"count\" : 6}, {\"date\" : \"2025-09-03\", \"subtask_type\" : null, \"count\" : 8}, {\"date\" : \"2025-09-02\", \"subtask_type\" : null, \"count\" : 8}, {\"date\" : \"2025-09-01\", \"subtask_type\" : null, \"count\" : 3}, {\"date\" : \"2025-08-31\", \"subtask_type\" : null, \"count\" : 4}, {\"date\" : \"2025-08-30\", \"subtask_type\" : null, \"count\" : 2}, {\"date\" : \"2025-08-29\", \"subtask_type\" : null, \"count\" : 5}]}"
    },
    {
        "result": "{\"parent_issue_type\" : \"На включение xDSL\", \"subtitles\" : [{\"date\" : \"2025-09-26\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-25\", \"subtask_type\" : null, \"count\" : 2}, {\"date\" : \"2025-09-24\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-22\", \"subtask_type\" : null, \"count\" : 2}, {\"date\" : \"2025-09-18\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-17\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-15\", \"subtask_type\" : null, \"count\" : 5}, {\"date\" : \"2025-09-13\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-12\", \"subtask_type\" : null, \"count\" : 2}, {\"date\" : \"2025-09-07\", \"subtask_type\" : null, \"count\" : 2}, {\"date\" : \"2025-09-05\", \"subtask_type\" : null, \"count\" : 2}, {\"date\" : \"2025-09-04\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-03\", \"subtask_type\" : null, \"count\" : 3}]}"
    },
    {
        "result": "{\"parent_issue_type\" : \"Нет покрытия\", \"subtitles\" : [{\"date\" : \"2025-09-29\", \"subtask_type\" : null, \"count\" : 2}, {\"date\" : \"2025-09-28\", \"subtask_type\" : null, \"count\" : 4}, {\"date\" : \"2025-09-27\", \"subtask_type\" : null, \"count\" : 3}, {\"date\" : \"2025-09-26\", \"subtask_type\" : null, \"count\" : 2}, {\"date\" : \"2025-09-25\", \"subtask_type\" : null, \"count\" : 2}, {\"date\" : \"2025-09-24\", \"subtask_type\" : null, \"count\" : 4}, {\"date\" : \"2025-09-23\", \"subtask_type\" : null, \"count\" : 4}, {\"date\" : \"2025-09-22\", \"subtask_type\" : null, \"count\" : 4}, {\"date\" : \"2025-09-21\", \"subtask_type\" : null, \"count\" : 5}, {\"date\" : \"2025-09-20\", \"subtask_type\" : null, \"count\" : 2}, {\"date\" : \"2025-09-19\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-18\", \"subtask_type\" : null, \"count\" : 3}, {\"date\" : \"2025-09-17\", \"subtask_type\" : null, \"count\" : 2}, {\"date\" : \"2025-09-16\", \"subtask_type\" : null, \"count\" : 3}, {\"date\" : \"2025-09-15\", \"subtask_type\" : null, \"count\" : 3}, {\"date\" : \"2025-09-14\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-13\", \"subtask_type\" : null, \"count\" : 3}, {\"date\" : \"2025-09-12\", \"subtask_type\" : null, \"count\" : 5}, {\"date\" : \"2025-09-11\", \"subtask_type\" : null, \"count\" : 6}, {\"date\" : \"2025-09-10\", \"subtask_type\" : null, \"count\" : 4}, {\"date\" : \"2025-09-09\", \"subtask_type\" : null, \"count\" : 6}, {\"date\" : \"2025-09-08\", \"subtask_type\" : null, \"count\" : 2}, {\"date\" : \"2025-09-07\", \"subtask_type\" : null, \"count\" : 2}, {\"date\" : \"2025-09-06\", \"subtask_type\" : null, \"count\" : 3}, {\"date\" : \"2025-09-05\", \"subtask_type\" : null, \"count\" : 6}, {\"date\" : \"2025-09-04\", \"subtask_type\" : null, \"count\" : 6}, {\"date\" : \"2025-09-03\", \"subtask_type\" : null, \"count\" : 3}, {\"date\" : \"2025-09-02\", \"subtask_type\" : null, \"count\" : 5}, {\"date\" : \"2025-09-01\", \"subtask_type\" : null, \"count\" : 3}, {\"date\" : \"2025-08-31\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-08-30\", \"subtask_type\" : null, \"count\" : 5}, {\"date\" : \"2025-08-29\", \"subtask_type\" : null, \"count\" : 6}]}"
    },
    {
        "result": "{\"parent_issue_type\" : \"ОКП: Добавление адреса в справочник\", \"subtitles\" : [{\"date\" : \"2025-09-29\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-25\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-22\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-19\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-18\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-17\", \"subtask_type\" : null, \"count\" : 2}, {\"date\" : \"2025-09-16\", \"subtask_type\" : null, \"count\" : 2}, {\"date\" : \"2025-09-12\", \"subtask_type\" : null, \"count\" : 3}, {\"date\" : \"2025-09-10\", \"subtask_type\" : null, \"count\" : 2}, {\"date\" : \"2025-09-09\", \"subtask_type\" : null, \"count\" : 2}, {\"date\" : \"2025-09-08\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-02\", \"subtask_type\" : null, \"count\" : 1}]}"
    },
    {
        "result": "{\"parent_issue_type\" : \"Обращения по аренде\", \"subtitles\" : [{\"date\" : \"2025-09-19\", \"subtask_type\" : null, \"count\" : 1}]}"
    },
    {
        "result": "{\"parent_issue_type\" : \"Перевод в красную зону\", \"subtitles\" : [{\"date\" : \"2025-09-25\", \"subtask_type\" : null, \"count\" : 1}]}"
    },
    {
        "result": "{\"parent_issue_type\" : \"Перенос кабеля в одном здании\", \"subtitles\" : [{\"date\" : \"2025-09-29\", \"subtask_type\" : null, \"count\" : 5}, {\"date\" : \"2025-09-28\", \"subtask_type\" : null, \"count\" : 3}, {\"date\" : \"2025-09-27\", \"subtask_type\" : null, \"count\" : 5}, {\"date\" : \"2025-09-26\", \"subtask_type\" : null, \"count\" : 2}, {\"date\" : \"2025-09-25\", \"subtask_type\" : null, \"count\" : 3}, {\"date\" : \"2025-09-24\", \"subtask_type\" : null, \"count\" : 5}, {\"date\" : \"2025-09-23\", \"subtask_type\" : null, \"count\" : 4}, {\"date\" : \"2025-09-22\", \"subtask_type\" : null, \"count\" : 10}, {\"date\" : \"2025-09-21\", \"subtask_type\" : null, \"count\" : 3}, {\"date\" : \"2025-09-20\", \"subtask_type\" : null, \"count\" : 3}, {\"date\" : \"2025-09-19\", \"subtask_type\" : null, \"count\" : 5}, {\"date\" : \"2025-09-18\", \"subtask_type\" : null, \"count\" : 3}, {\"date\" : \"2025-09-17\", \"subtask_type\" : null, \"count\" : 10}, {\"date\" : \"2025-09-16\", \"subtask_type\" : null, \"count\" : 6}, {\"date\" : \"2025-09-16\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 1}, {\"date\" : \"2025-09-15\", \"subtask_type\" : null, \"count\" : 4}, {\"date\" : \"2025-09-14\", \"subtask_type\" : null, \"count\" : 3}, {\"date\" : \"2025-09-13\", \"subtask_type\" : null, \"count\" : 2}, {\"date\" : \"2025-09-12\", \"subtask_type\" : null, \"count\" : 3}, {\"date\" : \"2025-09-11\", \"subtask_type\" : null, \"count\" : 4}, {\"date\" : \"2025-09-10\", \"subtask_type\" : null, \"count\" : 2}, {\"date\" : \"2025-09-10\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 1}, {\"date\" : \"2025-09-09\", \"subtask_type\" : null, \"count\" : 3}, {\"date\" : \"2025-09-08\", \"subtask_type\" : null, \"count\" : 4}, {\"date\" : \"2025-09-08\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 1}, {\"date\" : \"2025-09-07\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-06\", \"subtask_type\" : null, \"count\" : 3}, {\"date\" : \"2025-09-05\", \"subtask_type\" : null, \"count\" : 7}, {\"date\" : \"2025-09-04\", \"subtask_type\" : null, \"count\" : 9}, {\"date\" : \"2025-09-03\", \"subtask_type\" : null, \"count\" : 8}, {\"date\" : \"2025-09-02\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-01\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-08-31\", \"subtask_type\" : null, \"count\" : 2}, {\"date\" : \"2025-08-30\", \"subtask_type\" : null, \"count\" : 4}, {\"date\" : \"2025-08-29\", \"subtask_type\" : null, \"count\" : 7}]}"
    },
    {
        "result": "{\"parent_issue_type\" : \"Перенос кабеля на другой адрес\", \"subtitles\" : [{\"date\" : \"2025-09-29\", \"subtask_type\" : null, \"count\" : 19}, {\"date\" : \"2025-09-29\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 13}, {\"date\" : \"2025-09-28\", \"subtask_type\" : null, \"count\" : 10}, {\"date\" : \"2025-09-27\", \"subtask_type\" : null, \"count\" : 9}, {\"date\" : \"2025-09-26\", \"subtask_type\" : null, \"count\" : 10}, {\"date\" : \"2025-09-26\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 3}, {\"date\" : \"2025-09-25\", \"subtask_type\" : null, \"count\" : 13}, {\"date\" : \"2025-09-25\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 2}, {\"date\" : \"2025-09-24\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 18}, {\"date\" : \"2025-09-24\", \"subtask_type\" : null, \"count\" : 17}, {\"date\" : \"2025-09-23\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 14}, {\"date\" : \"2025-09-23\", \"subtask_type\" : null, \"count\" : 12}, {\"date\" : \"2025-09-22\", \"subtask_type\" : null, \"count\" : 19}, {\"date\" : \"2025-09-22\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 3}, {\"date\" : \"2025-09-21\", \"subtask_type\" : null, \"count\" : 5}, {\"date\" : \"2025-09-20\", \"subtask_type\" : null, \"count\" : 8}, {\"date\" : \"2025-09-19\", \"subtask_type\" : null, \"count\" : 11}, {\"date\" : \"2025-09-19\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 8}, {\"date\" : \"2025-09-18\", \"subtask_type\" : null, \"count\" : 11}, {\"date\" : \"2025-09-17\", \"subtask_type\" : null, \"count\" : 10}, {\"date\" : \"2025-09-16\", \"subtask_type\" : null, \"count\" : 14}, {\"date\" : \"2025-09-16\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 4}, {\"date\" : \"2025-09-15\", \"subtask_type\" : null, \"count\" : 16}, {\"date\" : \"2025-09-15\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 9}, {\"date\" : \"2025-09-14\", \"subtask_type\" : null, \"count\" : 8}, {\"date\" : \"2025-09-14\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 2}, {\"date\" : \"2025-09-13\", \"subtask_type\" : null, \"count\" : 6}, {\"date\" : \"2025-09-13\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 4}, {\"date\" : \"2025-09-12\", \"subtask_type\" : null, \"count\" : 11}, {\"date\" : \"2025-09-12\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 4}, {\"date\" : \"2025-09-11\", \"subtask_type\" : null, \"count\" : 7}, {\"date\" : \"2025-09-11\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 6}, {\"date\" : \"2025-09-10\", \"subtask_type\" : null, \"count\" : 15}, {\"date\" : \"2025-09-10\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 4}, {\"date\" : \"2025-09-09\", \"subtask_type\" : null, \"count\" : 13}, {\"date\" : \"2025-09-09\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 6}, {\"date\" : \"2025-09-08\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 13}, {\"date\" : \"2025-09-08\", \"subtask_type\" : null, \"count\" : 11}, {\"date\" : \"2025-09-07\", \"subtask_type\" : null, \"count\" : 8}, {\"date\" : \"2025-09-06\", \"subtask_type\" : null, \"count\" : 10}, {\"date\" : \"2025-09-06\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 1}, {\"date\" : \"2025-09-05\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 12}, {\"date\" : \"2025-09-05\", \"subtask_type\" : null, \"count\" : 9}, {\"date\" : \"2025-09-04\", \"subtask_type\" : null, \"count\" : 12}, {\"date\" : \"2025-09-04\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 9}, {\"date\" : \"2025-09-03\", \"subtask_type\" : null, \"count\" : 15}, {\"date\" : \"2025-09-03\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 5}, {\"date\" : \"2025-09-02\", \"subtask_type\" : null, \"count\" : 15}, {\"date\" : \"2025-09-02\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 3}, {\"date\" : \"2025-09-01\", \"subtask_type\" : null, \"count\" : 18}, {\"date\" : \"2025-08-31\", \"subtask_type\" : null, \"count\" : 9}, {\"date\" : \"2025-08-30\", \"subtask_type\" : null, \"count\" : 12}, {\"date\" : \"2025-08-29\", \"subtask_type\" : null, \"count\" : 21}, {\"date\" : \"2025-08-29\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 2}]}"
    },
    {
        "result": "{\"parent_issue_type\" : \"Перерасчет в биллинге\", \"subtitles\" : [{\"date\" : \"2025-09-24\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-20\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-09\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-08\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-05\", \"subtask_type\" : null, \"count\" : 2}, {\"date\" : \"2025-09-02\", \"subtask_type\" : null, \"count\" : 2}, {\"date\" : \"2025-09-01\", \"subtask_type\" : null, \"count\" : 8}]}"
    },
    {
        "result": "{\"parent_issue_type\" : \"Перерасчёт\", \"subtitles\" : [{\"date\" : \"2025-09-29\", \"subtask_type\" : null, \"count\" : 16}, {\"date\" : \"2025-09-28\", \"subtask_type\" : null, \"count\" : 11}, {\"date\" : \"2025-09-27\", \"subtask_type\" : null, \"count\" : 9}, {\"date\" : \"2025-09-26\", \"subtask_type\" : null, \"count\" : 17}, {\"date\" : \"2025-09-25\", \"subtask_type\" : null, \"count\" : 11}, {\"date\" : \"2025-09-24\", \"subtask_type\" : null, \"count\" : 13}, {\"date\" : \"2025-09-23\", \"subtask_type\" : null, \"count\" : 16}, {\"date\" : \"2025-09-22\", \"subtask_type\" : null, \"count\" : 9}, {\"date\" : \"2025-09-22\", \"subtask_type\" : \"SUB: Перерасчёт\", \"count\" : 1}, {\"date\" : \"2025-09-21\", \"subtask_type\" : null, \"count\" : 9}, {\"date\" : \"2025-09-20\", \"subtask_type\" : null, \"count\" : 14}, {\"date\" : \"2025-09-19\", \"subtask_type\" : null, \"count\" : 11}, {\"date\" : \"2025-09-18\", \"subtask_type\" : null, \"count\" : 17}, {\"date\" : \"2025-09-17\", \"subtask_type\" : null, \"count\" : 11}, {\"date\" : \"2025-09-16\", \"subtask_type\" : null, \"count\" : 19}, {\"date\" : \"2025-09-15\", \"subtask_type\" : null, \"count\" : 13}, {\"date\" : \"2025-09-14\", \"subtask_type\" : null, \"count\" : 16}, {\"date\" : \"2025-09-13\", \"subtask_type\" : null, \"count\" : 11}, {\"date\" : \"2025-09-12\", \"subtask_type\" : null, \"count\" : 8}, {\"date\" : \"2025-09-11\", \"subtask_type\" : null, \"count\" : 11}, {\"date\" : \"2025-09-10\", \"subtask_type\" : null, \"count\" : 13}, {\"date\" : \"2025-09-09\", \"subtask_type\" : null, \"count\" : 21}, {\"date\" : \"2025-09-08\", \"subtask_type\" : null, \"count\" : 39}, {\"date\" : \"2025-09-07\", \"subtask_type\" : null, \"count\" : 17}, {\"date\" : \"2025-09-06\", \"subtask_type\" : null, \"count\" : 18}, {\"date\" : \"2025-09-05\", \"subtask_type\" : null, \"count\" : 8}, {\"date\" : \"2025-09-04\", \"subtask_type\" : null, \"count\" : 13}, {\"date\" : \"2025-09-03\", \"subtask_type\" : null, \"count\" : 54}, {\"date\" : \"2025-09-02\", \"subtask_type\" : null, \"count\" : 45}, {\"date\" : \"2025-09-02\", \"subtask_type\" : \"SUB: Перерасчёт\", \"count\" : 1}, {\"date\" : \"2025-09-01\", \"subtask_type\" : null, \"count\" : 101}, {\"date\" : \"2025-08-31\", \"subtask_type\" : null, \"count\" : 23}, {\"date\" : \"2025-08-30\", \"subtask_type\" : null, \"count\" : 22}, {\"date\" : \"2025-08-29\", \"subtask_type\" : null, \"count\" : 61}]}"
    },
    {
        "result": "{\"parent_issue_type\" : \"Подключение без согласия жильцов\", \"subtitles\" : [{\"date\" : \"2025-09-29\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-04\", \"subtask_type\" : null, \"count\" : 1}]}"
    },
    {
        "result": "{\"parent_issue_type\" : \"Подключение нового абонента\", \"subtitles\" : [{\"date\" : \"2025-09-29\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Монтаж\", \"count\" : 221}, {\"date\" : \"2025-09-29\", \"subtask_type\" : \"[SUB] Новый абонент - ГТМ\", \"count\" : 159}, {\"date\" : \"2025-09-29\", \"subtask_type\" : null, \"count\" : 77}, {\"date\" : \"2025-09-29\", \"subtask_type\" : \"[SUB] Новый абонент: На уточнении\", \"count\" : 4}, {\"date\" : \"2025-09-28\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Монтаж\", \"count\" : 118}, {\"date\" : \"2025-09-28\", \"subtask_type\" : \"[SUB] Новый абонент - ГТМ\", \"count\" : 117}, {\"date\" : \"2025-09-28\", \"subtask_type\" : null, \"count\" : 30}, {\"date\" : \"2025-09-28\", \"subtask_type\" : \"[SUB] Новый абонент: На уточнении\", \"count\" : 9}, {\"date\" : \"2025-09-27\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Монтаж\", \"count\" : 121}, {\"date\" : \"2025-09-27\", \"subtask_type\" : \"[SUB] Новый абонент - ГТМ\", \"count\" : 117}, {\"date\" : \"2025-09-27\", \"subtask_type\" : null, \"count\" : 34}, {\"date\" : \"2025-09-27\", \"subtask_type\" : \"[SUB] Новый абонент: На уточнении\", \"count\" : 6}, {\"date\" : \"2025-09-26\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Монтаж\", \"count\" : 147}, {\"date\" : \"2025-09-26\", \"subtask_type\" : \"[SUB] Новый абонент - ГТМ\", \"count\" : 128}, {\"date\" : \"2025-09-26\", \"subtask_type\" : null, \"count\" : 25}, {\"date\" : \"2025-09-26\", \"subtask_type\" : \"[SUB] Новый абонент: На уточнении\", \"count\" : 7}, {\"date\" : \"2025-09-25\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Монтаж\", \"count\" : 170}, {\"date\" : \"2025-09-25\", \"subtask_type\" : \"[SUB] Новый абонент - ГТМ\", \"count\" : 160}, {\"date\" : \"2025-09-25\", \"subtask_type\" : null, \"count\" : 22}, {\"date\" : \"2025-09-25\", \"subtask_type\" : \"[SUB] Новый абонент: На уточнении\", \"count\" : 9}, {\"date\" : \"2025-09-25\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Просчёт\", \"count\" : 1}, {\"date\" : \"2025-09-24\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Монтаж\", \"count\" : 199}, {\"date\" : \"2025-09-24\", \"subtask_type\" : \"[SUB] Новый абонент - ГТМ\", \"count\" : 119}, {\"date\" : \"2025-09-24\", \"subtask_type\" : null, \"count\" : 24}, {\"date\" : \"2025-09-24\", \"subtask_type\" : \"[SUB] Новый абонент: На уточнении\", \"count\" : 4}, {\"date\" : \"2025-09-24\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Просчёт\", \"count\" : 1}, {\"date\" : \"2025-09-23\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Монтаж\", \"count\" : 172}, {\"date\" : \"2025-09-23\", \"subtask_type\" : \"[SUB] Новый абонент - ГТМ\", \"count\" : 139}, {\"date\" : \"2025-09-23\", \"subtask_type\" : null, \"count\" : 28}, {\"date\" : \"2025-09-23\", \"subtask_type\" : \"[SUB] Новый абонент: На уточнении\", \"count\" : 5}, {\"date\" : \"2025-09-23\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Просчёт\", \"count\" : 1}, {\"date\" : \"2025-09-22\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Монтаж\", \"count\" : 195}, {\"date\" : \"2025-09-22\", \"subtask_type\" : \"[SUB] Новый абонент - ГТМ\", \"count\" : 132}, {\"date\" : \"2025-09-22\", \"subtask_type\" : null, \"count\" : 27}, {\"date\" : \"2025-09-22\", \"subtask_type\" : \"[SUB] Новый абонент: На уточнении\", \"count\" : 11}, {\"date\" : \"2025-09-22\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Просчёт\", \"count\" : 2}, {\"date\" : \"2025-09-21\", \"subtask_type\" : \"[SUB] Новый абонент - ГТМ\", \"count\" : 109}, {\"date\" : \"2025-09-21\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Монтаж\", \"count\" : 100}, {\"date\" : \"2025-09-21\", \"subtask_type\" : null, \"count\" : 15}, {\"date\" : \"2025-09-21\", \"subtask_type\" : \"[SUB] Новый абонент: На уточнении\", \"count\" : 2}, {\"date\" : \"2025-09-20\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Монтаж\", \"count\" : 142}, {\"date\" : \"2025-09-20\", \"subtask_type\" : \"[SUB] Новый абонент - ГТМ\", \"count\" : 119}, {\"date\" : \"2025-09-20\", \"subtask_type\" : null, \"count\" : 16}, {\"date\" : \"2025-09-20\", \"subtask_type\" : \"[SUB] Новый абонент: На уточнении\", \"count\" : 5}, {\"date\" : \"2025-09-19\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Монтаж\", \"count\" : 144}, {\"date\" : \"2025-09-19\", \"subtask_type\" : \"[SUB] Новый абонент - ГТМ\", \"count\" : 116}, {\"date\" : \"2025-09-19\", \"subtask_type\" : null, \"count\" : 11}, {\"date\" : \"2025-09-19\", \"subtask_type\" : \"[SUB] Новый абонент: На уточнении\", \"count\" : 1}, {\"date\" : \"2025-09-19\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Просчёт\", \"count\" : 1}, {\"date\" : \"2025-09-18\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Монтаж\", \"count\" : 165}, {\"date\" : \"2025-09-18\", \"subtask_type\" : \"[SUB] Новый абонент - ГТМ\", \"count\" : 141}, {\"date\" : \"2025-09-18\", \"subtask_type\" : null, \"count\" : 24}, {\"date\" : \"2025-09-18\", \"subtask_type\" : \"[SUB] Новый абонент: На уточнении\", \"count\" : 17}, {\"date\" : \"2025-09-18\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Просчёт\", \"count\" : 1}, {\"date\" : \"2025-09-17\", \"subtask_type\" : \"[SUB] Новый абонент - ГТМ\", \"count\" : 144}, {\"date\" : \"2025-09-17\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Монтаж\", \"count\" : 104}, {\"date\" : \"2025-09-17\", \"subtask_type\" : null, \"count\" : 26}, {\"date\" : \"2025-09-17\", \"subtask_type\" : \"[SUB] Новый абонент: На уточнении\", \"count\" : 3}, {\"date\" : \"2025-09-17\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Просчёт\", \"count\" : 1}, {\"date\" : \"2025-09-16\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Монтаж\", \"count\" : 184}, {\"date\" : \"2025-09-16\", \"subtask_type\" : \"[SUB] Новый абонент - ГТМ\", \"count\" : 148}, {\"date\" : \"2025-09-16\", \"subtask_type\" : null, \"count\" : 22}, {\"date\" : \"2025-09-16\", \"subtask_type\" : \"[SUB] Новый абонент: На уточнении\", \"count\" : 4}, {\"date\" : \"2025-09-16\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Просчёт\", \"count\" : 2}, {\"date\" : \"2025-09-15\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Монтаж\", \"count\" : 183}, {\"date\" : \"2025-09-15\", \"subtask_type\" : \"[SUB] Новый абонент - ГТМ\", \"count\" : 167}, {\"date\" : \"2025-09-15\", \"subtask_type\" : null, \"count\" : 28}, {\"date\" : \"2025-09-15\", \"subtask_type\" : \"[SUB] Новый абонент: На уточнении\", \"count\" : 5}, {\"date\" : \"2025-09-15\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Просчёт\", \"count\" : 3}, {\"date\" : \"2025-09-14\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Монтаж\", \"count\" : 111}, {\"date\" : \"2025-09-14\", \"subtask_type\" : \"[SUB] Новый абонент - ГТМ\", \"count\" : 100}, {\"date\" : \"2025-09-14\", \"subtask_type\" : \"[SUB] Новый абонент: На уточнении\", \"count\" : 5}, {\"date\" : \"2025-09-14\", \"subtask_type\" : null, \"count\" : 5}, {\"date\" : \"2025-09-13\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Монтаж\", \"count\" : 122}, {\"date\" : \"2025-09-13\", \"subtask_type\" : \"[SUB] Новый абонент - ГТМ\", \"count\" : 104}, {\"date\" : \"2025-09-13\", \"subtask_type\" : null, \"count\" : 12}, {\"date\" : \"2025-09-13\", \"subtask_type\" : \"[SUB] Новый абонент: На уточнении\", \"count\" : 9}, {\"date\" : \"2025-09-12\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Монтаж\", \"count\" : 156}, {\"date\" : \"2025-09-12\", \"subtask_type\" : \"[SUB] Новый абонент - ГТМ\", \"count\" : 143}, {\"date\" : \"2025-09-12\", \"subtask_type\" : null, \"count\" : 20}, {\"date\" : \"2025-09-12\", \"subtask_type\" : \"[SUB] Новый абонент: На уточнении\", \"count\" : 7}, {\"date\" : \"2025-09-12\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Просчёт\", \"count\" : 6}, {\"date\" : \"2025-09-11\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Монтаж\", \"count\" : 147}, {\"date\" : \"2025-09-11\", \"subtask_type\" : \"[SUB] Новый абонент - ГТМ\", \"count\" : 132}, {\"date\" : \"2025-09-11\", \"subtask_type\" : null, \"count\" : 12}, {\"date\" : \"2025-09-11\", \"subtask_type\" : \"[SUB] Новый абонент: На уточнении\", \"count\" : 11}, {\"date\" : \"2025-09-11\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Просчёт\", \"count\" : 2}, {\"date\" : \"2025-09-10\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Монтаж\", \"count\" : 196}, {\"date\" : \"2025-09-10\", \"subtask_type\" : \"[SUB] Новый абонент - ГТМ\", \"count\" : 124}, {\"date\" : \"2025-09-10\", \"subtask_type\" : null, \"count\" : 25}, {\"date\" : \"2025-09-10\", \"subtask_type\" : \"[SUB] Новый абонент: На уточнении\", \"count\" : 10}, {\"date\" : \"2025-09-10\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Просчёт\", \"count\" : 1}, {\"date\" : \"2025-09-09\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Монтаж\", \"count\" : 154}, {\"date\" : \"2025-09-09\", \"subtask_type\" : \"[SUB] Новый абонент - ГТМ\", \"count\" : 143}, {\"date\" : \"2025-09-09\", \"subtask_type\" : null, \"count\" : 15}, {\"date\" : \"2025-09-09\", \"subtask_type\" : \"[SUB] Новый абонент: На уточнении\", \"count\" : 3}, {\"date\" : \"2025-09-09\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Просчёт\", \"count\" : 1}, {\"date\" : \"2025-09-08\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Монтаж\", \"count\" : 219}, {\"date\" : \"2025-09-08\", \"subtask_type\" : \"[SUB] Новый абонент - ГТМ\", \"count\" : 185}, {\"date\" : \"2025-09-08\", \"subtask_type\" : null, \"count\" : 20}, {\"date\" : \"2025-09-08\", \"subtask_type\" : \"[SUB] Новый абонент: На уточнении\", \"count\" : 8}, {\"date\" : \"2025-09-08\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Просчёт\", \"count\" : 4}, {\"date\" : \"2025-09-07\", \"subtask_type\" : \"[SUB] Новый абонент - ГТМ\", \"count\" : 114}, {\"date\" : \"2025-09-07\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Монтаж\", \"count\" : 106}, {\"date\" : \"2025-09-07\", \"subtask_type\" : null, \"count\" : 11}, {\"date\" : \"2025-09-07\", \"subtask_type\" : \"[SUB] Новый абонент: На уточнении\", \"count\" : 3}, {\"date\" : \"2025-09-06\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Монтаж\", \"count\" : 160}, {\"date\" : \"2025-09-06\", \"subtask_type\" : \"[SUB] Новый абонент - ГТМ\", \"count\" : 116}, {\"date\" : \"2025-09-06\", \"subtask_type\" : null, \"count\" : 14}, {\"date\" : \"2025-09-06\", \"subtask_type\" : \"[SUB] Новый абонент: На уточнении\", \"count\" : 10}, {\"date\" : \"2025-09-05\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Монтаж\", \"count\" : 177}, {\"date\" : \"2025-09-05\", \"subtask_type\" : \"[SUB] Новый абонент - ГТМ\", \"count\" : 156}, {\"date\" : \"2025-09-05\", \"subtask_type\" : null, \"count\" : 27}, {\"date\" : \"2025-09-05\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Просчёт\", \"count\" : 7}, {\"date\" : \"2025-09-05\", \"subtask_type\" : \"[SUB] Новый абонент: На уточнении\", \"count\" : 2}, {\"date\" : \"2025-09-04\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Монтаж\", \"count\" : 159}, {\"date\" : \"2025-09-04\", \"subtask_type\" : \"[SUB] Новый абонент - ГТМ\", \"count\" : 153}, {\"date\" : \"2025-09-04\", \"subtask_type\" : null, \"count\" : 26}, {\"date\" : \"2025-09-04\", \"subtask_type\" : \"[SUB] Новый абонент: На уточнении\", \"count\" : 5}, {\"date\" : \"2025-09-04\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Просчёт\", \"count\" : 2}, {\"date\" : \"2025-09-03\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Монтаж\", \"count\" : 179}, {\"date\" : \"2025-09-03\", \"subtask_type\" : \"[SUB] Новый абонент - ГТМ\", \"count\" : 167}, {\"date\" : \"2025-09-03\", \"subtask_type\" : null, \"count\" : 43}, {\"date\" : \"2025-09-03\", \"subtask_type\" : \"[SUB] Новый абонент: На уточнении\", \"count\" : 14}, {\"date\" : \"2025-09-03\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Просчёт\", \"count\" : 1}, {\"date\" : \"2025-09-02\", \"subtask_type\" : \"[SUB] Новый абонент - ГТМ\", \"count\" : 211}, {\"date\" : \"2025-09-02\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Монтаж\", \"count\" : 209}, {\"date\" : \"2025-09-02\", \"subtask_type\" : null, \"count\" : 29}, {\"date\" : \"2025-09-02\", \"subtask_type\" : \"[SUB] Новый абонент: На уточнении\", \"count\" : 6}, {\"date\" : \"2025-09-02\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Просчёт\", \"count\" : 2}, {\"date\" : \"2025-09-01\", \"subtask_type\" : \"[SUB] Новый абонент - ГТМ\", \"count\" : 344}, {\"date\" : \"2025-09-01\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Монтаж\", \"count\" : 136}, {\"date\" : \"2025-09-01\", \"subtask_type\" : null, \"count\" : 24}, {\"date\" : \"2025-09-01\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Просчёт\", \"count\" : 4}, {\"date\" : \"2025-09-01\", \"subtask_type\" : \"[SUB] Новый абонент: На уточнении\", \"count\" : 2}, {\"date\" : \"2025-08-31\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Монтаж\", \"count\" : 166}, {\"date\" : \"2025-08-31\", \"subtask_type\" : \"[SUB] Новый абонент - ГТМ\", \"count\" : 100}, {\"date\" : \"2025-08-31\", \"subtask_type\" : null, \"count\" : 17}, {\"date\" : \"2025-08-31\", \"subtask_type\" : \"[SUB] Новый абонент: На уточнении\", \"count\" : 1}, {\"date\" : \"2025-08-30\", \"subtask_type\" : \"[SUB] Новый абонент - ГТМ\", \"count\" : 137}, {\"date\" : \"2025-08-30\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Монтаж\", \"count\" : 73}, {\"date\" : \"2025-08-30\", \"subtask_type\" : null, \"count\" : 29}, {\"date\" : \"2025-08-29\", \"subtask_type\" : \"[SUB] Новый абонент - ГТМ\", \"count\" : 135}, {\"date\" : \"2025-08-29\", \"subtask_type\" : \"[SUB] Новый абонент - ОАП: Монтаж\", \"count\" : 134}, {\"date\" : \"2025-08-29\", \"subtask_type\" : null, \"count\" : 26}, {\"date\" : \"2025-08-29\", \"subtask_type\" : \"[SUB] Новый абонент: На уточнении\", \"count\" : 5}]}"
    },
    {
        "result": "{\"parent_issue_type\" : \"Подключение нового корпоративного абонента\", \"subtitles\" : [{\"date\" : \"2025-09-29\", \"subtask_type\" : null, \"count\" : 13}, {\"date\" : \"2025-09-28\", \"subtask_type\" : null, \"count\" : 7}, {\"date\" : \"2025-09-27\", \"subtask_type\" : null, \"count\" : 4}, {\"date\" : \"2025-09-26\", \"subtask_type\" : null, \"count\" : 12}, {\"date\" : \"2025-09-25\", \"subtask_type\" : null, \"count\" : 12}, {\"date\" : \"2025-09-24\", \"subtask_type\" : null, \"count\" : 15}, {\"date\" : \"2025-09-23\", \"subtask_type\" : null, \"count\" : 22}, {\"date\" : \"2025-09-22\", \"subtask_type\" : null, \"count\" : 13}, {\"date\" : \"2025-09-21\", \"subtask_type\" : null, \"count\" : 7}, {\"date\" : \"2025-09-20\", \"subtask_type\" : null, \"count\" : 10}, {\"date\" : \"2025-09-19\", \"subtask_type\" : null, \"count\" : 14}, {\"date\" : \"2025-09-18\", \"subtask_type\" : null, \"count\" : 14}, {\"date\" : \"2025-09-17\", \"subtask_type\" : null, \"count\" : 14}, {\"date\" : \"2025-09-16\", \"subtask_type\" : null, \"count\" : 14}, {\"date\" : \"2025-09-15\", \"subtask_type\" : null, \"count\" : 10}, {\"date\" : \"2025-09-14\", \"subtask_type\" : null, \"count\" : 5}, {\"date\" : \"2025-09-13\", \"subtask_type\" : null, \"count\" : 8}, {\"date\" : \"2025-09-12\", \"subtask_type\" : null, \"count\" : 19}, {\"date\" : \"2025-09-11\", \"subtask_type\" : null, \"count\" : 14}, {\"date\" : \"2025-09-10\", \"subtask_type\" : null, \"count\" : 23}, {\"date\" : \"2025-09-09\", \"subtask_type\" : null, \"count\" : 22}, {\"date\" : \"2025-09-08\", \"subtask_type\" : null, \"count\" : 8}, {\"date\" : \"2025-09-07\", \"subtask_type\" : null, \"count\" : 15}, {\"date\" : \"2025-09-06\", \"subtask_type\" : null, \"count\" : 5}, {\"date\" : \"2025-09-05\", \"subtask_type\" : null, \"count\" : 15}, {\"date\" : \"2025-09-04\", \"subtask_type\" : null, \"count\" : 20}, {\"date\" : \"2025-09-03\", \"subtask_type\" : null, \"count\" : 10}, {\"date\" : \"2025-09-02\", \"subtask_type\" : null, \"count\" : 16}, {\"date\" : \"2025-09-01\", \"subtask_type\" : null, \"count\" : 6}, {\"date\" : \"2025-08-31\", \"subtask_type\" : null, \"count\" : 7}, {\"date\" : \"2025-08-30\", \"subtask_type\" : null, \"count\" : 9}, {\"date\" : \"2025-08-29\", \"subtask_type\" : null, \"count\" : 12}]}"
    },
    {
        "result": "{\"parent_issue_type\" : \"Смена ТП 300/500\", \"subtitles\" : [{\"date\" : \"2025-09-29\", \"subtask_type\" : null, \"count\" : 18}, {\"date\" : \"2025-09-29\", \"subtask_type\" : \"[SUB] Замена оборудования\", \"count\" : 5}, {\"date\" : \"2025-09-28\", \"subtask_type\" : null, \"count\" : 7}, {\"date\" : \"2025-09-28\", \"subtask_type\" : \"[SUB] Замена оборудования\", \"count\" : 1}, {\"date\" : \"2025-09-27\", \"subtask_type\" : null, \"count\" : 10}, {\"date\" : \"2025-09-27\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 1}, {\"date\" : \"2025-09-27\", \"subtask_type\" : \"[SUB] Замена оборудования\", \"count\" : 1}, {\"date\" : \"2025-09-26\", \"subtask_type\" : null, \"count\" : 10}, {\"date\" : \"2025-09-25\", \"subtask_type\" : null, \"count\" : 7}, {\"date\" : \"2025-09-24\", \"subtask_type\" : null, \"count\" : 10}, {\"date\" : \"2025-09-24\", \"subtask_type\" : \"[SUB] Замена оборудования\", \"count\" : 2}, {\"date\" : \"2025-09-23\", \"subtask_type\" : null, \"count\" : 5}, {\"date\" : \"2025-09-23\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 1}, {\"date\" : \"2025-09-23\", \"subtask_type\" : \"[SUB] Замена оборудования\", \"count\" : 1}, {\"date\" : \"2025-09-22\", \"subtask_type\" : null, \"count\" : 5}, {\"date\" : \"2025-09-21\", \"subtask_type\" : null, \"count\" : 2}, {\"date\" : \"2025-09-20\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-19\", \"subtask_type\" : null, \"count\" : 5}, {\"date\" : \"2025-09-19\", \"subtask_type\" : \"[SUB] Замена оборудования\", \"count\" : 1}, {\"date\" : \"2025-09-18\", \"subtask_type\" : null, \"count\" : 3}, {\"date\" : \"2025-09-18\", \"subtask_type\" : \"[SUB] Замена оборудования\", \"count\" : 2}, {\"date\" : \"2025-09-17\", \"subtask_type\" : null, \"count\" : 4}, {\"date\" : \"2025-09-17\", \"subtask_type\" : \"[SUB] Замена оборудования\", \"count\" : 2}, {\"date\" : \"2025-09-16\", \"subtask_type\" : null, \"count\" : 6}, {\"date\" : \"2025-09-16\", \"subtask_type\" : \"[SUB] Замена оборудования\", \"count\" : 2}, {\"date\" : \"2025-09-14\", \"subtask_type\" : null, \"count\" : 4}, {\"date\" : \"2025-09-14\", \"subtask_type\" : \"[SUB] Замена оборудования\", \"count\" : 2}, {\"date\" : \"2025-09-13\", \"subtask_type\" : null, \"count\" : 3}, {\"date\" : \"2025-09-12\", \"subtask_type\" : null, \"count\" : 3}, {\"date\" : \"2025-09-12\", \"subtask_type\" : \"[SUB] Замена оборудования\", \"count\" : 3}, {\"date\" : \"2025-09-11\", \"subtask_type\" : null, \"count\" : 3}, {\"date\" : \"2025-09-11\", \"subtask_type\" : \"[SUB] Замена оборудования\", \"count\" : 1}, {\"date\" : \"2025-09-10\", \"subtask_type\" : null, \"count\" : 8}, {\"date\" : \"2025-09-10\", \"subtask_type\" : \"[SUB] Замена оборудования\", \"count\" : 4}, {\"date\" : \"2025-09-09\", \"subtask_type\" : null, \"count\" : 5}, {\"date\" : \"2025-09-09\", \"subtask_type\" : \"[SUB] Замена оборудования\", \"count\" : 3}, {\"date\" : \"2025-09-08\", \"subtask_type\" : null, \"count\" : 2}, {\"date\" : \"2025-09-08\", \"subtask_type\" : \"[SUB] Замена оборудования\", \"count\" : 1}, {\"date\" : \"2025-09-07\", \"subtask_type\" : null, \"count\" : 3}, {\"date\" : \"2025-09-07\", \"subtask_type\" : \"[SUB] Замена оборудования\", \"count\" : 2}, {\"date\" : \"2025-09-06\", \"subtask_type\" : null, \"count\" : 2}, {\"date\" : \"2025-09-06\", \"subtask_type\" : \"[SUB] Замена оборудования\", \"count\" : 1}, {\"date\" : \"2025-09-05\", \"subtask_type\" : null, \"count\" : 6}, {\"date\" : \"2025-09-05\", \"subtask_type\" : \"[SUB] Замена оборудования\", \"count\" : 3}, {\"date\" : \"2025-09-04\", \"subtask_type\" : null, \"count\" : 3}, {\"date\" : \"2025-09-04\", \"subtask_type\" : \"[SUB] Замена оборудования\", \"count\" : 2}, {\"date\" : \"2025-09-03\", \"subtask_type\" : \"[SUB] Замена оборудования\", \"count\" : 1}, {\"date\" : \"2025-09-03\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-02\", \"subtask_type\" : null, \"count\" : 7}, {\"date\" : \"2025-09-02\", \"subtask_type\" : \"[SUB] Замена оборудования\", \"count\" : 1}, {\"date\" : \"2025-09-01\", \"subtask_type\" : null, \"count\" : 8}, {\"date\" : \"2025-09-01\", \"subtask_type\" : \"[SUB] Замена оборудования\", \"count\" : 6}, {\"date\" : \"2025-08-31\", \"subtask_type\" : null, \"count\" : 5}, {\"date\" : \"2025-08-31\", \"subtask_type\" : \"[SUB] Замена оборудования\", \"count\" : 2}, {\"date\" : \"2025-08-30\", \"subtask_type\" : null, \"count\" : 5}, {\"date\" : \"2025-08-29\", \"subtask_type\" : null, \"count\" : 2}, {\"date\" : \"2025-08-29\", \"subtask_type\" : \"[SUB] Замена оборудования\", \"count\" : 1}]}"
    },
    {
        "result": "{\"parent_issue_type\" : \"Смена технологии\", \"subtitles\" : [{\"date\" : \"2025-09-24\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-06\", \"subtask_type\" : null, \"count\" : 1}]}"
    },
    {
        "result": "{\"parent_issue_type\" : \"Согласование особых условий\", \"subtitles\" : [{\"date\" : \"2025-09-28\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-15\", \"subtask_type\" : null, \"count\" : 1}]}"
    },
    {
        "result": "{\"parent_issue_type\" : \"ТП Сотрудник 550 + О!TV от Сайма Телеком\", \"subtitles\" : [{\"date\" : \"2025-09-29\", \"subtask_type\" : null, \"count\" : 4}, {\"date\" : \"2025-09-26\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-24\", \"subtask_type\" : null, \"count\" : 3}, {\"date\" : \"2025-09-23\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-22\", \"subtask_type\" : null, \"count\" : 3}, {\"date\" : \"2025-09-19\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-18\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-17\", \"subtask_type\" : null, \"count\" : 2}, {\"date\" : \"2025-09-15\", \"subtask_type\" : null, \"count\" : 2}, {\"date\" : \"2025-09-10\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-09\", \"subtask_type\" : null, \"count\" : 2}, {\"date\" : \"2025-09-07\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-05\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-04\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-03\", \"subtask_type\" : null, \"count\" : 2}, {\"date\" : \"2025-09-01\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-08-29\", \"subtask_type\" : null, \"count\" : 2}]}"
    },
    {
        "result": "{\"parent_issue_type\" : \"Тех лист: На исправление\", \"subtitles\" : [{\"date\" : \"2025-09-29\", \"subtask_type\" : null, \"count\" : 17}, {\"date\" : \"2025-09-28\", \"subtask_type\" : null, \"count\" : 12}, {\"date\" : \"2025-09-27\", \"subtask_type\" : null, \"count\" : 14}, {\"date\" : \"2025-09-26\", \"subtask_type\" : null, \"count\" : 13}, {\"date\" : \"2025-09-25\", \"subtask_type\" : null, \"count\" : 32}, {\"date\" : \"2025-09-24\", \"subtask_type\" : null, \"count\" : 29}, {\"date\" : \"2025-09-23\", \"subtask_type\" : null, \"count\" : 19}, {\"date\" : \"2025-09-22\", \"subtask_type\" : null, \"count\" : 19}, {\"date\" : \"2025-09-21\", \"subtask_type\" : null, \"count\" : 11}, {\"date\" : \"2025-09-20\", \"subtask_type\" : null, \"count\" : 10}, {\"date\" : \"2025-09-19\", \"subtask_type\" : null, \"count\" : 8}, {\"date\" : \"2025-09-18\", \"subtask_type\" : null, \"count\" : 17}, {\"date\" : \"2025-09-17\", \"subtask_type\" : null, \"count\" : 22}, {\"date\" : \"2025-09-16\", \"subtask_type\" : null, \"count\" : 20}, {\"date\" : \"2025-09-15\", \"subtask_type\" : null, \"count\" : 13}, {\"date\" : \"2025-09-14\", \"subtask_type\" : null, \"count\" : 4}, {\"date\" : \"2025-09-13\", \"subtask_type\" : null, \"count\" : 12}, {\"date\" : \"2025-09-12\", \"subtask_type\" : null, \"count\" : 15}, {\"date\" : \"2025-09-11\", \"subtask_type\" : null, \"count\" : 24}, {\"date\" : \"2025-09-10\", \"subtask_type\" : null, \"count\" : 17}, {\"date\" : \"2025-09-09\", \"subtask_type\" : null, \"count\" : 14}, {\"date\" : \"2025-09-08\", \"subtask_type\" : null, \"count\" : 13}, {\"date\" : \"2025-09-07\", \"subtask_type\" : null, \"count\" : 10}, {\"date\" : \"2025-09-06\", \"subtask_type\" : null, \"count\" : 4}, {\"date\" : \"2025-09-05\", \"subtask_type\" : null, \"count\" : 18}, {\"date\" : \"2025-09-04\", \"subtask_type\" : null, \"count\" : 18}, {\"date\" : \"2025-09-03\", \"subtask_type\" : null, \"count\" : 15}, {\"date\" : \"2025-09-02\", \"subtask_type\" : null, \"count\" : 10}, {\"date\" : \"2025-09-01\", \"subtask_type\" : null, \"count\" : 18}, {\"date\" : \"2025-08-31\", \"subtask_type\" : null, \"count\" : 10}, {\"date\" : \"2025-08-30\", \"subtask_type\" : null, \"count\" : 7}, {\"date\" : \"2025-08-29\", \"subtask_type\" : null, \"count\" : 31}]}"
    },
    {
        "result": "{\"parent_issue_type\" : \"Установка дополнительной O!TV приставки (выкуп)\", \"subtitles\" : [{\"date\" : \"2025-09-28\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-27\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-26\", \"subtask_type\" : null, \"count\" : 1}, {\"date\" : \"2025-09-23\", \"subtask_type\" : \"[SUB] Процедуры\", \"count\" : 3}, {\"date\" : \"2025-09-22\", \"subtask_type\" : \"[SUB] Процедуры\", \"count\" : 2}, {\"date\" : \"2025-09-19\", \"subtask_type\" : \"[SUB] Процедуры\", \"count\" : 1}, {\"date\" : \"2025-09-18\", \"subtask_type\" : \"[SUB] Процедуры\", \"count\" : 2}, {\"date\" : \"2025-09-17\", \"subtask_type\" : \"[SUB] Процедуры\", \"count\" : 1}, {\"date\" : \"2025-09-15\", \"subtask_type\" : \"[SUB] Процедуры\", \"count\" : 3}, {\"date\" : \"2025-09-12\", \"subtask_type\" : \"[SUB] Процедуры\", \"count\" : 3}, {\"date\" : \"2025-09-10\", \"subtask_type\" : \"[SUB] Процедуры\", \"count\" : 1}, {\"date\" : \"2025-09-09\", \"subtask_type\" : \"[SUB] Процедуры\", \"count\" : 3}, {\"date\" : \"2025-09-08\", \"subtask_type\" : \"[SUB] Процедуры\", \"count\" : 1}, {\"date\" : \"2025-09-05\", \"subtask_type\" : \"[SUB] Процедуры\", \"count\" : 1}, {\"date\" : \"2025-09-04\", \"subtask_type\" : \"[SUB] Процедуры\", \"count\" : 1}, {\"date\" : \"2025-09-03\", \"subtask_type\" : \"[SUB] Процедуры\", \"count\" : 1}, {\"date\" : \"2025-09-01\", \"subtask_type\" : \"[SUB] Процедуры\", \"count\" : 2}]}"
    },
    {
        "result": "{\"parent_issue_type\" : \"Установка роутера (выкуп)\", \"subtitles\" : [{\"date\" : \"2025-09-29\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 2}, {\"date\" : \"2025-09-27\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 1}, {\"date\" : \"2025-09-26\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 2}, {\"date\" : \"2025-09-25\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 1}, {\"date\" : \"2025-09-24\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 1}, {\"date\" : \"2025-09-23\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 3}, {\"date\" : \"2025-09-22\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 1}, {\"date\" : \"2025-09-21\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 3}, {\"date\" : \"2025-09-20\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 1}, {\"date\" : \"2025-09-19\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 3}, {\"date\" : \"2025-09-18\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 4}, {\"date\" : \"2025-09-17\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 1}, {\"date\" : \"2025-09-16\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 1}, {\"date\" : \"2025-09-15\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 1}, {\"date\" : \"2025-09-14\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 3}, {\"date\" : \"2025-09-13\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 1}, {\"date\" : \"2025-09-12\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 4}, {\"date\" : \"2025-09-11\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 3}, {\"date\" : \"2025-09-10\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 1}, {\"date\" : \"2025-09-09\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 4}, {\"date\" : \"2025-09-09\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 2}, {\"date\" : \"2025-09-08\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 3}, {\"date\" : \"2025-09-07\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 2}, {\"date\" : \"2025-09-07\", \"subtask_type\" : \"[SUB] SAIMA: На уточнении\", \"count\" : 1}, {\"date\" : \"2025-09-06\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 1}, {\"date\" : \"2025-09-05\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 3}, {\"date\" : \"2025-09-04\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 1}, {\"date\" : \"2025-09-03\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 4}, {\"date\" : \"2025-09-02\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 1}, {\"date\" : \"2025-09-01\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 3}, {\"date\" : \"2025-08-31\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 1}, {\"date\" : \"2025-08-30\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 1}, {\"date\" : \"2025-08-29\", \"subtask_type\" : \"[SUB] Инженеры\", \"count\" : 3}]}"
    }
]

get_jira_issue_counts_by_date_and_type_ma = [
    {
        "issue_date": "2025-08-29",
        "parent_issue_type": "Массовая проблема - Не работает Интернет",
        "issues_count": 126
    },
    {
        "issue_date": "2025-08-30",
        "parent_issue_type": "Массовая проблема - Не работает Интернет",
        "issues_count": 225
    },
    {
        "issue_date": "2025-08-31",
        "parent_issue_type": "Массовая проблема - O!TV",
        "issues_count": 1
    },
    {
        "issue_date": "2025-08-31",
        "parent_issue_type": "Массовая проблема - Не работает Интернет",
        "issues_count": 40
    },
    {
        "issue_date": "2025-09-01",
        "parent_issue_type": "Массовая проблема - O!TV",
        "issues_count": 1
    },
    {
        "issue_date": "2025-09-01",
        "parent_issue_type": "Массовая проблема - Не работает Интернет",
        "issues_count": 547
    },
    {
        "issue_date": "2025-09-02",
        "parent_issue_type": "Массовая проблема - O!TV",
        "issues_count": 1
    },
    {
        "issue_date": "2025-09-02",
        "parent_issue_type": "Массовая проблема - Не работает Интернет",
        "issues_count": 130
    },
    {
        "issue_date": "2025-09-03",
        "parent_issue_type": "Массовая проблема - Не работает Интернет",
        "issues_count": 53
    },
    {
        "issue_date": "2025-09-04",
        "parent_issue_type": "Массовая проблема - Не работает Интернет",
        "issues_count": 104
    },
    {
        "issue_date": "2025-09-05",
        "parent_issue_type": "Массовая проблема - Не работает Интернет",
        "issues_count": 103
    },
    {
        "issue_date": "2025-09-06",
        "parent_issue_type": "Массовая проблема - Не работает Интернет",
        "issues_count": 412
    },
    {
        "issue_date": "2025-09-07",
        "parent_issue_type": "Массовая проблема - O!TV",
        "issues_count": 1
    },
    {
        "issue_date": "2025-09-07",
        "parent_issue_type": "Массовая проблема - Не работает Интернет",
        "issues_count": 677
    },
    {
        "issue_date": "2025-09-08",
        "parent_issue_type": "Массовая проблема - Не работает Интернет",
        "issues_count": 388
    },
    {
        "issue_date": "2025-09-09",
        "parent_issue_type": "Массовая проблема - O!TV",
        "issues_count": 1
    },
    {
        "issue_date": "2025-09-09",
        "parent_issue_type": "Массовая проблема - Не работает Интернет",
        "issues_count": 86
    },
    {
        "issue_date": "2025-09-10",
        "parent_issue_type": "Массовая проблема - Не работает Интернет",
        "issues_count": 36
    },
    {
        "issue_date": "2025-09-11",
        "parent_issue_type": "Массовая проблема - Не работает Интернет",
        "issues_count": 44
    },
    {
        "issue_date": "2025-09-12",
        "parent_issue_type": "Массовая проблема - Не работает Интернет",
        "issues_count": 18
    },
    {
        "issue_date": "2025-09-13",
        "parent_issue_type": "Массовая проблема - Не работает Интернет",
        "issues_count": 24
    },
    {
        "issue_date": "2025-09-14",
        "parent_issue_type": "Массовая проблема - Не работает Интернет",
        "issues_count": 667
    },
    {
        "issue_date": "2025-09-15",
        "parent_issue_type": "Массовая проблема - Не работает Интернет",
        "issues_count": 324
    },
    {
        "issue_date": "2025-09-16",
        "parent_issue_type": "Массовая проблема - Не работает Интернет",
        "issues_count": 331
    },
    {
        "issue_date": "2025-09-17",
        "parent_issue_type": "Массовая проблема - Не работает Интернет",
        "issues_count": 155
    },
    {
        "issue_date": "2025-09-18",
        "parent_issue_type": "Массовая проблема - Не работает Интернет",
        "issues_count": 357
    },
    {
        "issue_date": "2025-09-18",
        "parent_issue_type": "Массовая проблема - Низкая скорость Интернета",
        "issues_count": 1
    },
    {
        "issue_date": "2025-09-19",
        "parent_issue_type": "Массовая проблема - O!TV",
        "issues_count": 1
    },
    {
        "issue_date": "2025-09-19",
        "parent_issue_type": "Массовая проблема - Не работает Интернет",
        "issues_count": 361
    },
    {
        "issue_date": "2025-09-20",
        "parent_issue_type": "Массовая проблема - Не работает Интернет",
        "issues_count": 198
    },
    {
        "issue_date": "2025-09-21",
        "parent_issue_type": "Массовая проблема - Не работает Интернет",
        "issues_count": 194
    },
    {
        "issue_date": "2025-09-22",
        "parent_issue_type": "Массовая проблема - Не работает Интернет",
        "issues_count": 24
    },
    {
        "issue_date": "2025-09-23",
        "parent_issue_type": "Массовая проблема - Не работает Интернет",
        "issues_count": 80
    },
    {
        "issue_date": "2025-09-24",
        "parent_issue_type": "Массовая проблема - O!TV",
        "issues_count": 1
    },
    {
        "issue_date": "2025-09-24",
        "parent_issue_type": "Массовая проблема - Не работает Интернет",
        "issues_count": 306
    },
    {
        "issue_date": "2025-09-25",
        "parent_issue_type": "Массовая проблема - Не работает Интернет",
        "issues_count": 544
    },
    {
        "issue_date": "2025-09-26",
        "parent_issue_type": "Массовая проблема - Не работает Интернет",
        "issues_count": 256
    },
    {
        "issue_date": "2025-09-27",
        "parent_issue_type": "Массовая проблема - Не работает Интернет",
        "issues_count": 75
    },
    {
        "issue_date": "2025-09-28",
        "parent_issue_type": "Массовая проблема - Не работает Интернет",
        "issues_count": 51
    },
    {
        "issue_date": "2025-09-29",
        "parent_issue_type": "Массовая проблема - O!TV",
        "issues_count": 6
    },
    {
        "issue_date": "2025-09-29",
        "parent_issue_type": "Массовая проблема - Не работает Интернет",
        "issues_count": 235
    },
    {
        "issue_date": "2025-09-29",
        "parent_issue_type": "Массовая проблема - Низкая скорость Интернета",
        "issues_count": 8
    }
]

get_open_issues_subtasks_by_region_month_ms = json.loads(r'''[
    {
        "title": "MAIN SERVICES Версия TV-приставки",
        "область": null,
        "month": "2025-09-01",
        "subtasks": "[{\"count\": 4, \"month\": \"2025-09-01\", \"status\": \"Open\", \"subtask_title\": \"MAIN SERVICES [SUB] SAIMA: На уточнении\"}]"
    },
    {
        "title": "MAIN SERVICES Демонтаж в US",
        "область": "Джалал-Абадская область",
        "month": "2025-09-01",
        "subtasks": "[{\"count\": 69, \"month\": \"2025-09-01\", \"status\": \"Бухгалтерия\", \"subtask_title\": \"MAIN SERVICES ГУАА - Демонтаж оборудования \"}]"
    },
    {
        "title": "MAIN SERVICES Демонтаж в US",
        "область": "Иссык-Кульская область",
        "month": "2025-09-01",
        "subtasks": "[{\"count\": 1, \"month\": \"2025-09-01\", \"status\": \"Бухгалтерия\", \"subtask_title\": \"MAIN SERVICES ГУАА - Демонтаж оборудования \"}]"
    },
    {
        "title": "MAIN SERVICES Демонтаж в US",
        "область": "Нарынская область",
        "month": "2025-08-01",
        "subtasks": "[{\"count\": 1, \"month\": \"2025-08-01\", \"status\": \"Бухгалтерия\", \"subtask_title\": \"MAIN SERVICES ГУАА - Демонтаж оборудования \"}]"
    },
    {
        "title": "MAIN SERVICES Демонтаж в US",
        "область": "Нарынская область",
        "month": "2025-09-01",
        "subtasks": "[{\"count\": 12, \"month\": \"2025-09-01\", \"status\": \"Бухгалтерия\", \"subtask_title\": \"MAIN SERVICES ГУАА - Демонтаж оборудования \"}]"
    },
    {
        "title": "MAIN SERVICES Демонтаж в US",
        "область": "Ошская область",
        "month": "2025-09-01",
        "subtasks": "[{\"count\": 224, \"month\": \"2025-09-01\", \"status\": \"Бухгалтерия\", \"subtask_title\": \"MAIN SERVICES ГУАА - Демонтаж оборудования \"}]"
    },
    {
        "title": "MAIN SERVICES Затруднение в работе O!TV",
        "область": "Чуйская область",
        "month": "2025-09-01",
        "subtasks": "[{\"count\": 1, \"month\": \"2025-09-01\", \"status\": \"Изменена дата выезда\", \"subtask_title\": \"MAIN SERVICES [SUB] Инженеры\"}, {\"count\": 1, \"month\": \"2025-09-01\", \"status\": \"ПЕРЕДАНО в НУР: QA\", \"subtask_title\": null}]"
    },
    {
        "title": "MAIN SERVICES Не работает проводной интернет",
        "область": "Джалал-Абадская область",
        "month": "2025-09-01",
        "subtasks": "[{\"count\": 1, \"month\": \"2025-09-01\", \"status\": \"СОГЛАСОВАНИЕ ОАП\", \"subtask_title\": \"MAIN SERVICES [SUB] Монтажники - ОАП\"}]"
    },
    {
        "title": "MAIN SERVICES Не работает проводной интернет",
        "область": "Нарынская область",
        "month": "2025-08-01",
        "subtasks": "[{\"count\": 1, \"month\": \"2025-08-01\", \"status\": \"На уточнении ТМЦ\", \"subtask_title\": \"MAIN SERVICES [SUB] Монтажники\"}]"
    },
    {
        "title": "MAIN SERVICES Не работает проводной интернет",
        "область": "Ошская область",
        "month": "2025-09-01",
        "subtasks": "[{\"count\": 1, \"month\": \"2025-09-01\", \"status\": \"СОГЛАСОВАНИЕ ОАП\", \"subtask_title\": \"MAIN SERVICES [SUB] Монтажники - ОАП\"}]"
    },
    {
        "title": "MAIN SERVICES Не работает проводной интернет",
        "область": "Чуйская область",
        "month": "2025-09-01",
        "subtasks": "[{\"count\": 1, \"month\": \"2025-09-01\", \"status\": \"Изменена дата выезда\", \"subtask_title\": \"MAIN SERVICES [SUB] Инженеры\"}, {\"count\": 1, \"month\": \"2025-09-01\", \"status\": \"Изменена дата выезда\", \"subtask_title\": \"MAIN SERVICES [SUB] Монтажники\"}, {\"count\": 1, \"month\": \"2025-09-01\", \"status\": \"Open\", \"subtask_title\": \"MAIN SERVICES [SUB] Монтажники - ОАП\"}]"
    },
    {
        "title": "MAIN SERVICES Низкая скорость на проводном интернете",
        "область": "Иссык-Кульская область",
        "month": "2025-09-01",
        "subtasks": "[{\"count\": 1, \"month\": \"2025-09-01\", \"status\": \"Open\", \"subtask_title\": \"MAIN SERVICES [SUB] SAIMA: На уточнении\"}]"
    },
    {
        "title": "MAIN SERVICES Низкая скорость на проводном интернете",
        "область": "Чуйская область",
        "month": "2025-08-01",
        "subtasks": "[{\"count\": 1, \"month\": \"2025-08-01\", \"status\": \"Open\", \"subtask_title\": \"MAIN SERVICES [SUB] SAIMA: На уточнении\"}]"
    },
    {
        "title": "MAIN SERVICES Физически подключили, но сервисы не назначались",
        "область": null,
        "month": "2025-08-01",
        "subtasks": "[{\"count\": 2, \"month\": \"2025-08-01\", \"status\": \"Open\", \"subtask_title\": null}]"
    },
    {
        "title": "MAIN SERVICES Физически подключили, но сервисы не назначались",
        "область": null,
        "month": "2025-09-01",
        "subtasks": "[{\"count\": 3, \"month\": \"2025-09-01\", \"status\": \"Open\", \"subtask_title\": null}]"
    }
]''')

get_open_issues_subtasks_by_region_month_ma = json.loads(r'''[
    {
        "title": "MASS ACCIDENTS Массовая проблема - O!TV",
        "область": null,
        "month": "2025-10-01",
        "subtasks": "[{\"count\": 6, \"month\": \"2025-10-01\", \"status\": \"Open\", \"subtask_title\": null}]"
    },
    {
        "title": "MASS ACCIDENTS Массовая проблема - Не работает Интернет",
        "область": "Чуйская область",
        "month": "2025-10-01",
        "subtasks": "[{\"count\": 3, \"month\": \"2025-10-01\", \"status\": \"Open\", \"subtask_title\": null}]"
    },
    {
        "title": "MASS ACCIDENTS Массовая проблема - Не работает Интернет",
        "область": null,
        "month": "2025-10-01",
        "subtasks": "[{\"count\": 29, \"month\": \"2025-10-01\", \"status\": \"Open\", \"subtask_title\": null}]"
    }
]''')

get_open_issues_subtasks_by_region_month_cs = json.loads(r'''[
    {
        "title": "CUSTOMER SERVICES [SUB] Новый абонент - ОАП: Монтаж",
        "область": "Чуйская область",
        "month": "2025-09-01",
        "subtasks": "[{\"count\": 1, \"month\": \"2025-09-01\", \"status\": \"По звонку\", \"subtask_title\": null}, {\"count\": 1, \"month\": \"2025-09-01\", \"status\": \"СОГЛАСОВАНИЕ ОАП\", \"subtask_title\": null}]"
    },
    {
        "title": "CUSTOMER SERVICES Восстановление линии",
        "область": "Ошская область",
        "month": "2025-09-01",
        "subtasks": "[{\"count\": 1, \"month\": \"2025-09-01\", \"status\": \"Open\", \"subtask_title\": null}]"
    },
    {
        "title": "CUSTOMER SERVICES Добавление адреса",
        "область": "Джалал-Абадская область",
        "month": "2025-09-01",
        "subtasks": "[{\"count\": 1, \"month\": \"2025-09-01\", \"status\": \"На уточнение\", \"subtask_title\": null}]"
    },
    {
        "title": "CUSTOMER SERVICES Добавление адреса",
        "область": "Ошская область",
        "month": "2025-09-01",
        "subtasks": "[{\"count\": 4, \"month\": \"2025-09-01\", \"status\": \"На уточнение\", \"subtask_title\": null}]"
    },
    {
        "title": "CUSTOMER SERVICES Добавление адреса",
        "область": "Чуйская область",
        "month": "2025-09-01",
        "subtasks": "[{\"count\": 21, \"month\": \"2025-09-01\", \"status\": \"На уточнение\", \"subtask_title\": null}, {\"count\": 7, \"month\": \"2025-09-01\", \"status\": \"Open\", \"subtask_title\": null}]"
    },
    {
        "title": "CUSTOMER SERVICES Исправление данных в договоре",
        "область": null,
        "month": "2025-08-01",
        "subtasks": "[{\"count\": 1, \"month\": \"2025-08-01\", \"status\": \"На уточнении\", \"subtask_title\": null}]"
    },
    {
        "title": "CUSTOMER SERVICES Исправление данных в договоре",
        "область": null,
        "month": "2025-09-01",
        "subtasks": "[{\"count\": 8, \"month\": \"2025-09-01\", \"status\": \"На уточнении\", \"subtask_title\": null}]"
    },
    {
        "title": "CUSTOMER SERVICES Перенос кабеля на другой адрес",
        "область": "Иссык-Кульская область",
        "month": "2025-09-01",
        "subtasks": "[{\"count\": 1, \"month\": \"2025-09-01\", \"status\": \"Перенос даты подключения\", \"subtask_title\": null}]"
    },
    {
        "title": "CUSTOMER SERVICES Перенос кабеля на другой адрес",
        "область": "Нарынская область",
        "month": "2025-09-01",
        "subtasks": "[{\"count\": 2, \"month\": \"2025-09-01\", \"status\": \"Перенос даты подключения\", \"subtask_title\": null}, {\"count\": 1, \"month\": \"2025-09-01\", \"status\": \"В РАБОТЕ У БРИГАДЫ\", \"subtask_title\": null}]"
    },
    {
        "title": "CUSTOMER SERVICES Перенос кабеля на другой адрес",
        "область": "Ошская область",
        "month": "2025-08-01",
        "subtasks": "[{\"count\": 3, \"month\": \"2025-08-01\", \"status\": \"СОГЛАСОВАНИЕ ОАП\", \"subtask_title\": null}]"
    },
    {
        "title": "CUSTOMER SERVICES Перенос кабеля на другой адрес",
        "область": "Ошская область",
        "month": "2025-09-01",
        "subtasks": "[{\"count\": 26, \"month\": \"2025-09-01\", \"status\": \"СОГЛАСОВАНИЕ ОАП\", \"subtask_title\": null}]"
    },
    {
        "title": "CUSTOMER SERVICES Перенос кабеля на другой адрес",
        "область": "Чуйская область",
        "month": "2025-08-01",
        "subtasks": "[{\"count\": 1, \"month\": \"2025-08-01\", \"status\": \"В РАБОТЕ У БРИГАДЫ\", \"subtask_title\": null}]"
    },
    {
        "title": "CUSTOMER SERVICES Перенос кабеля на другой адрес",
        "область": "Чуйская область",
        "month": "2025-09-01",
        "subtasks": "[{\"count\": 1, \"month\": \"2025-09-01\", \"status\": \"Перенос даты подключения\", \"subtask_title\": null}, {\"count\": 3, \"month\": \"2025-09-01\", \"status\": \"На уточнении\", \"subtask_title\": null}]"
    },
    {
        "title": "CUSTOMER SERVICES Перерасчёт",
        "область": null,
        "month": "2025-09-01",
        "subtasks": "[{\"count\": 1, \"month\": \"2025-09-01\", \"status\": \"Open\", \"subtask_title\": \"CUSTOMER SERVICES SUB: Перерасчёт\"}]"
    },
    {
        "title": "CUSTOMER SERVICES Подключение нового абонента",
        "область": "Баткенская область",
        "month": "2025-09-01",
        "subtasks": "[{\"count\": 1, \"month\": \"2025-09-01\", \"status\": \"Перенос даты подключения\", \"subtask_title\": \"CUSTOMER SERVICES [SUB] Новый абонент - ОАП: Монтаж\"}, {\"count\": 12, \"month\": \"2025-09-01\", \"status\": \"ЗАВЕДЁН В БИЛЛИНГЕ\", \"subtask_title\": null}]"
    },
    {
        "title": "CUSTOMER SERVICES Подключение нового абонента",
        "область": "Джалал-Абадская область",
        "month": "2025-08-01",
        "subtasks": "[{\"count\": 1, \"month\": \"2025-08-01\", \"status\": \"По звонку\", \"subtask_title\": \"CUSTOMER SERVICES [SUB] Новый абонент - ОАП: Монтаж\"}, {\"count\": 2, \"month\": \"2025-08-01\", \"status\": \"ЗАВЕДЁН В БИЛЛИНГЕ\", \"subtask_title\": null}]"
    },
    {
        "title": "CUSTOMER SERVICES Подключение нового абонента",
        "область": "Джалал-Абадская область",
        "month": "2025-09-01",
        "subtasks": "[{\"count\": 1, \"month\": \"2025-09-01\", \"status\": \"Open\", \"subtask_title\": \"CUSTOMER SERVICES [SUB] Новый абонент - ГТМ\"}, {\"count\": 5, \"month\": \"2025-09-01\", \"status\": \"ПЕРЕДАНО в BACKOFFICE\", \"subtask_title\": \"CUSTOMER SERVICES [SUB] Новый абонент - ОАП: Монтаж\"}, {\"count\": 2, \"month\": \"2025-09-01\", \"status\": \"Перенос даты подключения\", \"subtask_title\": \"CUSTOMER SERVICES [SUB] Новый абонент - ОАП: Монтаж\"}, {\"count\": 12, \"month\": \"2025-09-01\", \"status\": \"По звонку\", \"subtask_title\": \"CUSTOMER SERVICES [SUB] Новый абонент - ОАП: Монтаж\"}, {\"count\": 1, \"month\": \"2025-09-01\", \"status\": \"Реконструкция сети\", \"subtask_title\": \"CUSTOMER SERVICES [SUB] Новый абонент - ОАП: Монтаж\"}, {\"count\": 2, \"month\": \"2025-09-01\", \"status\": \"В РАБОТЕ У БРИГАДЫ\", \"subtask_title\": \"CUSTOMER SERVICES [SUB] Новый абонент - ОАП: Монтаж\"}, {\"count\": 1, \"month\": \"2025-09-01\", \"status\": \"Open\", \"subtask_title\": \"CUSTOMER SERVICES [SUB] Новый абонент - ОАП: Монтаж\"}, {\"count\": 2, \"month\": \"2025-09-01\", \"status\": \"В работе\", \"subtask_title\": \"CUSTOMER SERVICES [SUB] Новый абонент - ОАП: Монтаж\"}, {\"count\": 17, \"month\": \"2025-09-01\", \"status\": \"ЗАВЕДЁН В БИЛЛИНГЕ\", \"subtask_title\": null}, {\"count\": 4, \"month\": \"2025-09-01\", \"status\": \"ОТКРЫТИЕ ЛИЦЕВОГО СЧЁТА\", \"subtask_title\": null}]"
    },
    {
        "title": "CUSTOMER SERVICES Подключение нового абонента",
        "область": "Иссык-Кульская область",
        "month": "2025-09-01",
        "subtasks": "[{\"count\": 2, \"month\": \"2025-09-01\", \"status\": \"Open\", \"subtask_title\": \"CUSTOMER SERVICES [SUB] Новый абонент - ГТМ\"}, {\"count\": 1, \"month\": \"2025-09-01\", \"status\": \"В работе\", \"subtask_title\": \"CUSTOMER SERVICES [SUB] Новый абонент - ОАП: Монтаж\"}, {\"count\": 1, \"month\": \"2025-09-01\", \"status\": \"По звонку\", \"subtask_title\": \"CUSTOMER SERVICES [SUB] Новый абонент - ОАП: Монтаж\"}, {\"count\": 1, \"month\": \"2025-09-01\", \"status\": \"Перенос даты подключения\", \"subtask_title\": \"CUSTOMER SERVICES [SUB] Новый абонент - ОАП: Монтаж\"}, {\"count\": 3, \"month\": \"2025-09-01\", \"status\": \"ЗАВЕДЁН В БИЛЛИНГЕ\", \"subtask_title\": null}, {\"count\": 11, \"month\": \"2025-09-01\", \"status\": \"ОТКРЫТИЕ ЛИЦЕВОГО СЧЁТА\", \"subtask_title\": null}, {\"count\": 1, \"month\": \"2025-09-01\", \"status\": \"ПЕРЕДАНО ОАП\", \"subtask_title\": null}]"
    },
    {
        "title": "CUSTOMER SERVICES Подключение нового абонента",
        "область": "Нарынская область",
        "month": "2025-09-01",
        "subtasks": "[{\"count\": 1, \"month\": \"2025-09-01\", \"status\": \"Перенос даты подключения\", \"subtask_title\": \"CUSTOMER SERVICES [SUB] Новый абонент - ОАП: Монтаж\"}, {\"count\": 1, \"month\": \"2025-09-01\", \"status\": \"ПЕРЕДАНО в BACKOFFICE\", \"subtask_title\": \"CUSTOMER SERVICES [SUB] Новый абонент - ОАП: Монтаж\"}, {\"count\": 2, \"month\": \"2025-09-01\", \"status\": \"В РАБОТЕ У БРИГАДЫ\", \"subtask_title\": \"CUSTOMER SERVICES [SUB] Новый абонент - ОАП: Монтаж\"}, {\"count\": 4, \"month\": \"2025-09-01\", \"status\": \"ЗАВЕДЁН В БИЛЛИНГЕ\", \"subtask_title\": null}, {\"count\": 3, \"month\": \"2025-09-01\", \"status\": \"ОТКРЫТИЕ ЛИЦЕВОГО СЧЁТА\", \"subtask_title\": null}]"
    },
    {
        "title": "CUSTOMER SERVICES Подключение нового абонента",
        "область": "Ошская область",
        "month": "2025-08-01",
        "subtasks": "[{\"count\": 2, \"month\": \"2025-08-01\", \"status\": \"ПЕРЕДАНО в BACKOFFICE\", \"subtask_title\": \"CUSTOMER SERVICES [SUB] Новый абонент - ОАП: Монтаж\"}, {\"count\": 1, \"month\": \"2025-08-01\", \"status\": \"Реконструкция сети\", \"subtask_title\": \"CUSTOMER SERVICES [SUB] Новый абонент - ОАП: Монтаж\"}, {\"count\": 9, \"month\": \"2025-08-01\", \"status\": \"ЗАВЕДЁН В БИЛЛИНГЕ\", \"subtask_title\": null}]"
    },
    {
        "title": "CUSTOMER SERVICES Подключение нового абонента",
        "область": "Ошская область",
        "month": "2025-09-01",
        "subtasks": "[{\"count\": 1, \"month\": \"2025-09-01\", \"status\": \"ПЕРЕДАНО в BACKOFFICE\", \"subtask_title\": \"CUSTOMER SERVICES [SUB] Новый абонент - ОАП: Монтаж\"}, {\"count\": 7, \"month\": \"2025-09-01\", \"status\": \"ПРОБЛЕМЫ С ПОДКЛЮЧЕНИЕМ\", \"subtask_title\": \"CUSTOMER SERVICES [SUB] Новый абонент - ОАП: Монтаж\"}, {\"count\": 11, \"month\": \"2025-09-01\", \"status\": \"По звонку\", \"subtask_title\": \"CUSTOMER SERVICES [SUB] Новый абонент - ОАП: Монтаж\"}, {\"count\": 3, \"month\": \"2025-09-01\", \"status\": \"В РАБОТЕ У БРИГАДЫ\", \"subtask_title\": \"CUSTOMER SERVICES [SUB] Новый абонент - ОАП: Монтаж\"}, {\"count\": 3, \"month\": \"2025-09-01\", \"status\": \"Перенос даты подключения\", \"subtask_title\": \"CUSTOMER SERVICES [SUB] Новый абонент - ОАП: Монтаж\"}, {\"count\": 8, \"month\": \"2025-09-01\", \"status\": \"Реконструкция сети\", \"subtask_title\": \"CUSTOMER SERVICES [SUB] Новый абонент - ОАП: Монтаж\"}, {\"count\": 1, \"month\": \"2025-09-01\", \"status\": \"УСТАНОВКА ОБОРУДОВАНИЯ\", \"subtask_title\": null}, {\"count\": 19, \"month\": \"2025-09-01\", \"status\": \"ОТКРЫТИЕ ЛИЦЕВОГО СЧЁТА\", \"subtask_title\": null}, {\"count\": 39, \"month\": \"2025-09-01\", \"status\": \"ЗАВЕДЁН В БИЛЛИНГЕ\", \"subtask_title\": null}]"
    },
    {
        "title": "CUSTOMER SERVICES Подключение нового абонента",
        "область": "Таласская область",
        "month": "2025-09-01",
        "subtasks": "[{\"count\": 6, \"month\": \"2025-09-01\", \"status\": \"ДОБАВЛЕНИЕ АДРЕСА\", \"subtask_title\": null}, {\"count\": 1, \"month\": \"2025-09-01\", \"status\": \"ОТКРЫТИЕ ЛИЦЕВОГО СЧЁТА\", \"subtask_title\": null}, {\"count\": 2, \"month\": \"2025-09-01\", \"status\": \"ЗАВЕДЁН В БИЛЛИНГЕ\", \"subtask_title\": null}]"
    },
    {
        "title": "CUSTOMER SERVICES Подключение нового абонента",
        "область": "Чуйская область",
        "month": "2025-08-01",
        "subtasks": "[{\"count\": 1, \"month\": \"2025-08-01\", \"status\": \"В РАБОТЕ У БРИГАДЫ\", \"subtask_title\": \"CUSTOMER SERVICES [SUB] Новый абонент - ОАП: Монтаж\"}, {\"count\": 1, \"month\": \"2025-08-01\", \"status\": \"Перенос даты подключения\", \"subtask_title\": \"CUSTOMER SERVICES [SUB] Новый абонент - ОАП: Монтаж\"}, {\"count\": 2, \"month\": \"2025-08-01\", \"status\": \"Реконструкция сети ОАП\", \"subtask_title\": \"CUSTOMER SERVICES [SUB] Новый абонент - ОАП: Монтаж\"}, {\"count\": 2, \"month\": \"2025-08-01\", \"status\": \"ОТКРЫТИЕ ЛИЦЕВОГО СЧЁТА\", \"subtask_title\": null}, {\"count\": 10, \"month\": \"2025-08-01\", \"status\": \"ЗАВЕДЁН В БИЛЛИНГЕ\", \"subtask_title\": null}]"
    },
    {
        "title": "CUSTOMER SERVICES Подключение нового абонента",
        "область": "Чуйская область",
        "month": "2025-09-01",
        "subtasks": "[{\"count\": 1, \"month\": \"2025-09-01\", \"status\": \"Open\", \"subtask_title\": \"CUSTOMER SERVICES [SUB] Новый абонент - ГТМ\"}, {\"count\": 3, \"month\": \"2025-09-01\", \"status\": \"Open\", \"subtask_title\": \"CUSTOMER SERVICES [SUB] Новый абонент - ОАП: Монтаж\"}, {\"count\": 13, \"month\": \"2025-09-01\", \"status\": \"В РАБОТЕ У БРИГАДЫ\", \"subtask_title\": \"CUSTOMER SERVICES [SUB] Новый абонент - ОАП: Монтаж\"}, {\"count\": 1, \"month\": \"2025-09-01\", \"status\": \"В работе\", \"subtask_title\": \"CUSTOMER SERVICES [SUB] Новый абонент - ОАП: Монтаж\"}, {\"count\": 1, \"month\": \"2025-09-01\", \"status\": \"СТРОИТЕЛЬСТВО СЕТИ\", \"subtask_title\": \"CUSTOMER SERVICES [SUB] Новый абонент - ОАП: Монтаж\"}, {\"count\": 21, \"month\": \"2025-09-01\", \"status\": \"По звонку\", \"subtask_title\": \"CUSTOMER SERVICES [SUB] Новый абонент - ОАП: Монтаж\"}, {\"count\": 9, \"month\": \"2025-09-01\", \"status\": \"Реконструкция сети\", \"subtask_title\": \"CUSTOMER SERVICES [SUB] Новый абонент - ОАП: Монтаж\"}, {\"count\": 3, \"month\": \"2025-09-01\", \"status\": \"Реконструкция сети ОАП\", \"subtask_title\": \"CUSTOMER SERVICES [SUB] Новый абонент - ОАП: Монтаж\"}, {\"count\": 9, \"month\": \"2025-09-01\", \"status\": \"ОАП\", \"subtask_title\": \"CUSTOMER SERVICES [SUB] Новый абонент - ОАП: Монтаж\"}, {\"count\": 1, \"month\": \"2025-09-01\", \"status\": \"ОАП не подключенный\", \"subtask_title\": \"CUSTOMER SERVICES [SUB] Новый абонент - ОАП: Монтаж\"}, {\"count\": 10, \"month\": \"2025-09-01\", \"status\": \"ПРОБЛЕМЫ С ПОДКЛЮЧЕНИЕМ\", \"subtask_title\": \"CUSTOMER SERVICES [SUB] Новый абонент - ОАП: Монтаж\"}, {\"count\": 34, \"month\": \"2025-09-01\", \"status\": \"Перенос даты подключения\", \"subtask_title\": \"CUSTOMER SERVICES [SUB] Новый абонент - ОАП: Монтаж\"}, {\"count\": 43, \"month\": \"2025-09-01\", \"status\": \"ЗАВЕДЁН В БИЛЛИНГЕ\", \"subtask_title\": null}, {\"count\": 17, \"month\": \"2025-09-01\", \"status\": \"ОТКРЫТИЕ ЛИЦЕВОГО СЧЁТА\", \"subtask_title\": null}, {\"count\": 3, \"month\": \"2025-09-01\", \"status\": \"Open\", \"subtask_title\": null}]"
    },
    {
        "title": "CUSTOMER SERVICES Подключение нового корпоративного абонента",
        "область": "Баткенская область",
        "month": "2025-09-01",
        "subtasks": "[{\"count\": 6, \"month\": \"2025-09-01\", \"status\": \"Open\", \"subtask_title\": null}]"
    },
    {
        "title": "CUSTOMER SERVICES Подключение нового корпоративного абонента",
        "область": "Джалал-Абадская область",
        "month": "2025-09-01",
        "subtasks": "[{\"count\": 20, \"month\": \"2025-09-01\", \"status\": \"Open\", \"subtask_title\": null}, {\"count\": 3, \"month\": \"2025-09-01\", \"status\": \"В работе\", \"subtask_title\": null}]"
    },
    {
        "title": "CUSTOMER SERVICES Подключение нового корпоративного абонента",
        "область": "Иссык-Кульская область",
        "month": "2025-09-01",
        "subtasks": "[{\"count\": 2, \"month\": \"2025-09-01\", \"status\": \"В работе\", \"subtask_title\": null}]"
    },
    {
        "title": "CUSTOMER SERVICES Подключение нового корпоративного абонента",
        "область": "Ошская область",
        "month": "2025-09-01",
        "subtasks": "[{\"count\": 1, \"month\": \"2025-09-01\", \"status\": \"В работе\", \"subtask_title\": null}, {\"count\": 5, \"month\": \"2025-09-01\", \"status\": \"Open\", \"subtask_title\": null}]"
    },
    {
        "title": "CUSTOMER SERVICES Подключение нового корпоративного абонента",
        "область": "Таласская область",
        "month": "2025-09-01",
        "subtasks": "[{\"count\": 3, \"month\": \"2025-09-01\", \"status\": \"Open\", \"subtask_title\": null}]"
    },
    {
        "title": "CUSTOMER SERVICES Подключение нового корпоративного абонента",
        "область": "Чуйская область",
        "month": "2025-09-01",
        "subtasks": "[{\"count\": 1, \"month\": \"2025-09-01\", \"status\": \"В работе\", \"subtask_title\": null}]"
    },
    {
        "title": "CUSTOMER SERVICES Смена ТП 300/500",
        "область": "Джалал-Абадская область",
        "month": "2025-09-01",
        "subtasks": "[{\"count\": 1, \"month\": \"2025-09-01\", \"status\": \"ОЖИДАНИЕ ОПЛАТЫ\", \"subtask_title\": null}]"
    },
    {
        "title": "CUSTOMER SERVICES Смена ТП 300/500",
        "область": "Иссык-Кульская область",
        "month": "2025-09-01",
        "subtasks": "[{\"count\": 1, \"month\": \"2025-09-01\", \"status\": \"ОЖИДАНИЕ ОПЛАТЫ\", \"subtask_title\": null}]"
    },
    {
        "title": "CUSTOMER SERVICES Смена ТП 300/500",
        "область": "Ошская область",
        "month": "2025-09-01",
        "subtasks": "[{\"count\": 4, \"month\": \"2025-09-01\", \"status\": \"ОЖИДАНИЕ ОПЛАТЫ\", \"subtask_title\": null}]"
    },
    {
        "title": "CUSTOMER SERVICES Смена ТП 300/500",
        "область": "Чуйская область",
        "month": "2025-08-01",
        "subtasks": "[{\"count\": 1, \"month\": \"2025-08-01\", \"status\": \"ОЖИДАНИЕ ОПЛАТЫ\", \"subtask_title\": null}]"
    },
    {
        "title": "CUSTOMER SERVICES Смена ТП 300/500",
        "область": "Чуйская область",
        "month": "2025-09-01",
        "subtasks": "[{\"count\": 1, \"month\": \"2025-09-01\", \"status\": \"Open\", \"subtask_title\": \"MAIN SERVICES [SUB] SAIMA: На уточнении\"}, {\"count\": 1, \"month\": \"2025-09-01\", \"status\": \"На уточнении\", \"subtask_title\": \"MAIN SERVICES [SUB] Замена оборудования\"}, {\"count\": 10, \"month\": \"2025-09-01\", \"status\": \"ОЖИДАНИЕ ОПЛАТЫ\", \"subtask_title\": null}]"
    },
    {
        "title": "CUSTOMER SERVICES Смена технологии",
        "область": "Чуйская область",
        "month": "2025-09-01",
        "subtasks": "[{\"count\": 1, \"month\": \"2025-09-01\", \"status\": \"На уточнении\", \"subtask_title\": null}]"
    },
    {
        "title": "CUSTOMER SERVICES Согласование особых условий",
        "область": null,
        "month": "2025-09-01",
        "subtasks": "[{\"count\": 2, \"month\": \"2025-09-01\", \"status\": \"Open\", \"subtask_title\": null}]"
    },
    {
        "title": "CUSTOMER SERVICES ТП Сотрудник 550 + О!TV от Сайма Телеком",
        "область": "Чуйская область",
        "month": "2025-09-01",
        "subtasks": "[{\"count\": 1, \"month\": \"2025-09-01\", \"status\": \"На уточнение\", \"subtask_title\": \"CUSTOMER SERVICES Добавление адреса\"}]"
    },
    {
        "title": "CUSTOMER SERVICES Установка дополнительной O!TV приставки (выкуп)",
        "область": "Чуйская область",
        "month": "2025-09-01",
        "subtasks": "[{\"count\": 1, \"month\": \"2025-09-01\", \"status\": \"Перенос даты подключения\", \"subtask_title\": \"PROCEDURES [SUB] Процедуры\"}]"
    }
]''')

get_unique_unresolved_issues_by_month_region_title_ms = json.loads(r'''[
    {
        "month": "2025-08-01",
        "область": "Нарынская область",
        "title": "MAIN SERVICES Демонтаж в US",
        "count": 1
    },
    {
        "month": "2025-08-01",
        "область": "Нарынская область",
        "title": "MAIN SERVICES Не работает проводной интернет",
        "count": 1
    },
    {
        "month": "2025-08-01",
        "область": null,
        "title": "MAIN SERVICES Физически подключили, но сервисы не назначались",
        "count": 2
    },
    {
        "month": "2025-09-01",
        "область": "Джалал-Абадская область",
        "title": "MAIN SERVICES Демонтаж в US",
        "count": 69
    },
    {
        "month": "2025-09-01",
        "область": "Джалал-Абадская область",
        "title": "MAIN SERVICES Не работает проводной интернет",
        "count": 1
    },
    {
        "month": "2025-09-01",
        "область": "Иссык-Кульская область",
        "title": "MAIN SERVICES Демонтаж в US",
        "count": 1
    },
    {
        "month": "2025-09-01",
        "область": "Нарынская область",
        "title": "MAIN SERVICES Демонтаж в US",
        "count": 12
    },
    {
        "month": "2025-09-01",
        "область": "Ошская область",
        "title": "MAIN SERVICES Демонтаж в US",
        "count": 224
    },
    {
        "month": "2025-09-01",
        "область": "Ошская область",
        "title": "MAIN SERVICES Не работает проводной интернет",
        "count": 1
    },
    {
        "month": "2025-09-01",
        "область": "Чуйская область",
        "title": "MAIN SERVICES Затруднение в работе O!TV",
        "count": 2
    },
    {
        "month": "2025-09-01",
        "область": "Чуйская область",
        "title": "MAIN SERVICES Не работает проводной интернет",
        "count": 3
    },
    {
        "month": "2025-09-01",
        "область": null,
        "title": "MAIN SERVICES Версия TV-приставки",
        "count": 4
    },
    {
        "month": "2025-09-01",
        "область": null,
        "title": "MAIN SERVICES Физически подключили, но сервисы не назначались",
        "count": 3
    }
]''')

get_unique_unresolved_issues_by_month_region_title_ma = []

get_unique_unresolved_issues_by_month_region_title_cs = json.loads(r'''[
    {
        "month": "2025-08-01",
        "область": "Джалал-Абадская область",
        "title": "CUSTOMER SERVICES Подключение нового абонента",
        "count": 3
    },
    {
        "month": "2025-08-01",
        "область": "Ошская область",
        "title": "CUSTOMER SERVICES Перенос кабеля на другой адрес",
        "count": 3
    },
    {
        "month": "2025-08-01",
        "область": "Ошская область",
        "title": "CUSTOMER SERVICES Подключение нового абонента",
        "count": 12
    },
    {
        "month": "2025-08-01",
        "область": "Чуйская область",
        "title": "CUSTOMER SERVICES Перенос кабеля на другой адрес",
        "count": 1
    },
    {
        "month": "2025-08-01",
        "область": "Чуйская область",
        "title": "CUSTOMER SERVICES Подключение нового абонента",
        "count": 16
    },
    {
        "month": "2025-08-01",
        "область": "Чуйская область",
        "title": "CUSTOMER SERVICES Смена ТП 300/500",
        "count": 1
    },
    {
        "month": "2025-08-01",
        "область": null,
        "title": "CUSTOMER SERVICES Исправление данных в договоре",
        "count": 1
    },
    {
        "month": "2025-09-01",
        "область": "Баткенская область",
        "title": "CUSTOMER SERVICES Подключение нового абонента",
        "count": 13
    },
    {
        "month": "2025-09-01",
        "область": "Баткенская область",
        "title": "CUSTOMER SERVICES Подключение нового корпоративного абонента",
        "count": 6
    },
    {
        "month": "2025-09-01",
        "область": "Джалал-Абадская область",
        "title": "CUSTOMER SERVICES Добавление адреса",
        "count": 1
    },
    {
        "month": "2025-09-01",
        "область": "Джалал-Абадская область",
        "title": "CUSTOMER SERVICES Подключение нового абонента",
        "count": 45
    },
    {
        "month": "2025-09-01",
        "область": "Джалал-Абадская область",
        "title": "CUSTOMER SERVICES Подключение нового корпоративного абонента",
        "count": 23
    },
    {
        "month": "2025-09-01",
        "область": "Джалал-Абадская область",
        "title": "CUSTOMER SERVICES Смена ТП 300/500",
        "count": 1
    },
    {
        "month": "2025-09-01",
        "область": "Иссык-Кульская область",
        "title": "CUSTOMER SERVICES Перенос кабеля на другой адрес",
        "count": 1
    },
    {
        "month": "2025-09-01",
        "область": "Иссык-Кульская область",
        "title": "CUSTOMER SERVICES Подключение нового абонента",
        "count": 19
    },
    {
        "month": "2025-09-01",
        "область": "Иссык-Кульская область",
        "title": "CUSTOMER SERVICES Подключение нового корпоративного абонента",
        "count": 2
    },
    {
        "month": "2025-09-01",
        "область": "Иссык-Кульская область",
        "title": "CUSTOMER SERVICES Смена ТП 300/500",
        "count": 1
    },
    {
        "month": "2025-09-01",
        "область": "Нарынская область",
        "title": "CUSTOMER SERVICES Перенос кабеля на другой адрес",
        "count": 3
    },
    {
        "month": "2025-09-01",
        "область": "Нарынская область",
        "title": "CUSTOMER SERVICES Подключение нового абонента",
        "count": 10
    },
    {
        "month": "2025-09-01",
        "область": "Ошская область",
        "title": "CUSTOMER SERVICES Восстановление линии",
        "count": 1
    },
    {
        "month": "2025-09-01",
        "область": "Ошская область",
        "title": "CUSTOMER SERVICES Добавление адреса",
        "count": 4
    },
    {
        "month": "2025-09-01",
        "область": "Ошская область",
        "title": "CUSTOMER SERVICES Перенос кабеля на другой адрес",
        "count": 26
    },
    {
        "month": "2025-09-01",
        "область": "Ошская область",
        "title": "CUSTOMER SERVICES Подключение нового абонента",
        "count": 89
    },
    {
        "month": "2025-09-01",
        "область": "Ошская область",
        "title": "CUSTOMER SERVICES Подключение нового корпоративного абонента",
        "count": 6
    },
    {
        "month": "2025-09-01",
        "область": "Ошская область",
        "title": "CUSTOMER SERVICES Смена ТП 300/500",
        "count": 4
    },
    {
        "month": "2025-09-01",
        "область": "Таласская область",
        "title": "CUSTOMER SERVICES Подключение нового абонента",
        "count": 9
    },
    {
        "month": "2025-09-01",
        "область": "Таласская область",
        "title": "CUSTOMER SERVICES Подключение нового корпоративного абонента",
        "count": 3
    },
    {
        "month": "2025-09-01",
        "область": "Чуйская область",
        "title": "CUSTOMER SERVICES [SUB] Новый абонент - ОАП: Монтаж",
        "count": 2
    },
    {
        "month": "2025-09-01",
        "область": "Чуйская область",
        "title": "CUSTOMER SERVICES Восстановление линии",
        "count": 1
    },
    {
        "month": "2025-09-01",
        "область": "Чуйская область",
        "title": "CUSTOMER SERVICES Добавление адреса",
        "count": 28
    },
    {
        "month": "2025-09-01",
        "область": "Чуйская область",
        "title": "CUSTOMER SERVICES Перенос кабеля на другой адрес",
        "count": 4
    },
    {
        "month": "2025-09-01",
        "область": "Чуйская область",
        "title": "CUSTOMER SERVICES Подключение нового абонента",
        "count": 163
    },
    {
        "month": "2025-09-01",
        "область": "Чуйская область",
        "title": "CUSTOMER SERVICES Подключение нового корпоративного абонента",
        "count": 1
    },
    {
        "month": "2025-09-01",
        "область": "Чуйская область",
        "title": "CUSTOMER SERVICES Смена ТП 300/500",
        "count": 11
    },
    {
        "month": "2025-09-01",
        "область": "Чуйская область",
        "title": "CUSTOMER SERVICES Смена технологии",
        "count": 1
    },
    {
        "month": "2025-09-01",
        "область": "Чуйская область",
        "title": "CUSTOMER SERVICES ТП Сотрудник 550 + О!TV от Сайма Телеком",
        "count": 1
    },
    {
        "month": "2025-09-01",
        "область": "Чуйская область",
        "title": "CUSTOMER SERVICES Установка дополнительной O!TV приставки (выкуп)",
        "count": 1
    },
    {
        "month": "2025-09-01",
        "область": null,
        "title": "CUSTOMER SERVICES Исправление данных в договоре",
        "count": 8
    },
    {
        "month": "2025-09-01",
        "область": null,
        "title": "CUSTOMER SERVICES Перерасчёт",
        "count": 1
    },
    {
        "month": "2025-09-01",
        "область": null,
        "title": "CUSTOMER SERVICES Согласование особых условий",
        "count": 2
    }
]''')


