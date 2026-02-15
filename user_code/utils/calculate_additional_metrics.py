from .str_to_time import str_to_time, str_to_datetime_object
from dateutil import parser


def calculate_additional_metrics(item, assigny=None) -> dict:
    """
    Высчитывает и возвращает дополнительные метрики для анализа
    чата и действий специалистов.
    """
    chat_created_at_str = None
    if item.get('created_at'):
        try:
            parsed_date = parser.parse(item['created_at'])
            chat_created_at_str = parsed_date.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            pass

    assignee_created_at = None
    assignee_unblocked_at = None
    assignee_finished_at = None
    user_holding_time = 0

    if assigny:
        if assigny.get('created_at'):
            dt_obj = str_to_datetime_object(assigny['created_at'])
            assignee_created_at = dt_obj.strftime("%Y-%m-%d %H:%M:%S") if dt_obj else None

        if assigny.get('unblocked_at'):
            dt_obj = str_to_datetime_object(assigny['unblocked_at'])
            assignee_unblocked_at = dt_obj.strftime("%Y-%m-%d %H:%M:%S") if dt_obj else None

        if assigny.get('last_action_at'):
            dt_obj = str_to_datetime_object(assigny['last_action_at'])
            assignee_finished_at = dt_obj.strftime("%Y-%m-%d %H:%M:%S") if dt_obj else None

        if assigny.get('user_holding_time'):
            if isinstance(assigny['user_holding_time'], (int, float)):
                user_holding_time = assigny['user_holding_time']
            else:
                try:
                    user_holding_time = str_to_time(assigny['user_holding_time'])
                except (ValueError, TypeError):
                    user_holding_time = 0

    return {
        "chat_created_at": chat_created_at_str,
        "user_holding_time": user_holding_time,
        "assignee_created_at": assignee_created_at,
        "assignee_unblocked_at": assignee_unblocked_at,
        "assignee_finished_at": assignee_finished_at,
    }