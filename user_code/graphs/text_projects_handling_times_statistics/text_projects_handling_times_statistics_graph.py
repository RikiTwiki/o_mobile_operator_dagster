from dagster import graph

from excel_files.text_projects_handling_times_statistics import get_chats_op, get_user_statuses_op, \
    text_handling_times_excel, upload_text_handling_times_excels_op


# импортируй свои ops (или держи всё в одном модуле)
# from .your_module import get_chats_op, get_user_statuses_op, text_handling_times_excel

@graph
def text_projects_handling_time_graph():
    chats = get_chats_op()
    statuses = get_user_statuses_op()
    saved = text_handling_times_excel(chats, statuses)
    upload_text_handling_times_excels_op(saved)