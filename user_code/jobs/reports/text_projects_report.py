from dagster import job

from assets.extractor.bot_data import bot_handling_data


@job
def report_job():
    bot_handling_data()
