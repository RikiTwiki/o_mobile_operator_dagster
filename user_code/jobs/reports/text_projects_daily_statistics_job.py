from dagster import job
from resources import resources
from jobs.reports.text_projects_report import report_job  # если требуется импорт других jobs
from graphs.text_projects_daily_statistics_graph import process_aggregation_project_data

# Job на основе process_aggregation_project_data графа
text_projects_daily_statistics_job = process_aggregation_project_data.to_job(
    name="text_projects_daily_statistics",
    resource_defs=resources,
    config={
        "ops": {
            "process_aggregation_project_data": {
                "config": {
                    "start_date":       "2025-07-01T00:00:00",
                    "end_date":         "2025-07-22T00:00:00",
                    "month_start":      "2025-07-01T00:00:00",
                    "month_end":        "2025-07-22T00:00:00",
                    "project_ids":      [1,3,4,5,6,8,9],
                }
            }
        }
    }
)