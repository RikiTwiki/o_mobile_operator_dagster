# ops/saima_second_line/process_ops.py
from __future__ import annotations

import os
from dataclasses import asdict, is_dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests
from dagster import op, get_dagster_logger, In, Out, Failure

from utils.getters.jira_data_getter_next import JiraDataGetterNext
from utils.getters.omni_data_getter import OmniDataGetter

from utils.array_operations import left_join_array
from datetime import timedelta, datetime, date

def _to_dict(x: Any) -> Dict[str, Any]:
    if x is None:
        return {}
    if isinstance(x, dict):
        return x
    if is_dataclass(x):
        return asdict(x)
    if hasattr(x, "__dict__"):
        return dict(x.__dict__)
    try:
        return dict(x)
    except Exception:
        return {"value": x}


def _as_records(items: Any) -> List[Dict[str, Any]]:
    if not items:
        return []
    if isinstance(items, list):
        return [_to_dict(i) for i in items]
    return [_to_dict(items)]


def _date10(val: Any) -> str:
    s = str(val or "")
    return s[:10]

def _to_int(v: Any) -> int:
    try:
        if v is None:
            return 0
        if isinstance(v, bool):
            return int(v)
        return int(float(v))
    except Exception:
        return 0

def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default

def _safe_div(a: float, b: float) -> float:
    return a / b if b else 0.0

def _as_dict(x: Any) -> Dict[str, Any]:
    if x is None:
        return {}
    if isinstance(x, dict):
        return x
    if hasattr(x, "dict") and callable(getattr(x, "dict")):
        return x.dict()
    if hasattr(x, "__dict__"):
        return {k: v for k, v in vars(x).items() if not k.startswith("_")}
    try:
        return dict(x)
    except Exception:
        return {"value": x}


def _to_dict_list(items: Any) -> List[Dict[str, Any]]:
    if items is None:
        return []
    if isinstance(items, list):
        return [_as_dict(i) for i in items]
    return [_as_dict(items)]


def _norm_day(v: Any) -> str:
    """
    Приводим к ключу дня: 'YYYY-MM-DD'
    (подходит для join + для getDataInDates у тебя обычно тоже так)
    """
    if v is None:
        return ""
    s = str(v).strip()
    # 'YYYY-MM-DD HH:MM:SS' -> 'YYYY-MM-DD'
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    return s


def _norm_items_date(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for it in items:
        d = dict(it)
        d["date"] = _norm_day(d.get("date"))
        out.append(d)
    # сортировка на всякий
    out.sort(key=lambda x: x.get("date") or "")
    return out

@op(required_resource_keys={"report_utils", "source_dwh_prod"})
def main_data_op(context) -> List[Dict[str, Any]]:
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate

    jira = JiraDataGetterNext(dwh_conn=context.resources.source_dwh_prod, start_date=start_date, end_date=end_date, jira="jirasd_saima")
    jira.start_date = start_date
    jira.end_date = end_date
    jira.set_days_trunked_data()

    issues_daily_data = _to_dict_list(jira.get_created_issues_data_by_types(False))

    context.log.info(f"issues_daily_data: {issues_daily_data}")

    average_issue_duration_daily_data = _to_dict_list(jira.get_issues_status_from_created_data_by_types())

    context.log.info(f"average_issue_duration_daily_data: {average_issue_duration_daily_data}")

    average_in_progress_issued_duration_daily_data = _to_dict_list(
        jira.get_issues_status_from_in_progress_data_by_types(["10101", "13701"])
    )

    context.log.info(f"average_in_progress_issued_duration_daily_data: {average_in_progress_issued_duration_daily_data}")

    average_handling_time_issued_duration_daily_data = _to_dict_list(
        jira.get_issues_status_from_in_progress_data_by_types(
            ["13701"], False, "avg_handling_time_count", "avg_handling_time_min", "handling_time_sum"
        )
    )

    context.log.info(f"average_handling_time_issued_duration_daily_data: {average_handling_time_issued_duration_daily_data}")

    issues_daily_data = _norm_items_date(issues_daily_data)
    average_issue_duration_daily_data = _norm_items_date(average_issue_duration_daily_data)
    average_in_progress_issued_duration_daily_data = _norm_items_date(average_in_progress_issued_duration_daily_data)
    average_handling_time_issued_duration_daily_data = _norm_items_date(average_handling_time_issued_duration_daily_data)

    main = left_join_array(issues_daily_data, average_issue_duration_daily_data, "date")
    main = left_join_array(main, average_in_progress_issued_duration_daily_data, "date")
    main = left_join_array(main, average_handling_time_issued_duration_daily_data, "date")

    context.log.info(
        f"jira_second_line_main_data_op: rows={len(main)} "
        f"(issues={len(issues_daily_data)}, open_close={len(average_issue_duration_daily_data)}, "
        f"in_progress={len(average_in_progress_issued_duration_daily_data)}, aht={len(average_handling_time_issued_duration_daily_data)})"
    )
    if main[:2]:
        context.log.debug(f"jira_second_line_main_data_op sample: {main[:2]}")

    context.log.info(f"main: {main}")

    return main

@op(required_resource_keys={"report_utils", "source_dwh_prod"})
def jira_handling_daily_data_op(context) -> List[Dict[str, Any]]:
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate

    jira = JiraDataGetterNext(dwh_conn=context.resources.source_dwh_prod, start_date=start_date, end_date=end_date, jira="jirasd_saima")
    jira.start_date = start_date
    jira.end_date = end_date
    jira.set_days_trunked_data()

    raw = _to_dict_list(jira.get_issue_by_resolution_count())
    out: List[Dict[str, Any]] = []

    for it in raw:
        d = _norm_day(it.get("date"))

        resolved = _to_int(it.get("resolved"))
        not_resolved = _to_int(it.get("not_resolved"))
        client_side_reason = _to_int(it.get("client_side_reason"))
        infrastructure_hardware_issue = _to_int(it.get("infrastructure_hardware_issue"))
        ticket_cancel = _to_int(it.get("ticket_cancel"))

        resolved_count = _to_int(it.get("resolved_count"))
        if resolved_count == 0:
            resolved_count = resolved + not_resolved

        total_count = _to_int(it.get("count"))
        if total_count == 0:
            total_count = (
                resolved
                + not_resolved
                + client_side_reason
                + infrastructure_hardware_issue
                + ticket_cancel
            )

        resolved_percent = it.get("resolved_percent")
        try:
            resolved_percent = float(resolved_percent) if resolved_percent is not None else None
        except Exception:
            resolved_percent = None

        if resolved_percent is None:
            resolved_percent = (resolved / resolved_count * 100.0) if resolved_count > 0 else 0.0

        row = {
            "date": d,

            "resolved": resolved,
            "not_resolved": not_resolved,
            "client_side_reason": client_side_reason,
            "infrastructure_hardware_issue": infrastructure_hardware_issue,
            "ticket_cancel": ticket_cancel,

            "resolved_count": resolved_count,          # resolved + not_resolved
            "resolved_percent": round(float(resolved_percent), 1),

            "count": total_count,                       # сумма всех статусов (для max/stacked)
        }

        # (опционально) чтобы было удобно где-то ещё:
        row["not_resolved_percent"] = round(
            (not_resolved / resolved_count * 100.0) if resolved_count > 0 else 0.0, 1
        )

        out.append(row)

    out.sort(key=lambda x: x.get("date") or "")

    context.log.info(f"jira_second_line_handling_daily_data_op: rows={len(out)} start={start_date} end={end_date}")
    if out[:2]:
        context.log.debug(f"jira_second_line_handling_daily_data_op sample: {out[:2]}")

    return out

@op(required_resource_keys={"report_utils", "source_cp", "source_dwh_prod"})
def omni_second_line_requests_daily_op(context) -> List[Dict[str, Any]]:
    """
    PHP аналог блока:
      $linked_jira_issue = $jiraData->get_linked_jira_issues_by_date()();
      $data = $OmniDataGetter->get_all_aggregated_requests_new()();
      foreach $data: row.linked_jira_issue = map[date]; row.total = sum(components)
    """
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate

    # 1) linked_jira_issue (daily)
    jira = JiraDataGetterNext(dwh_conn=context.resources.source_dwh_prod, start_date=start_date, end_date=end_date, jira="jirasd_saima")
    jira.start_date = start_date
    jira.end_date = end_date
    jira.set_days_trunked_data()

    linked_raw = _to_dict_list(jira.get_linked_jira_issues_by_date())
    linked_map: Dict[str, int] = {}
    for it in linked_raw:
        d = _norm_day(it.get("date"))
        linked_map[d] = _to_int(it.get("linked_jira_issue"))

    # 2) omni aggregated requests (daily)
    omni = OmniDataGetter(conn=context.resources.source_cp, start_date=start_date, end_date=end_date)
    omni.start_date = start_date
    omni.end_date = end_date
    omni.set_days_trunked_data()

    data_raw = _to_dict_list(omni.get_all_aggregated_requests_new())
    out: List[Dict[str, Any]] = []

    for it in data_raw:
        d = _norm_day(it.get("date"))
        created_other = _to_int(it.get("created_other_jira_issue"))
        closed_1 = _to_int(it.get("closed_by_first_line"))
        closed_2 = _to_int(it.get("closed_by_second_line"))

        linked = _to_int(linked_map.get(d, 0))

        total = created_other + linked + closed_1 + closed_2

        out.append(
            {
                "date": d,
                "created_other_jira_issue": created_other,
                "linked_jira_issue": linked,
                "closed_by_first_line": closed_1,
                "closed_by_second_line": closed_2,
                "total": total,
            }
        )

    out.sort(key=lambda x: x.get("date") or "")
    context.log.info(
        f"omni_second_line_requests_daily_op: rows={len(out)} "
        f"linked_rows={len(linked_raw)} start={start_date} end={end_date}"
    )
    if out[:2]:
        context.log.debug(f"omni_second_line_requests_daily_op sample: {out[:2]}")
    return out

@op(required_resource_keys={"report_utils", "source_cp", "source_dwh_prod"})
def omni_second_line_requests_avg_op(context) -> Dict[str, Any]:
    """
    PHP аналог:
      Omni: StartDate = Dates[0]; EndDate = EndDate; set_months_trunked_data()(); get_all_aggregated_requests_new()(true)
      Jira: StartDate = Dates[0]; EndDate = EndDate; set_days_trunked_data()(); get_linked_jira_issues_by_date()(true)
    Возвращаем агрегаты по окну (обычно последние 8 дней) — одной строкой.
    """
    dates = context.resources.report_utils.get_utils()
    # как в PHP: берем "самую раннюю" дату из списка
    range_start = dates.Dates[0] if getattr(dates, "Dates", None) else dates.StartDate
    range_end = dates.EndDate
    start_date = dates.StartDate
    end_date = dates.EndDate

    # Omni aggregate (unset_date=True)
    omni = OmniDataGetter(conn=context.resources.source_cp, start_date=start_date, end_date=end_date)
    omni.start_date = range_start
    omni.end_date = range_end
    omni.set_months_trunked_data()

    avg_rows = _to_dict_list(omni.get_all_aggregated_requests_new(True))
    avg_row = avg_rows[0] if avg_rows else {}

    closed_1 = _to_int(avg_row.get("closed_by_first_line"))
    closed_2 = _to_int(avg_row.get("closed_by_second_line"))
    created_other = _to_int(avg_row.get("created_other_jira_issue"))

    # Jira linked aggregate (unset_date=True)
    jira = JiraDataGetterNext(dwh_conn=context.resources.source_dwh_prod, start_date=start_date, end_date=end_date, jira="jirasd_saima")
    jira.start_date = range_start
    jira.end_date = range_end
    jira.set_days_trunked_data()

    linked_rows = _to_dict_list(jira.get_linked_jira_issues_by_date(True))
    linked = _to_int((linked_rows[0] or {}).get("linked_jira_issue")) if linked_rows else 0

    total = created_other + linked + closed_1 + closed_2

    out = {
        "range_start": str(range_start),
        "range_end": str(range_end),
        "closed_by_first_line": closed_1,
        "closed_by_second_line": closed_2,
        "created_other_jira_issue": created_other,
        "linked_jira_issue": linked,
        "total": total,
    }

    context.log.info(
        "omni_second_line_requests_avg_op: "
        f"range={out['range_start']}..{out['range_end']} "
        f"total={total} (1line={closed_1}, 2line={closed_2}, other={created_other}, linked={linked})"
    )
    return out


@op(required_resource_keys={"report_utils", "source_dwh_prod"})
def jira_resolution_daily_op(context) -> List[Dict[str, Any]]:
    """
    PHP аналог:
      $jiraData = new JiraDataGetterNext(Dates[0], EndDate, ...); set_days_trunked_data()();
      $resolutionDailyData = $jiraData->getIssueBytResolution();
    Возвращаем rows: {date, status, title, count}
    """
    dates = context.resources.report_utils.get_utils()
    range_start = dates.Dates[0] if getattr(dates, "Dates", None) else dates.StartDate
    range_end = dates.EndDate
    start_date = dates.StartDate
    end_date = dates.EndDate

    jira = JiraDataGetterNext(dwh_conn=context.resources.source_dwh_prod, start_date=start_date, end_date=end_date, jira="jirasd_saima")
    jira.start_date = range_start
    jira.end_date = range_end
    jira.set_days_trunked_data()

    raw = _to_dict_list(jira.get_issue_by_resolution())
    out: List[Dict[str, Any]] = []

    for it in raw:
        out.append(
            {
                "date": _norm_day(it.get("date")),
                "status": it.get("status"),
                "title": it.get("title"),
                "count": _to_int(it.get("count")),
            }
        )

    status_priority = {
        "Помогли": 1,
        "Не помогли": 2,
        "Другое": 3,
        "Причина на стороне абонента": 4,
        "Отмена заявки": 5,
        "Проблемы на линии/ с оборудованием": 6,
    }

    # сортируем для стабильности
    out.sort(key=lambda x: (
        status_priority.get(x.get("status") or "", 99),
        x.get("title") or "",
        x.get("date") or "",
    ))

    context.log.info(f"jira_resolution_daily_op: rows={len(out)} range={range_start}..{range_end}")
    if out[:2]:
        context.log.debug(f"jira_resolution_daily_op sample: {out[:2]}")
    return out


@op(required_resource_keys={"report_utils", "source_dwh_prod"})
def jira_second_line_month_kpis_op(context) -> Dict[str, Any]:
    """
    PHP аналог "month kpi" блока:
      jira set_months_trunked_data()()
      issuesMonthlyData = getCreatedIssuesDataByTypes(false, true)
      issuesResolutionMonthlyData = getIssueBytResolutionCount(true)
      avgIssues = total/7
      Avg durations (open_close / in_progress / handling_time)
    """
    dates = context.resources.report_utils.get_utils()
    range_start = dates.Dates[0] if getattr(dates, "Dates", None) else dates.StartDate
    range_end = dates.EndDate
    start_date = dates.StartDate
    end_date = dates.EndDate

    jira = JiraDataGetterNext(dwh_conn=context.resources.source_dwh_prod, start_date=start_date, end_date=end_date, jira="jirasd_saima")
    jira.start_date = range_start
    jira.end_date = range_end
    jira.set_months_trunked_data()

    issues_month = _to_dict_list(jira.get_created_issues_data_by_types(False, True))
    resolution_month = _to_dict_list(jira.get_issue_by_resolution_count(True))

    avg_open_close = _to_dict_list(jira.get_issues_status_from_created_data_by_types(True))
    avg_in_progress = _to_dict_list(jira.get_issues_status_from_in_progress_data_by_types(["10101", "13701"], True))
    avg_handling = _to_dict_list(
        jira.get_issues_status_from_in_progress_data_by_types(
            ["13701"], True, "avg_handling_time_count", "avg_handling_time_min", "handling_time_sum"
        )
    )

    total_issues = _to_int((issues_month[0] or {}).get("total")) if issues_month else 0

    # В PHP делят на 7
    avg_issues = round(total_issues / 7, 1) if total_issues else 0.0

    res0 = resolution_month[0] if resolution_month else {}
    resolved_percent = round(_to_float(res0.get("resolved_percent")), 1)
    not_resolved_percent = round(_to_float(res0.get("not_resolved_percent")), 1)

    avg_open_close_min = _to_float((avg_open_close[0] or {}).get("avg_open_close_min"))
    avg_in_progress_min = _to_float((avg_in_progress[0] or {}).get("avg_in_progress_min"))
    avg_handling_time_min = _to_float((avg_handling[0] or {}).get("avg_handling_time_min"))

    out = {
        "range_start": str(range_start),
        "range_end": str(range_end),

        "total_issues": total_issues,
        "avg_issues": avg_issues,

        "resolved_percent": resolved_percent,
        "not_resolved_percent": not_resolved_percent,

        "avg_open_close_min": avg_open_close_min,
        "avg_in_progress_min": avg_in_progress_min,
        "avg_handling_time_min": avg_handling_time_min,

        # полезно для разборов (если нужно)
        "resolved": _to_int(res0.get("resolved")),
        "not_resolved": _to_int(res0.get("not_resolved")),
        "client_side_reason": _to_int(res0.get("client_side_reason")),
        "infrastructure_hardware_issue": _to_int(res0.get("infrastructure_hardware_issue")),
        "ticket_cancel": _to_int(res0.get("ticket_cancel")),
    }

    context.log.info(
        "jira_second_line_month_kpis_op: "
        f"avg_issues={out['avg_issues']} "
        f"resolved%={out['resolved_percent']} not_resolved%={out['not_resolved_percent']} "
        f"open_close={out['avg_open_close_min']} in_progress={out['avg_in_progress_min']} aht={out['avg_handling_time_min']}"
    )
    return out

@op(required_resource_keys={"report_utils", "source_dwh_prod"})
def jira_second_line_daily_data_op(context) -> Dict[str, Any]:
    """
    Аналог:
      $issuesDailyData
      $AverageIssueDurationDailyData (OPEN->CLOSE)
      $AverageInProgressIssuedDurationDailyData (WORK->RESOLVE)
      $AverageHandlingTimeIssuedDurationDailyData (WA video call AHT)
      $jiraHandlingDailyData (resolution count + проценты)
      mainData = leftJoin(date)
    """
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate

    jira = JiraDataGetterNext(
        start_date=start_date,
        end_date=end_date,
        dwh_conn=context.resources.source_dwh_prod,
        jira_saima=context.resources.source_dwh_prod,
        jira='jirasd_saima'
    )
    jira.start_date = start_date
    jira.end_date = end_date
    jira.set_days_trunked_data()

    issues_daily = _as_records(jira.get_created_issues_data_by_types(False))
    avg_open_close_daily = _as_records(jira.get_issues_status_from_created_data_by_types())
    avg_in_progress_daily = _as_records(jira.get_issues_status_from_in_progress_data_by_types(["10101", "13701"]))
    avg_handling_daily = _as_records(
        jira.get_issues_status_from_in_progress_data_by_types(
            ["13701"], False, "avg_handling_time_count", "avg_handling_time_min", "handling_time_sum"
        )
    )
    handling_daily = _as_records(jira.get_issue_by_resolution_count())

    main = left_join_array(issues_daily, avg_open_close_daily, "date")
    main = left_join_array(main, avg_in_progress_daily, "date")
    main = left_join_array(main, avg_handling_daily, "date")

    return {
        "main": main,
        "handling": handling_daily,
    }


@op(required_resource_keys={"report_utils", "source_dwh_prod"})
def jira_second_line_month_data_op(context) -> Dict[str, Any]:
    """
    Аналог:
      $issuesMonthlyData, $issuesResolutionMonthlyData
      avgIssues = total/7
      проценты resolved/not_resolved
      averageOpenCloseIssueDurationMonth, averageInProgress..., averageHandlingTime...
      + linked_jira_issue_avg (для конверсии дальше)
    """
    dates = context.resources.report_utils.get_utils()
    start_8d = dates.Dates[0]  # как в PHP: $this->Dates[0]
    end_date = dates.EndDate

    jira = JiraDataGetterNext(
        start_date=start_8d,
        end_date=end_date,
        dwh_conn=context.resources.source_dwh_prod,
        jira_saima=context.resources.source_dwh_prod,
        jira='jirasd_saima'
    )
    jira.start_date = start_8d
    jira.end_date = end_date
    jira.set_months_trunked_data()

    issues_month = _as_records(jira.get_created_issues_data_by_types(False, True))
    res_month = _as_records(jira.get_issue_by_resolution_count(True))

    # PHP делил на 7 жёстко
    avg_issues = round(_safe_div(float(issues_month[0].get("total", 0) if issues_month else 0), 7), 1)

    not_resolved_percent = round(float(res_month[0].get("not_resolved_percent", 0) if res_month else 0), 1)
    resolved_percent = round(float(res_month[0].get("resolved_percent", 0) if res_month else 0), 1)

    avg_open_close_m = _as_records(jira.get_issues_status_from_created_data_by_types(True))
    avg_in_progress_m = _as_records(jira.get_issues_status_from_in_progress_data_by_types(["10101", "13701"], True))
    avg_handling_m = _as_records(
        jira.get_issues_status_from_in_progress_data_by_types(
            ["13701"], True, "avg_handling_time_count", "avg_handling_time_min", "handling_time_sum"
        )
    )

    average_open_close = float(avg_open_close_m[0].get("avg_open_close_min", 0) if avg_open_close_m else 0)
    average_in_progress = float(avg_in_progress_m[0].get("avg_in_progress_min", 0) if avg_in_progress_m else 0)
    average_handling_time = float(avg_handling_m[0].get("avg_handling_time_min", 0) if avg_handling_m else 0)

    # для конверсии: linked_jira_issue_avg берётся через daysTrunkedData (как в PHP)
    jira2 = JiraDataGetterNext(
        start_date=start_8d,
        end_date=end_date,
        dwh_conn=context.resources.source_dwh_prod,
        jira_saima=context.resources.source_dwh_prod,
        jira='jirasd_saima'
    )
    jira2.StartDate = start_8d
    jira2.EndDate = end_date
    jira2.set_days_trunked_data()
    linked_avg = _as_records(jira2.get_linked_jira_issues_by_date(True))  # avg

    return {
        "issues_month": issues_month,
        "resolution_month": res_month,
        "avg_issues": avg_issues,
        "resolved_percent": resolved_percent,
        "not_resolved_percent": not_resolved_percent,
        "average_open_close_min": average_open_close,
        "average_in_progress_min": average_in_progress,
        "average_handling_time_min": average_handling_time,
        "linked_jira_issue_avg": linked_avg,
    }


@op(required_resource_keys={"report_utils", "source_dwh", "source_dwh_prod", "source_cp"})
def omni_conversion_daily_data_op(context, jira_daily: Dict[str, Any]) -> Dict[str, Any]:
    """
    Аналог блока:
      linked_jira_issue по дням (jira)
      $data = OmniDataGetter->get_all_aggregated_requests_new()()
      для каждого дня: row->linked_jira_issue = jira; row->total = sum(4 компонентов)
    """
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate

    omni = OmniDataGetter(conn=context.resources.source_cp)
    omni.start_date = start_date
    omni.end_date = end_date
    omni.set_days_trunked_data()

    jira = JiraDataGetterNext(
        start_date=start_date,
        end_date=end_date,
        dwh_conn=context.resources.source_dwh_prod,
        jira_saima=context.resources.source_dwh_prod,
        jira='jirasd_saima'
    )
    jira.start_date = start_date
    jira.end_date = end_date
    jira.set_days_trunked_data()

    linked = _as_records(jira.get_linked_jira_issues_by_date())  # per day
    linked_map = {_date10(i.get("date")): int(i.get("linked_jira_issue", 0)) for i in linked}

    data = _as_records(omni.get_all_aggregated_requests_new())

    out: List[Dict[str, Any]] = []
    for r in data:
        d = _date10(r.get("date"))
        r["linked_jira_issue"] = int(linked_map.get(d, 0))

        # PHP: total = created_other_jira_issue + linked_jira_issue + closed_by_first_line + closed_by_second_line
        r["total"] = (
            int(r.get("created_other_jira_issue", 0))
            + int(r.get("linked_jira_issue", 0))
            + int(r.get("closed_by_first_line", 0))
            + int(r.get("closed_by_second_line", 0))
        )
        out.append(r)

    return {"data": out}


@op(required_resource_keys={"report_utils", "source_dwh", "source_dwh_prod", "source_cp"})
def omni_conversion_avg_data_op(context) -> Dict[str, Any]:
    """
    Аналог:
      $avgData = Omni->get_all_aggregated_requests_new()(true) (месячное/среднее)
    """
    dates = context.resources.report_utils.get_utils()
    start_8d = dates.Dates[0]
    end_date = dates.EndDate

    omni = OmniDataGetter(conn=context.resources.source_cp)
    omni.start_date = start_8d
    omni.end_date = end_date
    omni.set_months_trunked_data()

    avg_data = _as_records(omni.get_all_aggregated_requests_new(True))
    return {"avg": avg_data}


@op(required_resource_keys={"report_utils", "source_dwh_prod"})
def jira_video_call_reasons_daily_op(context) -> List[Dict[str, Any]]:
    """
    Аналог:
      $reasonsDailyData = jira->getIssues() на диапазоне Dates[0]..EndDate
    """
    dates = context.resources.report_utils.get_utils()
    start_8d = dates.Dates[0]
    end_date = dates.EndDate

    jira = JiraDataGetterNext(
        start_date=start_8d,
        end_date=end_date,
        dwh_conn=context.resources.source_dwh_prod,
        jira_saima=context.resources.source_dwh_prod,
        jira='jirasd_saima'
    )
    jira.start_date = start_8d
    jira.end_date = end_date
    jira.set_days_trunked_data()

    get_dagster_logger().info(f"data: {_as_records(jira.get_issues())}")

    return _as_records(jira.get_issues())


@op(required_resource_keys={"report_utils", "source_dwh_prod"})
def jira_resolution_breakdown_daily_op(context) -> List[Dict[str, Any]]:
    """
    Аналог:
      $resolutionDailyData = jira->getIssueBytResolution()
      дальше в PHP группировка по status + title и назначение id
    """
    dates = context.resources.report_utils.get_utils()
    start_8d = dates.Dates[0]
    end_date = dates.EndDate

    jira = JiraDataGetterNext(
        start_date=start_8d,
        end_date=end_date,
        dwh_conn=context.resources.source_dwh_prod,
        jira_saima=context.resources.source_dwh_prod,
    )
    jira.start_date = start_8d
    jira.end_date = end_date
    jira.set_days_trunked_data()

    return _as_records(jira.get_issue_by_resolution())

@op(
    required_resource_keys={"report_utils"},
    ins={"file_path": In(str)},
    out=Out(dict),
)
def upload_report_instance_saima_second_line_op(context, file_path: str) -> Dict[str, Any]:
    cfg = context.op_config or {}

    dates = context.resources.report_utils.get_utils()
    rd = dates.ReportDate
    if isinstance(rd, str):
        report_day = datetime.strptime(rd[:10], "%Y-%m-%d").date()
    elif isinstance(rd, datetime):
        report_day = rd.date()
    else:
        report_day = rd  # date

    day_str = report_day.strftime("%Y.%m.%d")

    tz_name = cfg.get("timezone") or os.getenv("REPORTS_TZ") or "Asia/Bishkek"
    context.log.info(f"День отчёта: {day_str} (from report_utils.ReportDate, TZ={tz_name})")

    base_url = cfg.get("base_url") or os.getenv("REPORTS_API_BASE", "https://rv.o.kg")
    report_id = int(cfg.get("report_id", 13))
    api_key = cfg.get("api_key") or os.getenv("REPORTS_API_KEY") or os.getenv("RV_REPORTS_API_KEY")
    if not api_key:
        raise Failure("Не задан API ключ (REPORTS_API_KEY / RV_REPORTS_API_KEY или op_config.api_key).")

    timeout_connect = int(cfg.get("timeout_connect", 10))
    timeout_read = int(cfg.get("timeout_read", 120))
    verify_ssl = bool(cfg.get("verify_ssl", True))

    title = cfg.get("title") or "SAIMA | Статистика по 2 линии поддержки"

    url = f"{base_url.rstrip('/')}/api/report-instances/upload/{report_id}"
    headers = {"x-api-key": api_key}
    data = {"title": title, "day": day_str}

    try:
        with open(file_path, "rb") as f:
            files = {"file": (os.path.basename(file_path), f, "application/pdf")}
            resp = requests.post(url, headers=headers, data=data, files=files,
                                 timeout=(timeout_connect, timeout_read), verify=verify_ssl)
    except Exception as e:
        return {"status_code": -1, "text": str(e)}

    try:
        return resp.json()
    except Exception:
        return {"status_code": resp.status_code, "text": resp.text}