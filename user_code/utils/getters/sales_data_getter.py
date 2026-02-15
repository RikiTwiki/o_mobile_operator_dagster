from pathlib import Path

import pandas as pd
from dagster import get_dagster_logger

from utils.path import BASE_SQL_PATH

from typing import Any, Dict, List, Sequence, Tuple


class SalesDataGetter:
    def __init__(self, start_date, end_date, dwh_conn, naumen_conn=None, report_date=None):
        self.start_date = start_date
        self.end_date = end_date
        self.report_date = report_date
        self.dwh_conn = dwh_conn
        self.naumen_conn = naumen_conn
        self.base_sql_path = Path(BASE_SQL_PATH)

    def get_aggregated_sales_data(self):
        sql_path = self.base_sql_path / "dwh" / "get_aggregated_sales_data.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
        }

        df = pd.read_sql_query(query, self.dwh_conn, params=params)

        data = (
            df.rename(columns={"day": "date"})
            .assign(value=lambda x: x["value"].fillna(0).astype(int))
            .to_dict("records")
        )

        return data

    def get_aggregated_transitions_data(self):
        sql_path = self.base_sql_path / "dwh" / "get_agggregated_transactions_data.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
        }

        df = pd.read_sql_query(query, self.dwh_conn, params=params)

        data = (
            df.rename(columns={"day": "date"})
            .assign(value=lambda x: x["value"].fillna(0).astype(int))
            .to_dict("records")
        )

        return data

    def get_aggregated_transitions_data_for_report_day(self):
        sql_path = self.base_sql_path / "dwh" / "get_aggregated_transitions_data_for_report_day.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {
            "report_date": self.report_date,
        }

        df = pd.read_sql_query(query, self.dwh_conn, params=params)

        data = (
            df.assign(value=lambda x: x["value"].fillna(0).astype(int))
            .rename(columns={"day": "date"})
            .to_dict("records")
        )

        return data

    def get_aggregated_sales_data_by_month(self):
        sql_path = self.base_sql_path / "dwh" / "get_aggregated_sales_data_by_month.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
        }

        df = pd.read_sql_query(query, self.dwh_conn, params=params)

        data = (
            df.assign(value=lambda x: x["value"].fillna(0).astype(int))
            .to_dict("records")
        )

        return data

    def get_personal_activation_data(self):
        sql_path = self.base_sql_path / "dwh" / "personal_activation_data.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
        }

        df = pd.read_sql_query(query, self.dwh_conn, params=params)

        data = (df.assign(value=lambda x: x["value"].fillna(0).astype(int))
                .rename(columns={"day": "date"})
                .to_dict("records"))

        return data

    def get_sales_profit_data(self):
        sql_path = self.base_sql_path / "dwh" / "sales_profit_data.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
        }

        df = pd.read_sql_query(query, self.dwh_conn, params=params)

        data = (
            df.rename(columns={"day": "date"})
            .assign(value=lambda x: x["value"].fillna(0).astype(int))
            .to_dict("records")
        )
        return data

    def get_sales_renewal_data(self):
        sql_path = self.base_sql_path / "dwh" / "sales_renewal_data.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
        }

        df = pd.read_sql_query(query, self.dwh_conn, params=params)

        data = (
            df.rename(columns={"day": "date"})
            .assign(value=lambda x: x["value"].fillna(0).astype(int))
            .to_dict("records")
        )
        return data

    def get_total_aspu_renewal_data(self):
        sql_path = self.base_sql_path / "dwh" / "total_aspu_renewal_data.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {"start_date": self.start_date, "end_date": self.end_date}

        df = pd.read_sql_query(query, self.dwh_conn, params=params)

        data = (
            df.rename(columns={"day": "date"})
            .assign(value=lambda x: x["value"].fillna(0).astype(int))
            .to_dict("records")
        )
        return data

    def get_sales_payments_data(self):
        sql_path = self.base_sql_path / "dwh" / "sales_payments_data.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {"start_date": self.start_date, "end_date": self.end_date}
        df = pd.read_sql_query(query, self.dwh_conn, params=params)

        data = (
            df.rename(columns={"day": "date"})
            .assign(value=lambda x: x["value"].fillna(0).astype(int))
            .to_dict("records")
        )
        return data

    def get_sales_activation_data(self):
        sql_path = self.base_sql_path / "dwh" / "sales_activation_data.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {"start_date": self.start_date, "end_date": self.end_date}
        df = pd.read_sql_query(query, self.dwh_conn, params=params)

        data = (
            df.rename(columns={"day": "date"})
            .assign(value=lambda x: x["value"].fillna(0).astype(int))
            .to_dict("records")
        )
        return data

    def get_total_aspu_activation_data(self):
        sql_path = self.base_sql_path / "dwh" / "total_aspu_activation_data.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {"start_date": self.start_date, "end_date": self.end_date}
        df = pd.read_sql_query(query, self.dwh_conn, params=params)

        data = (
            df.rename(columns={"day": "date"})
            .to_dict("records")
        )
        return data

    def get_sales_status_data(self):
        sql_path = self.base_sql_path / "dwh" / "sales_status_data.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {"start_date": self.start_date, "end_date": self.end_date}
        df = pd.read_sql_query(query, self.dwh_conn, params=params)

        data = (
            df.rename(columns={"day": "date"})
            .to_dict("records")
        )
        return data

    def get_aggregated_count_sales_last_30_days(self):
        sql_path = self.base_sql_path / "dwh" / "aggregated_count_sales_last_30_days.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {"start_date": self.start_date, "end_date": self.end_date}
        df = pd.read_sql_query(query, self.dwh_conn, params=params)

        data = (
            df.rename(columns={"day": "date"})
            .to_dict("records")
        )
        return data

    def get_sales_profit_graphic(self):
        sql_path = self.base_sql_path / "dwh" / "sales_profit_graphic.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {"start_date": self.start_date, "end_date": self.end_date}
        df = pd.read_sql_query(query, self.dwh_conn, params=params)

        data = (
            df.rename(columns={"day": "date", "value": "test"})
              .to_dict("records")
        )
        return data

    def get_sales_profit_graphic_by_month(self):
        sql_path = self.base_sql_path / "dwh" / "sales_profit_graphic_by_month.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {"start_date": self.start_date, "end_date": self.end_date}
        df = pd.read_sql_query(query, self.dwh_conn, params=params)

        data = (
            df.rename(columns={"day": "date", "value": "value"})
            .to_dict("records")
        )
        return data

    def get_sales_renewal_graphic(self):
        sql_path = self.base_sql_path / "dwh" / "sales_renewal_graphic.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {"start_date": self.start_date, "end_date": self.end_date}
        df = pd.read_sql_query(query, self.dwh_conn, params=params)

        data = (
            df.rename(columns={"day": "date", "value": "value"})
            .to_dict("records")
        )
        return data

    def get_sales_activation_graphic(self):
        sql_path = self.base_sql_path / "dwh" / "sales_activation_graphic.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {"start_date": self.start_date, "end_date": self.end_date}
        df = pd.read_sql_query(query, self.dwh_conn, params=params)

        data = (
            df.rename(columns={"day": "date", "value": "value"})
            .to_dict("records")
        )
        return data

    def get_total_aspu_data(self):
        sql_path = self.base_sql_path / "dwh" / "total_aspu_data.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {"start_date": self.start_date, "end_date": self.end_date}
        df = pd.read_sql_query(query, self.dwh_conn, params=params)

        data = (
            df.rename(columns={"day": "date"})
            .to_dict("records")
        )
        return data

    def get_personal_aggregated_sales_data(self):
        sql_path = self.base_sql_path / "dwh" / "personal_aggregated_sales_data.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {"start_date": self.start_date, "end_date": self.end_date}
        df = pd.read_sql_query(query, self.dwh_conn, params=params)

        data = (
            df.rename(columns={"day": "date",
                               "user_name": "user_name",
                               "value": "value"})
            .to_dict("records")
        )
        return data

    def get_bonus(self):
        sql_path = self.base_sql_path / "dwh" / "bonus.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {"start_date": self.start_date, "end_date": self.end_date}
        df = pd.read_sql_query(query, self.dwh_conn, params=params)

        data = (
            df.rename(columns={"day": "date", "name": "name", "value": "value"})
            .to_dict("records")
        )
        return data

    def get_bonus_graphic(self):
        sql_path = self.base_sql_path / "dwh" / "bonus_graphic.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {"start_date": self.start_date, "end_date": self.end_date}
        df = pd.read_sql_query(query, self.dwh_conn, params=params)

        data = (
            df.rename(columns={"day": "date", "value": "value"})
            .to_dict("records")
        )
        return data

    def get_mistakes_data(self):
        sql_path = self.base_sql_path / "dwh" / "mistakes_data.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {"start_date": self.start_date, "end_date": self.end_date}
        df = pd.read_sql_query(query, self.dwh_conn, params=params)

        data = (
            df.rename(columns={"day": "date"})
            .to_dict("records")
        )
        return data

    def get_renewal_status_data(self):
        sql_path = self.base_sql_path / "dwh" / "renewal_status_data.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {"start_date": self.start_date, "end_date": self.end_date}
        df = pd.read_sql_query(query, self.dwh_conn, params=params)

        data = (
            df.rename(columns={"day": "date"})
            .to_dict("records")
        )
        return data

    def get_first_price_plan(self):
        sql_path = self.base_sql_path / "dwh" / "first_price_plan.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {"start_date": self.start_date, "end_date": self.end_date}
        df = pd.read_sql_query(query, self.dwh_conn, params=params)

        data = (
            df.rename(columns={"day": "date"})
            .to_dict("records")
        )
        return data

    def get_aspu_lift_data(self):
        sql_path = self.base_sql_path / "dwh" / "aspu_lift_data.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {"start_date": self.start_date, "end_date": self.end_date}
        df = pd.read_sql_query(query, self.dwh_conn, params=params)

        data = (
            df.rename(columns={"day": "date", "up": "up", "down": "down"})
            .to_dict("records")
        )
        return data

    def get_aspu_lift_data_by_month(self):
        sql_path = self.base_sql_path / "dwh" / "aspu_lift_data_by_month.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {"start_date": self.start_date, "end_date": self.end_date}
        df = pd.read_sql_query(query, self.dwh_conn, params=params)

        data = (
            df.rename(columns={"month": "month", "up": "up", "down": "down"})
            .to_dict("records")
        )
        return data

    def get_detailed_sales_conversion(self):
        # --- читаем продажи из DWH ---
        dwh_sql = (self.base_sql_path / "dwh" / "detailed_sales_conversion.sql").read_text(encoding="utf-8")
        dwh_df = pd.read_sql_query(dwh_sql, self.dwh_conn,
                                   params={"start_date": self.start_date, "end_date": self.end_date})

        # --- читаем звонки из Naumen ---
        naumen_sql = (self.base_sql_path / "naumen" / "detailed_sales_conversion.sql").read_text(encoding="utf-8")
        naumen_df = pd.read_sql_query(naumen_sql, self.naumen_conn,
                                      params={"start_date": self.start_date, "end_date": self.end_date})

        # --- объединяем по дате ---
        merged = pd.merge(dwh_df, naumen_df, on="date", how="inner")

        # --- считаем конверсию ---
        merged["conversion"] = ((merged["sales"] / merged["calls"]) * 100).round()

        # --- отдаём результат ---
        data = merged[["date", "conversion"]].rename(columns={"conversion": "test"}).to_dict("records")
        return data

    def get_detailed_sales_conversion_by_month(self):
        # --- продажи по месяцам из DWH ---
        dwh_sql_path = self.base_sql_path / "dwh" / "detailed_sales_conversion_by_month.sql"
        dwh_sql = dwh_sql_path.read_text(encoding="utf-8")
        dwh_df = pd.read_sql_query(
            dwh_sql, self.dwh_conn,
            params={"start_date": self.start_date, "end_date": self.end_date}
        )

        # --- звонки по месяцам из Naumen ---
        naumen_sql_path = self.base_sql_path / "naumen" / "detailed_sales_conversion_by_month.sql"
        naumen_sql = naumen_sql_path.read_text(encoding="utf-8")
        naumen_df = pd.read_sql_query(
            naumen_sql, self.naumen_conn,
            params={"start_date": self.start_date, "end_date": self.end_date}
        )

        # --- inner join по месяцу и расчёт конверсии ---
        merged = pd.merge(dwh_df, naumen_df, on="date", how="inner")
        merged["value"] = ((merged["sales"] / merged["calls"]) * 100).round()

        # --- итоговый вид как в PHP: [{"date", "value"}] ---
        return merged[["date", "value"]].to_dict("records")

    def get_connections(self) -> List[Dict[str, Any]]:
        sql_path = self.base_sql_path / "dwh" / "connections.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {"start_date": self.start_date, "end_date": self.end_date}
        df = pd.read_sql_query(query, self.dwh_conn, params=params)

        # Приводим структуру к PHP-результату: date, value, unit="ед."
        data = (
            df.rename(columns={"day": "date", "connections": "value"})
            .assign(unit="ед.")
            .to_dict("records")
        )
        return data

    def get_detailed_sales_conversion_new(self) -> List[Dict[str, Any]]:
        log = get_dagster_logger()

        try:
            # 1) Подключения из DWH
            dwh_sql = (self.base_sql_path / "dwh" / "detailed_sales_conversion_new.sql").read_text(encoding="utf-8")
            dwh_df = pd.read_sql_query(
                dwh_sql,
                self.dwh_conn,
                params={"start_date": self.start_date, "end_date": self.end_date},
            )  # ожидаем колонки: date/DATE/day/DAY и connections/CONNECTIONS

            # 2) Дозвоны из Naumen
            calls_sql = (self.base_sql_path / "naumen" / "detailed_sales_calls_new.sql").read_text(encoding="utf-8")
            calls_df = pd.read_sql_query(
                calls_sql,
                self.naumen_conn,
                params={"start_date": self.start_date, "end_date": self.end_date},
            )  # ожидаем колонки: date/DATE/day/DAY и calls/CALLS

            # --- Нормализация колонок
            def _pick(colnames, opts, dfname):
                for c in opts:
                    if c in colnames:
                        return c
                log.error(f"[sales_conv] {dfname}: нет ни одной из колонок {opts}. cols={list(colnames)}")
                raise KeyError(f"{dfname}: missing columns {opts}")

            dwh_day_col = _pick(dwh_df.columns, ["DAY", "day", "date", "DATE"], "dwh_df")
            dwh_conn_col = _pick(dwh_df.columns, ["CONNECTIONS", "connections"], "dwh_df")
            calls_day_col = _pick(calls_df.columns, ["DAY", "day", "date", "DATE"], "calls_df")
            calls_col = _pick(calls_df.columns, ["calls", "CALLS"], "calls_df")

            dwh_df = dwh_df.rename(columns={dwh_day_col: "DAY", dwh_conn_col: "CONNECTIONS"})
            calls_df = calls_df.rename(columns={calls_day_col: "DAY", calls_col: "calls"})

            # --- Приводим даты к ISO YYYY-MM-DD
            dwh_df["DAY"] = pd.to_datetime(dwh_df["DAY"], errors="coerce").dt.strftime("%Y-%m-%d")
            calls_df["DAY"] = pd.to_datetime(calls_df["DAY"], errors="coerce").dt.strftime("%Y-%m-%d")

            # Если в датах есть NaN (разбор не удался) — возвращаем пусто
            if dwh_df["DAY"].isna().any():
                log.error(
                    f"[sales_conv] invalid dates in dwh_df: {dwh_df[dwh_df['DAY'].isna()].to_dict('records')[:3]}")
                return []
            if not calls_df.empty and calls_df["DAY"].isna().any():
                log.error(
                    f"[sales_conv] invalid dates in calls_df: {calls_df[calls_df['DAY'].isna()].to_dict('records')[:3]}")
                return []

            # 3) Левый джойн по дням; если звонков нет — 0
            merged = pd.merge(
                dwh_df[["DAY", "CONNECTIONS"]],
                calls_df[["DAY", "calls"]] if not calls_df.empty else pd.DataFrame(columns=["DAY", "calls"]),
                on="DAY",
                how="left",
            )
            merged["calls"] = merged["calls"].fillna(0)

            # 4) Конверсия: round(connections / calls * 100), если calls==0 → 0
            merged["value"] = merged.apply(
                lambda r: 0 if r["calls"] == 0 else round((float(r["CONNECTIONS"]) / float(r["calls"])) * 100),
                axis=1,
            )

            # 5) Итог
            out = (
                merged[["DAY", "value"]]
                .rename(columns={"DAY": "date"})
                .assign(unit="%")
                .to_dict("records")
            )
            return out

        except Exception as e:
            log.error(f"[sales_conv] failed: {e}", exc_info=True)
            return []