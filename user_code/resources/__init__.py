#resources.__init__.py
import os
from dagster import resource, Field, String
import psycopg2
from dagster._core.storage.fs_io_manager import fs_io_manager

from resources.mp_daily_report_config import MPDailyReportConfigResource
from resources.report_utils import ReportUtils

from resources.dwh import OracleDWHResource

io_manager_resource = fs_io_manager.configured({
    "base_dir": "/opt/dagster/storage"
})

@resource(config_schema={
    "host": Field(String, description="Postgres host"),
    "port": Field(int, is_required=False, default_value=5432, description="Postgres port"),
    "username": Field(String, description="Postgres username"),
    "password": Field(String, description="Postgres password"),
    "database": Field(String, description="Postgres database"),
})
def postgres_resource(context):
    config = context.resource_config
    conn = psycopg2.connect(
        host=config["host"],
        port=config.get("port", 5432),
        user=config["username"],
        password=config["password"],
        dbname=config["database"],
        options="-c timezone=Asia/Bishkek",
    )
    try:
        yield conn
    finally:
        conn.close()

# Ресурс для CP
source_cp_resource = postgres_resource.configured({
    "host": os.getenv("DB_CP_HOST"),
    "port": int(os.getenv("DB_CP_PORT", 7504)),
    "username": os.getenv("DB_CP_USERNAME"),
    "password": os.getenv("DB_CP_PASSWORD"),
    "database": os.getenv("DB_CP_DATABASE"),
})

# Ресурс для DWH
source_dwh_resource = postgres_resource.configured({
    "host": os.getenv("CC_DWH_DB_HOST"),
    "port": int(os.getenv("CC_DWH_DB_PORT", 5432)),
    "username": os.getenv("CC_DWH_DB_USER"),
    "password": os.getenv("CC_DWH_DB_PASSWORD"),
    "database": os.getenv("CC_DWH_DB_NAME"),
})

# Ресурс для DWH_prod
source_dwh_resource_prod = postgres_resource.configured({
    "host": os.getenv("CC_DWH_DB_HOST_PROD"),
    "port": int(os.getenv("CC_DWH_DB_PORT_PROD", 5432)),
    "username": os.getenv("CC_DWH_DB_USER_PROD"),
    "password": os.getenv("CC_DWH_DB_PASSWORD_PROD"),
    "database": os.getenv("CC_DWH_DB_NAME_PROD"),
})

# Ресурс для Naumen_1
source_naumen_1_resource = postgres_resource.configured({
    "host": os.getenv("DB_HOST_NAUMEN_1"),
    "port": int(os.getenv("DB_PORT_NAUMEN_1", 5432)),
    "username": os.getenv("DB_USERNAME_NAUMEN_1"),
    "password": os.getenv("DB_PASSWORD_NAUMEN_1"),
    "database": os.getenv("DB_DATABASE_NAUMEN_1"),
})

# Ресурс для Naumen_3
source_naumen_3_resource = postgres_resource.configured({
    "host": os.getenv("DB_HOST_NAUMEN_3"),
    "port": int(os.getenv("DB_PORT_NAUMEN_3", 5432)),
    "username": os.getenv("DB_USERNAME_NAUMEN_3"),
    "password": os.getenv("DB_PASSWORD_NAUMEN_3"),
    "database": os.getenv("DB_DATABASE_NAUMEN_3"),
})

# Ресурс для MP
source_mp_resource = postgres_resource.configured({
    "host": os.getenv("DB_MP_HOST"),
    "port": int(os.getenv("DB_MP_PORT", 8026)),
    "username": os.getenv("DB_MP_USERNAME"),
    "password": os.getenv("DB_MP_PASSWORD"),
    "database": os.getenv("DB_MP_DATABASE"),
})

# Ресурс для DWH_ORACLE
source_dwh_oracle_resource = OracleDWHResource(
    host=os.getenv("DB_HOST_DWH"),
    port=int(os.getenv("DB_PORT_DWH", "1521")),
    service_name=os.getenv("DB_DATABASE_DWH"),
    user=os.getenv("DB_USERNAME_DWH"),
    password=os.getenv("DB_PASSWORD_DWH"),
)

# Ресурс для NOD Dinosaur
source_nod_dinosaur = postgres_resource.configured({
    "host": os.getenv("DB_HOST_DINOSAUR"),
    "port": int(os.getenv("DB_PORT_DINOSAUR", 5432)),
    "username": os.getenv("DB_USERNAME_DINOSAUR"),
    "password": os.getenv("DB_PASSWORD_DINOSAUR"),
    "database": os.getenv("DB_DATABASE_DINOSAUR"),
})

# Ресурс для Jira O!
source_jira_nur = postgres_resource.configured({
    "host": os.getenv("DB_HOST_JIRA_SD"),
    "port": int(os.getenv("DB_PORT_JIRA_SD", 5432)),
    "username": os.getenv("DB_USERNAME_JIRA_SD"),
    "password": os.getenv("DB_PASSWORD_JIRA_SD"),
    "database": os.getenv("DB_DATABASE_JIRA_SD"),
})

# Ресурс для Jira Saima
source_jira_saima = postgres_resource.configured({
    "host": os.getenv("DB_HOST_JIRA_SAIMA"),
    "port": int(os.getenv("DB_PORT_JIRA_SAIMA", 5432)),
    "username": os.getenv("DB_USERNAME_JIRA_SAIMA"),
    "password": os.getenv("DB_PASSWORD_JIRA_SAIMA"),
    "database": os.getenv("DB_DATABASE_JIRA_SAIMA"),
})

# Ресурс для CCDWH
source_ccdwh_resource = postgres_resource.configured({
    "host": os.getenv("CC_CCDWH_DB_HOST"),
    "port": int(os.getenv("CC_CCDWH_DB_PORT", 5432)),
    "username": os.getenv("CC_CCDWH_DB_USER"),
    "password": os.getenv("CC_CCDWH_DB_PASSWORD"),
    "database": os.getenv("CC_CCDWH_DB_NAME"),
})

resources = {

    "source_cp": source_cp_resource,

    "source_dwh": source_dwh_resource,
    "source_dwh_prod": source_dwh_resource_prod,
    "source_naumen_1": source_naumen_1_resource,

    "source_naumen_3": source_naumen_3_resource,

    "source_mp": source_mp_resource,

    "source_dwh_oracle": source_dwh_oracle_resource,
    "source_nod_dinosaur": source_nod_dinosaur,
    "source_jira_nur" : source_jira_nur,
    "source_jira_saima": source_jira_saima,

    "source_ccdwh_resource": source_ccdwh_resource,

    "report_utils": ReportUtils(),
    "mp_daily_config": MPDailyReportConfigResource(),
    "io_manager": fs_io_manager.configured(
      {"base_dir": "/opt/dagster/mp"}
    ),
}
