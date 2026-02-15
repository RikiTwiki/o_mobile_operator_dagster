# config_loader.py
import os
from typing import Any, Optional
from dotenv import load_dotenv

load_dotenv()  # подхватываем .env из корня проекта

def env(name: str, default: Optional[str] = None, *, cast: type | None = None) -> Any:
    val = os.getenv(name, None)
    if val is None:
        val = default
    if cast is None or val is None:
        return val
    if cast is bool:
        s = str(val).strip().lower()
        return s in {"1", "true", "on", "yes", "y"}
    return cast(val)

CONFIG = {
    "jira": {
        "connections": {
            "jirasd_nur": {
                "host": env("JIRA_HOST", "https://sd.o.kg"),
                "api_prefix": env("JIRA_API_PREFIX", "/rest/api/2"),
                "username": env("JIRA_USERNAME", "nod"),
                "password": env("JIRA_PASSWORD", "iAmNotAGay"),
                    "contact_number_field_number": env("JIRA_CONTACT_NUMBER_FIELD_NUMBER", 10502, cast=int),
                "resolution_customfield_number": env("JIRA_RESOLUTION_FIELD_NUMBER", 10302, cast=int),
                "resolution_field_number": env("JIRA_RESOLUTION_FIELD_NUMBER", 10304, cast=int),
                "db": "jira_nur",
                "project_keys": ["SC", "COVER", "CSA", "MP", "MS", "NOTIF", "TERM", "NT", "PAYM", "AG", "OFD"],
                "responsible_groups": [
                    "qa_qualitycontrol",
                    "rnq",
                    "fraud",
                    "tmg",
                    "other",
                    "backoffice",
                    "terminal_group",
                    "vas",
                    "dengi_sales",
                    "skp_sales",
                    "dengi_otmena",
                    "dsr_test",
                    "scan_oper_bishkek",
                ],
                "sla_customfield_number_general": 10700,
                "sla_customfield_number_secondary": 11904,
                "closed_issue_title": "Закрытый",
                "modified_time": False,
            },
            "jirasd_saima": {
                "host": env("JIRA_HOST", "https://jirasd.saima.kg"),
                "api_prefix": env("JIRA_API_PREFIX", "/rest/api/2"),
                "username": env("JIRA_USERNAME", "nod"),
                "password": env("JIRA_PASSWORD", "qweqwe!@#!@#nod"),
                "contact_number_field_number": env("JIRA_CONTACT_NUMBER_FIELD_NUMBER", 10200, cast=int),
                "resolution_customfield_number": env("JIRA_RESOLUTION_FIELD_NUMBER", 10223, cast=int),
                "resolution_field_number": env("JIRA_RESOLUTION_FIELD_NUMBER", 10005, cast=int),
                "db": "jira_saima",
                "project_keys": ["CS", "MS"],
                "responsible_groups": [],
                "sla_customfield_number_general": 10121,
                "sla_customfield_number_secondary": 10312,
                "closed_issue_title": "Закрытый",
                "modified_time": False,
            },
        }
    }
}

def config(path: str, default: Any = None, **fmt) -> Any:
    key = path.format(**fmt)
    node: Any = CONFIG
    for part in key.split("."):
        if isinstance(node, dict) and part in node:
            node = node[part]
        else:
            return default
    return node
