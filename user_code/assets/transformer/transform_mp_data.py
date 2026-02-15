from datetime import timedelta
import pandas as pd
from dagster import asset, AssetIn
import json
from dateutil import parser
from typing import List, Dict, Any

from utils import str_to_time, calculate_additional_metrics


def decode_field(data, field):
    if not data.get(field):
        return []
    return data[field] if isinstance(data[field], list) else json.loads(data[field])


def reaction_to_seconds(reaction):
    if isinstance(reaction, timedelta):
        return reaction.total_seconds()
    return reaction


def calculate_bot_replies(messages, start_time):
    bots = []
    total_reaction = 0.0
    total_replies = 0
    answered = False

    for msg in messages:
        if msg.get("side") == "client":
            start_time = msg["created_at"]
            answered = False
        elif msg.get("side") == "bot" and not answered:
            reaction = str_to_time(msg["created_at"]) - str_to_time(start_time)
            bots.append({
                "agent_id": msg["user_id"],
                "created_at": msg["created_at"],
                "reply": 1,
                "reaction": reaction
            })
            total_reaction += reaction_to_seconds(reaction)
            total_replies += 1
            answered = True

    return bots, total_reaction, total_replies


def extract_agents(assignies, messages):
    result = []
    legs = []
    for assigny in assignies:
        start_time = assigny['created_at']
        answered = False

        for idx, msg in enumerate(messages):
            if msg.get("side") == "client":
                start_time = msg["created_at"]
                answered = False
            elif msg.get("side") == "user" and not answered:
                if assigny['user_id'] == msg['user_id'] and assigny['created_at'] == msg.get('a_created_at'):
                    reaction = str_to_time(msg["created_at"]) - str_to_time(start_time)
                    legs.append(msg["user_id"])
                    result.append({
                        "agent_id": msg["user_id"],
                        "reply": 1,
                        "reaction": reaction,
                        "created_at": msg.get("a_created_at"),
                        "message_created": msg["created_at"]
                    })
                    answered = True
    return result, legs


def process_chat(chat: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Трансформация одного чата в структурированные данные"""
    messages = decode_field(chat, "messages")
    assignies = decode_field(chat, "assignies")
    bots_meta = decode_field(chat, "bots")

    start_time = messages[0]['created_at'] if messages else None
    chat_holding_time = chat['chat_holding_time'] if chat['chat_holding_time'] else 0

    agents, legs = extract_agents(assignies, messages)
    bots, bot_total_reaction, bot_reply_count = calculate_bot_replies(messages, start_time)

    avg_bot_reaction = (bot_total_reaction / bot_reply_count) if bot_reply_count else 0
    chat_first_reaction = reaction_to_seconds(bots[0]['reaction']) if bots else 0

    results = []

    # Метрики для ботов
    for bot in bots_meta:
        matching_replies = [b for b in bots if b["agent_id"] == bot["user_id"]]
        total_reaction = sum(reaction_to_seconds(r["reaction"]) for r in matching_replies)
        count = len(matching_replies)
        avg = (total_reaction / count) if count else 0
        first = reaction_to_seconds(matching_replies[0]["reaction"]) if matching_replies else 0

        metrics = calculate_additional_metrics(chat, assigny=None)

        results.append({
            "chat_id": chat["id"],
            "chat_finished_at": chat["chat_finished_at"].strftime("%Y-%m-%d %H:%M:%S") if chat.get("chat_finished_at") else None,
            "chat_reaction_time": first,
            "chat_total_reaction_time": bot_total_reaction,
            "chat_count_replies": bot_reply_count,
            "chat_average_replies_time": avg_bot_reaction,
            "type": "bot",
            "project_id": chat["project_id"],
            "split_title": chat["split_title"],
            "is_bot_only": not bool(agents),
            "agent_leg_number": None,
            "user_id": bot["user_id"],
            "user_login": bot["login"],
            "user_reaction_time": first,
            "user_total_reaction_time": total_reaction,
            "user_count_replies": count,
            "user_average_replies_time": avg,
            **metrics
        })

    # Метрики для агентов
    for assigny in assignies:
        replies = [a for a in agents if
                   a['agent_id'] == assigny['user_id'] and a['created_at'] == assigny['created_at']]
        if not replies:
            continue

        total_reaction = sum(reaction_to_seconds(r['reaction']) for r in replies)
        count = sum(1 for r in replies if r['reply'] != 0)
        avg = (total_reaction / count) if count else 0
        metrics = calculate_additional_metrics(chat, assigny=assigny)

        results.append({
            "chat_id": assigny["chat_id"],
            "chat_finished_at": (
                parser.parse(assigny["chat_finished_at"]).strftime("%Y-%m-%d %H:%M:%S")
                if assigny.get("chat_finished_at") else None
            ),
            "chat_reaction_time": chat_first_reaction,
            "chat_total_reaction_time": bot_total_reaction,
            "chat_count_replies": bot_reply_count,
            "chat_average_replies_time": avg_bot_reaction,
            "type": "user",
            "project_id": assigny["project_id"],
            "split_title": assigny["split_title"],
            "is_bot_only": False,
            "closed_by_timeout": assigny.get("closed_by_timeout", False),
            "agent_leg_number": legs.index(assigny["user_id"]) + 1 if assigny["user_id"] in legs else None,
            "user_id": assigny["user_id"],
            "user_login": assigny["login"],
            "user_reaction_time": reaction_to_seconds(replies[0]["reaction"]),
            "user_total_reaction_time": total_reaction,
            "chat_holding_time": chat_holding_time,
            "user_count_replies": count,
            "user_average_replies_time": avg,
            "rate": 1,
            **metrics
        })

    return results


@asset(ins={"raw_data": AssetIn("load_raw_mp_data")})
def transform_mp_data(context, raw_data) -> List[Dict[str, Any]]:
    """Трансформация сырых данных MP в структурированный формат"""
    if isinstance(raw_data, pd.DataFrame):
        raw_data = raw_data.to_dict(orient="records")

    all_results = []

    for chat in raw_data:
        chat_results = process_chat(chat)
        all_results.extend(chat_results)

    if not all_results:
        context.log.info("Нет данных для трансформации")
        return []

    context.log.info(f"Трансформировано записей: {len(all_results)}")
    return all_results
