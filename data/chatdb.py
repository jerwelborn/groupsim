"""Extract group chats from iMessage chat.db
"""
import datetime
import os
import sqlite3

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional


CHATDB_PATH = os.path.join(Path.home(), "Library/Messages/chat.db")


READ_QUERY = """
WITH group_chat_names AS (

    -- Select names for contacts in target group chat.

    -- TODO: I took a shortcut by manually adding names for numbers of interest in the contacts ("handle") table, like:
    -- ALTER TABLE handle ADD name text;
    -- UPDATE handle SET name = 'jeremy' WHERE id LIKE '%123456789%';
    SELECT DISTINCT handle.name
    FROM chat
    JOIN chat_handle_join ON chat_handle_join.chat_id = chat.rowid
    JOIN handle ON handle.rowid = chat_handle_join.handle_id
    WHERE display_name LIKE ?

), group_chat_handles AS (

    -- Select handles for these names. There may be >1 (eg SMS and iMessage).
    SELECT *
    FROM handle
    RIGHT JOIN group_chat_names ON group_chat_names.name = handle.name

), messages AS (

    -- Collect conversations they're in and de-dupe.
    SELECT
        chat.rowid "chatid",
        message.rowid "msgid",
        chat.display_name "chat_name",
        datetime (message.date / 1000000000 + strftime ("%s", "2001-01-01"), "unixepoch", "localtime") AS timestamp,
        message.text,
        message.handle_id,
        -- Ah, so for not my msgs, use handles. But for my msgs, seems is_from_me is the source-of-truth.
        message.is_from_me
    FROM
        group_chat_handles
        JOIN chat_handle_join ON chat_handle_join.handle_id = group_chat_handles.rowid
        JOIN chat ON chat.rowid = chat_handle_join.chat_id
        JOIN chat_message_join ON chat_message_join.chat_id = chat.rowid
        JOIN message ON message.rowid = chat_message_join.message_id
    GROUP BY chatid, msgid

), filtered_messages AS (

    -- Filter out non-group chat msgs and tapback/react msgs.
    SELECT
        chat_name,
        CASE
            WHEN name IS NOT NULL then name
            ELSE
                CASE
                    WHEN messages.is_from_me THEN 'jeremy'
                    ELSE group_chat_handles.id
                END
        END speaker,
        text,
        timestamp
    FROM messages
    LEFT JOIN group_chat_handles ON group_chat_handles.rowid = messages.handle_id
    WHERE
        chat_name != ''
        AND text IS NOT NULL
        -- TODO use this as signal for up-sampling.
        AND text NOT LIKE 'Loved%'
        AND text NOT LIKE 'Liked%'
        AND text NOT LIKE 'Disliked%'
        AND text NOT LIKE 'Laughed at%'
        AND text NOT LIKE 'Emphasized%'
        AND text NOT LIKE 'Questioned%'
        AND text NOT LIKE 'Removed%'

)

SELECT *
FROM filtered_messages
"""


@dataclass
class Message:
    speaker: Optional[str]
    text: str
    timestamp: datetime.datetime


def extract_group_chats(chat_name: str) -> Dict[str, List[Message]]:
    """Extract group chats with speakers from a group chat called chat_name (or like %chat_name%)."""
    if not os.path.exists(CHATDB_PATH):
        raise FileNotFoundError(CHATDB_PATH)

    with sqlite3.connect(CHATDB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(READ_QUERY, [f"%{chat_name}%"])
        result_set = cursor.fetchall()

    chats = defaultdict(list)
    for row in result_set:
        chat_name, speaker, text, raw_timestamp = row
        text = text.strip()
        if speaker is not None:
            speaker = speaker[0].upper() + speaker[1:].lower()
        timestamp = datetime.datetime.fromisoformat(raw_timestamp)

        # Unicode object replacement character
        if any(ord(char) == 65532 for char in text):
            continue

        chats[chat_name].append(Message(speaker, text, timestamp))

    for chat_name, chat in chats.items():
        chats[chat_name] = sorted(chat, key=lambda m: m.timestamp)

    return chats
