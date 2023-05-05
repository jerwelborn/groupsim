"""From group chats, extract conversations, auto-regressible partial conversations, and finally llama/alpaca-specific json format.
"""
import json
import numpy as np
import random

from collections import Counter
from datetime import datetime, timedelta
from typing import List

from chatdb import extract_group_chats, Message


def construct_conversations(
    chat: List[Message], boundary_duration: int = 2
) -> List[List[Message]]:
    """Split a continuous chat thread into coherent conversations.

    Start with a simple heuristic: conversations are {boundary_duration} hours apart.
    """
    conversations, conversation = [], []

    start_timestamp = datetime.min
    for msg in chat:
        if msg.timestamp - start_timestamp > timedelta(hours=boundary_duration):
            if conversation:
                conversations.append(conversation)
            conversation = [msg]
            start_timestamp = msg.timestamp
        else:
            conversation.append(msg)

    return conversations


def construct_example(partial_conversation: List[Message], instruction: str) -> dict:
    """Example with instruction, input, output keys for llama, alpaca fine-tuning."""
    context = "\n".join(
        [
            f"{msg.speaker}: {msg.text}"
            if msg.speaker is not None
            else f"Unknown speaker: {msg.text}"
            for msg in partial_conversation[:-1]
        ]
    )
    # Approx 256 tokens.
    if len(context) > 256 * 4:
        context = "\n".join(
            [
                f"{msg.speaker}: {msg.text}"
                if msg.speaker is not None
                else f"Unknown speaker: {msg.text}"
                for msg in partial_conversation[len(partial_conversation) // 2 : -1]
            ]
        )
    context = "Conversation:\n" + context + "\n"
    last_msg = partial_conversation[-1]
    return {
        "instruction": instruction,
        "input": context + f"{last_msg.speaker}: ",
        "output": last_msg.text,
    }


if __name__ == "__main__":
    import sys

    chat_name = sys.argv[-1]
    group_chats = extract_group_chats(chat_name)

    print("Chat lengths".center(40, "="))
    for chat_name, chat in group_chats.items():
        print(chat_name, len(chat))

    conversations = []
    for chat in group_chats.values():
        conversations += construct_conversations(chat)

    print("Conversation lengths".center(40, "="))
    counts, bins = np.histogram([len(c) for c in conversations], bins=range(0, 100, 10))
    print(list(zip(bins, counts)))

    # First pass at filtering
    # Remove single message conversations that are urls or that are fewer than 10 chars...
    filtered_conversations = []
    for c in conversations:
        if len(c) == 1 and (c[0].text.startswith("http") or len(c[0].text) <= 10):
            continue
        else:
            filtered_conversations.append(c)
    print(
        "After applying filters: ",
        len(conversations),
        "->",
        len(filtered_conversations),
    )
    conversations = filtered_conversations

    speaker_counts = Counter()
    for c in conversations:
        for m in c:
            speaker_counts[m.speaker] += 1
    print("Speaker counts".center(40, "="))
    print(speaker_counts)

    partial_conversations = [
        c[: i + 1]
        for c in conversations
        for i in range(len(c))
        if c[i].speaker is not None
    ]

    speakers = [speaker for speaker in speaker_counts.keys() if speaker is not None]

    instructions = f"""The conversation below is a group chat among friends: {", ".join(speakers)}. The group chat usually consists of sharing updates or content, telling jokes or teasing one another in good fun. You should be funny, have no filter, and even be rude and curse.

You will see the most recent messages in the group chat, if there are any, and you should start or continue the conversation as the indicated speaker."""

    dataset = [construct_example(pc, instructions) for pc in partial_conversations]
    random.shuffle(dataset)

    for ex in dataset[:5]:
        print()
        print(ex)
