"""Build Ollama chat messages from skill + history + user input."""

from __future__ import annotations


def build_messages(
    *, system_prompt: str, history: list[dict], user_body: str
) -> list[dict]:
    """Compose Ollama /api/chat messages array.

    history items have shape {role, content, ...}; only role+content are
    used. Order: [system, ...history (chronological), user (current)].
    """
    msgs: list[dict] = [{"role": "system", "content": system_prompt}]
    for h in history:
        msgs.append({"role": h["role"], "content": h["content"]})
    msgs.append({"role": "user", "content": user_body})
    return msgs
