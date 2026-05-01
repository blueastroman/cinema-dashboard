from __future__ import annotations

import json
import urllib.request
from dataclasses import dataclass


ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_VERSION = "2023-06-01"


@dataclass
class AnthropicReviewClient:
    api_key: str
    model: str

    def send(self, *, system_prompt: str, content: str, max_tokens: int = 4000) -> list[dict]:
        payload = {
            "model": self.model,
            "max_tokens": max_tokens,
            "system": system_prompt,
            "messages": [{"role": "user", "content": content}],
        }
        request = urllib.request.Request(
            ANTHROPIC_API_URL,
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "x-api-key": self.api_key,
                "anthropic-version": ANTHROPIC_VERSION,
            },
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=120) as response:
            result = json.loads(response.read().decode("utf-8"))
        return self.parse_json_array(result["content"][0]["text"])

    @staticmethod
    def parse_json_array(text: str) -> list[dict]:
        clean = str(text or "").replace("```json", "").replace("```", "").strip()
        parsed = json.loads(clean)
        if not isinstance(parsed, list):
            raise ValueError("Anthropic response was not a JSON array.")
        return parsed
