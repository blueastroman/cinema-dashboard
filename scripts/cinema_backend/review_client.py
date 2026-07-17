from __future__ import annotations

import json
from dataclasses import dataclass

from anthropic import Anthropic


@dataclass
class AnthropicReviewClient:
    api_key: str
    model: str

    def send(self, *, system_prompt: str, content: str, max_tokens: int = 4000) -> list[dict]:
        client = Anthropic(api_key=self.api_key)
        response = client.messages.create(
            model=self.model,
            max_tokens=max_tokens,
            system=system_prompt,
            messages=[{"role": "user", "content": content}],
        )
        return self.parse_json_array(response.content[0].text)

    @staticmethod
    def parse_json_array(text: str) -> list[dict]:
        clean = str(text or "").replace("```json", "").replace("```", "").strip()
        parsed = json.loads(clean)
        if not isinstance(parsed, list):
            raise ValueError("Anthropic response was not a JSON array.")
        return parsed
