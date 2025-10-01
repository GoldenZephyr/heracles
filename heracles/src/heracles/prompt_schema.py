import logging

logger = logging.getLogger(__name__)


class Prompt:
    def __init__(
        self,
        system=None,
        in_context_examples_preamble=None,
        in_context_examples=None,
        novel_instruction_preamble=None,
        novel_instruction=None,
        novel_instruction_ps=None,
    ):
        self.system = system
        self.in_context_examples_preamble = in_context_examples_preamble
        self.in_context_examples = in_context_examples
        self.novel_instruction_preamble = novel_instruction_preamble
        self.novel_instruction = novel_instruction
        self.novel_instruction_ps = novel_instruction_ps

    @classmethod
    def from_dict(cls, d, novel_instruction=None):
        system = d.get("system", None)
        icl_pre = d.get("in_context_examples_preamble", None)
        icl_examples = [
            InContextExample.from_dict(e) for e in d.get("in_context_examples", [])
        ]
        novel_instruction_preamble = d.get("novel_instruction_preamble", None)
        novel_instruction_ps = d.get("novel_instruction_ps", None)

        if "novel_instruction" in d:
            novel_instruction = d["novel_instruction"]

        return cls(
            system,
            icl_pre,
            icl_examples,
            novel_instruction_preamble,
            novel_instruction,
            novel_instruction_ps,
        )

    def to_dict(self):
        d = {}
        if self.system:
            d["system"] = self.system
        if self.in_context_examples_preamble:
            d["in_context_examples_preamble"] = self.in_context_examples_preamble
        if self.in_context_examples:
            d["in_context_examples"] = self.in_context_examples
        if self.novel_instruction_preamble:
            d["novel_instruction_preamble"] = self.novel_instruction_preamble
        if self.novel_instruction:
            d["novel_instruction"] = self.novel_instruction
        if self.novel_instruction_ps:
            d["novel_instruction_ps"] = self.novel_instruction_ps

        return d

    def to_openai_json(self, novel_instruction):
        prompt = []

        if self.system:
            prompt.append({"role": "developer", "content": self.system})
        else:
            logger.warning("Prompt does not contain system prompt")

        if self.in_context_examples_preamble:
            prompt.append(
                {
                    "role": "developer",
                    "content": self.in_context_examples_preamble["system"],
                }
            )

            if "user" in self.in_context_examples_preamble:
                prompt.append(
                    {
                        "role": "user",
                        "content": self.in_context_examples_preamble["user"],
                    }
                )
            if "assistant" in self.in_context_examples_preamble:
                prompt.append(
                    {
                        "role": "assistant",
                        "content": self.in_context_examples_preamble["assistant"],
                    }
                )

        else:
            logger.warning("Prompt does not contain in context examples preamble")

        if self.in_context_examples:
            for e in self.in_context_examples:
                prompt += e.to_openai_json()
        else:
            logger.warning("Prompt does not contain in context examples")

        if self.novel_instruction_preamble:
            prompt.append(
                {"role": "developer", "content": self.novel_instruction_preamble}
            )
        else:
            logger.warning("Prompt does not include novel instruction preamble")

        if novel_instruction:
            if self.novel_instruction:
                logger.warning(
                    f"Overriding default novel instruction `{self.novel_instruction}` with new instruction `{novel_instruction}`"
                )
            prompt.append({"role": "user", "content": novel_instruction})
        elif self.novel_instruction:
            prompt.append({"role": "user", "content": self.novel_instruction})

        if self.novel_instruction_ps:
            prompt.append({"role": "developer", "content": self.novel_instruction_ps})

        return prompt

    def __repr__(self):
        return repr(self.to_openai_json(None))


class InContextExample:
    def __init__(self, user, assistant, system=None):
        self.system = system
        self.user = user
        self.assistant = assistant

    @classmethod
    def from_dict(cls, d):
        system = d["system"] if "system" in d else None
        user = d["user"]
        assistant = d["assistant"]

        return cls(user, assistant, system=system)

    def to_dict(self):
        d = {}
        if self.system:
            d["system"] = self.system
        if self.user:
            d["user"] = self.user
        if self.assistant:
            d["assistant"] = self.assistant

        return d

    def to_openai_json(self):
        parts = []

        if self.system:
            system_part = {"role": "developer", "content": self.system}
            parts.append(system_part)

        user_part = {"role": "user", "content": self.user}
        parts.append(user_part)
        assistant_part = {"role": "assistant", "content": self.assistant}
        parts.append(assistant_part)

        return parts
