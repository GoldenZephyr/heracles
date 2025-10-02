#!/usr/bin/env python3
from textual.app import App, ComposeResult
from textual.widgets import Input, Static, RichLog, TextArea, Label, Rule, Footer
from textual.containers import VerticalScroll
from textual.binding import Binding
import yaml
import threading


from heracles_evaluation.llm_agent import LlmAgent
from heracles_evaluation.llm_interface import AgentContext


def new_user_message(text):
    return [{"role": "user", "content": text}]


def generate_initial_prompt(agent: LlmAgent):
    prompt = agent.agent_info.prompt_settings.base_prompt
    return prompt


class MyTextArea(TextArea):
    BINDINGS = [
        Binding("ctrl+b", "submit", "Submit text"),
    ]

    def action_submit(self) -> None:
        self.app.action_submit()


class InputDisplayApp(App):
    def __init__(self, agent):
        self.agent = agent
        self.messages = generate_initial_prompt(agent).to_openai_json("Now you will interact with the user:")
        super().__init__()

    def compose(self) -> ComposeResult:
        """Create child widgets for the app."""
        yield VerticalScroll(
            Label("Agent Chat:"),
            Rule(),
            RichLog(highlight=True, markup=True, wrap=True),
            Rule(line_style="thick"),
            Label("Enter text below:"),
            Rule(),
            MyTextArea("text", id="text_area"),
            Footer(id="footer"),
        )

    def on_input_submitted(self, event: Input.Submitted) -> None:
        """Handle input submission."""

        input_widget = self.query_one("#input_box", Input)
        # Clear the input box
        input_widget.value = ""

        self.display_text = f"You entered: {event.value}"
        self.query_one("#display_panel", Static).update(self.display_text)
        text_log = self.query_one(RichLog)
        text_log.write(event.value)

    def action_submit(self) -> None:
        """Called when ctrl+b is pressed."""
        input_text_box = self.query_one("#text_area", MyTextArea)
        input_text = input_text_box.text
        text_log = self.query_one(RichLog)
        formatted_text = f"[bold black on white]User:[/] {input_text}"
        text_log.write(formatted_text)
        text_log.write("")
        input_text_box.text = ""

        self.messages += new_user_message(input_text)
        initial_length = len(self.messages)

        cxt = AgentContext(self.agent)
        cxt.history = self.messages

        def run_agent():
            success, answer = cxt.run()
            responses = cxt.get_agent_responses()
            for r in responses[initial_length:]:
                text_log.write(r.parsed_response)
                text_log.write("")

        thread = threading.Thread(target=run_agent)
        thread.start()

        #success, answer = cxt.run()
        #responses = cxt.get_agent_responses()
        #for r in responses[initial_length:]:
        #    text_log.write(r.parsed_response)
        #    text_log.write("")


if __name__ == "__main__":
    with open("agent_config.yaml", "r") as fo:
        yml = yaml.safe_load(fo)
    agent = LlmAgent(**yml)
    app = InputDisplayApp(agent)
    app.run()
