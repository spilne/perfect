import { runSource } from "./playground-client";
import { scenarios, type PlaygroundScenario } from "./scenarios";
import "./style.css";

const scenarioList = element<HTMLElement>("scenarios");
const scenarioTitle = element<HTMLElement>("scenario-title");
const scenarioDescription = element<HTMLElement>("scenario-description");
const editor = element<HTMLTextAreaElement>("editor");
const runButton = element<HTMLButtonElement>("run");
const resetButton = element<HTMLButtonElement>("reset");
const result = element<HTMLElement>("result");
const status = element<HTMLElement>("status");
const timing = element<HTMLElement>("timing");
const output = element<HTMLElement>("output");

let selected = scenarios[0]!;
let running = false;

function selectScenario(scenario: PlaygroundScenario): void {
  if (running) return;
  selected = scenario;
  scenarioTitle.textContent = scenario.title;
  scenarioDescription.textContent = scenario.description;
  editor.value = scenario.source;
  status.className = "status ready";
  status.textContent = "Ready";
  timing.textContent = "";
  output.replaceChildren(message("Edit the stream or run this preset."));
  renderScenarioButtons();
}

function renderScenarioButtons(): void {
  const buttons = scenarios.map((scenario) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = scenario.id === selected.id ? "scenario active" : "scenario";
    button.textContent = scenario.title;
    button.disabled = running;
    button.addEventListener("click", () => selectScenario(scenario));
    return button;
  });
  scenarioList.replaceChildren(...buttons);
}

async function execute(): Promise<void> {
  if (running) return;
  running = true;
  runButton.disabled = true;
  resetButton.disabled = true;
  result.setAttribute("aria-busy", "true");
  status.className = "status running";
  status.textContent = "Running";
  timing.textContent = "isolated worker";
  output.replaceChildren(message("Evaluating the edited stream…"));
  renderScenarioButtons();

  const startedAt = performance.now();
  try {
    const value = await runSource(editor.value);
    status.className = "status success";
    status.textContent = "Success";
    timing.textContent = `${Math.round(performance.now() - startedAt)} ms`;
    output.replaceChildren(renderValue(value));
  } catch (error) {
    status.className = "status failure";
    status.textContent = "Failed";
    timing.textContent = "edit and retry";
    output.replaceChildren(
      message(error instanceof Error ? error.message : String(error), "error-message"),
    );
  } finally {
    running = false;
    runButton.disabled = false;
    resetButton.disabled = false;
    result.setAttribute("aria-busy", "false");
    renderScenarioButtons();
  }
}

function renderValue(value: unknown): HTMLElement {
  const pre = document.createElement("pre");
  pre.className = "json-output";
  pre.textContent = JSON.stringify(value, null, 2) ?? String(value);
  return pre;
}

function message(text: string, className = "message"): HTMLElement {
  const node = document.createElement("p");
  node.className = className;
  node.textContent = text;
  return node;
}

function element<T extends HTMLElement>(id: string): T {
  const node = document.getElementById(id);
  if (node === null) throw new Error(`Missing #${id}`);
  return node as T;
}

runButton.addEventListener("click", () => void execute());
resetButton.addEventListener("click", () => selectScenario(selected));
editor.addEventListener("keydown", (event) => {
  if ((event.metaKey || event.ctrlKey) && event.key === "Enter") {
    event.preventDefault();
    void execute();
  }
});

selectScenario(selected);
void execute();
