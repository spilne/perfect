<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import { runSource } from "../../../templates/stackblitz/src/playground-client";
import { scenarios, type PlaygroundScenario } from "../../../templates/stackblitz/src/scenarios";

type RunStatus = "ready" | "running" | "success" | "failure";

const selected = ref<PlaygroundScenario>(scenarios[0]!);
const source = ref(selected.value.source);
const status = ref<RunStatus>("ready");
const elapsedMs = ref<number>();
const output = ref<unknown>();
const failure = ref("");
const formattedOutput = computed(() =>
  output.value === undefined ? "" : (JSON.stringify(output.value, null, 2) ?? String(output.value)),
);

function selectScenario(scenario: PlaygroundScenario): void {
  if (status.value === "running") return;
  selected.value = scenario;
  source.value = scenario.source;
  resetResult();
}

function resetSource(): void {
  source.value = selected.value.source;
  resetResult();
}

function resetResult(): void {
  status.value = "ready";
  elapsedMs.value = undefined;
  output.value = undefined;
  failure.value = "";
}

async function execute(): Promise<void> {
  if (status.value === "running") return;
  status.value = "running";
  elapsedMs.value = undefined;
  output.value = undefined;
  failure.value = "";

  const startedAt = performance.now();
  try {
    output.value = await runSource(source.value);
    elapsedMs.value = Math.round(performance.now() - startedAt);
    status.value = "success";
  } catch (error) {
    status.value = "failure";
    failure.value = error instanceof Error ? error.message : String(error);
  }
}

function editorKeydown(event: KeyboardEvent): void {
  if ((event.metaKey || event.ctrlKey) && event.key === "Enter") {
    event.preventDefault();
    void execute();
  }
}

onMounted(() => void execute());
</script>

<template>
  <main class="playground-shell">
    <header class="playground-hero">
      <p class="eyebrow">@perfect/core · stream lab</p>
      <h1>Edit the stream. Run the effect.</h1>
      <p class="lede">
        Explore concurrency, typed failures, source retry, state, cancellation, reliable
        observation, and reactive composition. Your edits run in a disposable worker with a
        five-second limit.
      </p>
      <a
        class="stackblitz-link"
        href="https://stackblitz.com/fork/github/spilne/perfect/tree/main/templates/stackblitz?title=Perfect%20Playground"
        target="_blank"
        rel="noreferrer"
      >
        Open with full TypeScript diagnostics in StackBlitz ↗
      </a>
    </header>

    <section class="scenario-card" aria-labelledby="scenario-title">
      <div class="scenario-list" aria-label="Example scenarios">
        <button
          v-for="scenario in scenarios"
          :key="scenario.id"
          type="button"
          class="scenario"
          :class="{ active: scenario.id === selected.id }"
          :disabled="status === 'running'"
          @click="selectScenario(scenario)"
        >
          {{ scenario.title }}
        </button>
      </div>
      <div class="scenario-copy">
        <h2 id="scenario-title">{{ selected.title }}</h2>
        <p>{{ selected.description }}</p>
      </div>
    </section>

    <section class="editor-card" aria-label="Editable stream program">
      <div class="editor-heading">
        <span>stream.js</span>
        <span>JavaScript · Ctrl/⌘ + Enter</span>
      </div>
      <textarea
        v-model="source"
        spellcheck="false"
        aria-label="Editable stream source"
        @keydown="editorKeydown"
      ></textarea>
      <div class="editor-actions">
        <button
          class="secondary"
          type="button"
          :disabled="status === 'running'"
          @click="resetSource"
        >
          Reset preset
        </button>
        <button type="button" :disabled="status === 'running'" @click="execute">
          {{ status === "running" ? "Running…" : "Run stream" }}
        </button>
      </div>
    </section>

    <section class="result-card" aria-live="polite" :aria-busy="status === 'running'">
      <div class="result-heading">
        <span class="status" :class="status">{{ status }}</span>
        <span v-if="status === 'success'" class="timing">{{ elapsedMs }} ms</span>
        <span v-else-if="status === 'running'" class="timing">isolated worker</span>
        <span v-else-if="status === 'failure'" class="timing">edit and retry</span>
      </div>

      <p v-if="status === 'ready' || status === 'running'" class="message">
        {{ status === "running" ? "Evaluating the edited stream…" : "Ready to run." }}
      </p>
      <p v-else-if="status === 'failure'" class="error-message">{{ failure }}</p>
      <pre v-else class="json-output">{{ formattedOutput }}</pre>
    </section>

    <p class="editor-note">
      The inline editor exposes <code>Stream</code>, <code>RetryPolicy</code>, <code>delay</code>,
      <code>succeed</code>, and <code>fail</code>. It runs JavaScript for fast experiments; use
      StackBlitz for the full <code>Stream&lt;A, S&gt;</code> TypeScript experience.
    </p>
  </main>
</template>

<style scoped>
.playground-shell {
  width: min(1040px, calc(100% - 32px));
  margin: 0 auto;
  padding: 68px 0 40px;
}

.playground-hero {
  max-width: 820px;
  margin-bottom: 34px;
}

.eyebrow {
  margin: 0 0 12px;
  color: var(--vp-c-brand-1);
  font-family: var(--vp-font-family-mono);
  font-size: 0.76rem;
  font-weight: 750;
  letter-spacing: 0.13em;
  text-transform: uppercase;
}

h1 {
  margin: 0;
  border: 0;
  color: var(--vp-c-text-1);
  font-size: clamp(2.7rem, 7vw, 5.7rem);
  font-weight: 780;
  letter-spacing: -0.06em;
  line-height: 0.94;
}

.lede {
  max-width: 720px;
  margin: 22px 0 18px;
  color: var(--vp-c-text-2);
  font-size: clamp(1rem, 2vw, 1.18rem);
  line-height: 1.65;
}

.stackblitz-link {
  color: var(--vp-c-brand-1);
  font-size: 0.9rem;
  font-weight: 700;
}

.scenario-card,
.editor-card,
.result-card {
  overflow: hidden;
  border: 1px solid var(--vp-c-divider);
  border-radius: 20px;
  background: var(--vp-c-bg-soft);
  box-shadow: 0 20px 60px rgb(29 58 48 / 9%);
}

.scenario-card {
  display: grid;
  grid-template-columns: minmax(220px, 0.7fr) minmax(300px, 1.3fr);
}

.scenario-list {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 7px;
  border-right: 1px solid var(--vp-c-divider);
  padding: 18px;
  background: var(--vp-c-bg-alt);
}

button,
textarea {
  font: inherit;
}

button {
  border: 0;
  border-radius: 11px;
  padding: 10px 15px;
  color: #fff;
  background: var(--vp-c-brand-1);
  cursor: pointer;
  font-weight: 700;
}

button:hover:not(:disabled) {
  background: var(--vp-c-brand-2);
}

button:disabled {
  cursor: wait;
  opacity: 0.62;
}

.scenario {
  padding: 9px 10px;
  color: var(--vp-c-text-2);
  background: transparent;
  font-size: 0.78rem;
  text-align: left;
}

.scenario:hover:not(:disabled) {
  color: var(--vp-c-brand-1);
  background: var(--vp-c-default-soft);
}

.scenario.active {
  color: #fff;
  background: var(--vp-c-brand-1);
}

.scenario-copy {
  align-self: center;
  padding: 26px 30px;
}

.scenario-copy h2 {
  margin: 0 0 8px;
  border: 0;
  padding: 0;
  color: var(--vp-c-text-1);
  font-size: 1.24rem;
}

.scenario-copy p {
  margin: 0;
  color: var(--vp-c-text-2);
  line-height: 1.55;
}

.editor-card {
  margin-top: 16px;
  color: #dcebe5;
  background: #172a24;
}

.editor-heading,
.result-heading,
.editor-actions {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
}

.editor-heading {
  border-bottom: 1px solid rgb(255 255 255 / 9%);
  padding: 12px 18px;
  color: #8eb6a8;
  font-family: var(--vp-font-family-mono);
  font-size: 0.72rem;
}

textarea {
  display: block;
  width: 100%;
  min-height: 310px;
  resize: vertical;
  border: 0;
  padding: 22px 24px;
  color: #d9eee6;
  background: #172a24;
  font-family: var(--vp-font-family-mono);
  font-size: 0.86rem;
  line-height: 1.65;
  outline: none;
  tab-size: 2;
}

textarea:focus {
  box-shadow: inset 0 0 0 2px color-mix(in srgb, var(--vp-c-brand-1) 48%, transparent);
}

.editor-actions {
  border-top: 1px solid rgb(255 255 255 / 9%);
  padding: 12px 16px;
}

.secondary {
  color: #b8d1c8;
  background: rgb(255 255 255 / 8%);
}

.result-card {
  min-height: 180px;
  margin-top: 16px;
  padding: 24px;
}

.status {
  display: inline-flex;
  border-radius: 999px;
  padding: 6px 10px;
  font-size: 0.71rem;
  font-weight: 800;
  letter-spacing: 0.06em;
  text-transform: uppercase;
}

.status.ready,
.status.running {
  color: var(--vp-c-text-2);
  background: var(--vp-c-default-soft);
}

.status.success {
  color: var(--vp-c-green-1);
  background: var(--vp-c-green-soft);
}

.status.failure {
  color: var(--vp-c-danger-1);
  background: var(--vp-c-danger-soft);
}

.timing {
  color: var(--vp-c-text-3);
  font-family: var(--vp-font-family-mono);
  font-size: 0.78rem;
}

.message,
.error-message,
.json-output {
  margin: 20px 0 0;
}

.message {
  color: var(--vp-c-text-2);
}

.error-message {
  border-left: 3px solid var(--vp-c-danger-1);
  padding: 12px 14px;
  color: var(--vp-c-danger-1);
  background: var(--vp-c-danger-soft);
}

.json-output {
  overflow: auto;
  max-height: 360px;
  border-radius: 12px;
  padding: 16px;
  color: #cce6dc;
  background: #1c312a;
  font-family: var(--vp-font-family-mono);
  font-size: 0.79rem;
  line-height: 1.55;
}

.editor-note {
  margin: 18px 4px 0;
  color: var(--vp-c-text-3);
  font-size: 0.82rem;
}

@media (max-width: 720px) {
  .playground-shell {
    padding-top: 42px;
  }

  .scenario-card {
    grid-template-columns: 1fr;
  }

  .scenario-list {
    border-right: 0;
    border-bottom: 1px solid var(--vp-c-divider);
  }

  .editor-heading {
    align-items: flex-start;
    flex-direction: column;
  }

  textarea {
    min-height: 360px;
    font-size: 0.78rem;
  }
}
</style>
