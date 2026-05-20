<script setup lang="ts">
import { syntaxStyle, type SyntaxStyle } from "./syntax-style";

const tabs: { value: SyntaxStyle; label: string }[] = [
  { value: "chainable", label: "Chainable" },
  { value: "generator", label: "Generator" },
];
</script>

<template>
  <div class="syntax-tabs">
    <div class="syntax-tabs-header" role="tablist">
      <button
        v-for="tab in tabs"
        :key="tab.value"
        type="button"
        role="tab"
        :aria-selected="syntaxStyle === tab.value"
        :class="{ active: syntaxStyle === tab.value }"
        @click="syntaxStyle = tab.value"
      >
        {{ tab.label }}
      </button>
    </div>
    <div class="syntax-tabs-body" :data-active="syntaxStyle">
      <slot />
    </div>
  </div>
</template>

<style scoped>
.syntax-tabs {
  margin: 16px 0;
  border: 1px solid var(--vp-c-divider);
  border-radius: 8px;
  overflow: hidden;
}

.syntax-tabs-header {
  display: flex;
  gap: 0;
  background: var(--vp-c-bg-soft);
  border-bottom: 1px solid var(--vp-c-divider);
}

.syntax-tabs-header button {
  padding: 8px 16px;
  font-size: 13px;
  font-weight: 500;
  color: var(--vp-c-text-2);
  background: transparent;
  border: none;
  border-bottom: 2px solid transparent;
  cursor: pointer;
  transition: color 0.15s, border-color 0.15s, background 0.15s;
}

.syntax-tabs-header button:hover {
  color: var(--vp-c-text-1);
}

.syntax-tabs-header button.active {
  color: var(--vp-c-brand-1);
  border-bottom-color: var(--vp-c-brand-1);
  background: var(--vp-c-bg);
}

.syntax-tabs-body {
  padding: 0 16px;
}

.syntax-tabs-body :deep([data-syntax]) {
  display: none;
}
.syntax-tabs-body[data-active="generator"] :deep([data-syntax="generator"]) {
  display: block;
}
.syntax-tabs-body[data-active="chainable"] :deep([data-syntax="chainable"]) {
  display: block;
}
</style>
