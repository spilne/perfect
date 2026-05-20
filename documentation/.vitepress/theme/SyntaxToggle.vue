<script setup lang="ts">
import { syntaxStyle, type SyntaxStyle } from "./syntax-style";

const tabs: { value: SyntaxStyle; label: string }[] = [
  { value: "chainable", label: "chain" },
  { value: "generator", label: "gen" },
];
</script>

<template>
  <div class="syntax-toggle" role="radiogroup" aria-label="Syntax style">
    <button
      v-for="tab in tabs"
      :key="tab.value"
      type="button"
      role="radio"
      :aria-checked="syntaxStyle === tab.value"
      :class="{ active: syntaxStyle === tab.value }"
      :title="`Show ${tab.value}-style examples`"
      @click="syntaxStyle = tab.value"
    >
      {{ tab.label }}
    </button>
  </div>
</template>

<style scoped>
.syntax-toggle {
  display: inline-flex;
  align-items: center;
  gap: 0;
  margin-right: 16px;
  padding: 2px;
  background: var(--vp-c-bg-soft);
  border: 1px solid var(--vp-c-divider);
  border-radius: 6px;
}

.syntax-toggle button {
  padding: 3px 8px;
  font-size: 12px;
  font-weight: 500;
  font-family: var(--vp-font-family-mono);
  color: var(--vp-c-text-2);
  background: transparent;
  border: none;
  border-radius: 4px;
  cursor: pointer;
  transition: color 0.15s, background 0.15s;
}

.syntax-toggle button:hover {
  color: var(--vp-c-text-1);
}

.syntax-toggle button.active {
  color: var(--vp-c-brand-1);
  background: var(--vp-c-bg);
  box-shadow: 0 1px 2px rgba(0, 0, 0, 0.05);
}
</style>
