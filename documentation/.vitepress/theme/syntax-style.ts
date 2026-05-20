import { ref, watch } from "vue";

export type SyntaxStyle = "generator" | "chainable";

const KEY = "perfect-syntax-style";

function initial(): SyntaxStyle {
  if (typeof localStorage === "undefined") return "chainable";
  const v = localStorage.getItem(KEY);
  return v === "generator" ? "generator" : "chainable";
}

export const syntaxStyle = ref<SyntaxStyle>(initial());

if (typeof window !== "undefined") {
  watch(syntaxStyle, (v) => {
    try {
      localStorage.setItem(KEY, v);
    } catch {
      // ignore quota errors
    }
  });
}
