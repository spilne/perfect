import DefaultTheme from "vitepress/theme";
import type { Theme } from "vitepress";
import { h } from "vue";
import SyntaxTabs from "./SyntaxTabs.vue";
import SyntaxToggle from "./SyntaxToggle.vue";

export default {
  extends: DefaultTheme,
  Layout: () =>
    h(DefaultTheme.Layout, null, {
      "nav-bar-content-after": () => h(SyntaxToggle),
    }),
  enhanceApp({ app }) {
    app.component("SyntaxTabs", SyntaxTabs);
  },
} satisfies Theme;
