export function rewriteEmbeddedExamples(
  markdown: string,
  render: (reference: { file: string; region: string }) => string,
): string {
  const embeds =
    /<!-- @embed (?<file>[^#\s]+)#(?<region>[^\s]+) -->\s*\r?\n```[a-z]*\r?\n[\s\S]*?\r?\n```\s*\r?\n<!-- @end -->/g;
  const matches = [...markdown.matchAll(embeds)];
  const starts = [...markdown.matchAll(/<!--\s*@embed\b/g)].length;
  const ends = [...markdown.matchAll(/<!--\s*@end\b/g)].length;
  if (matches.length !== starts || starts !== ends) {
    throw new Error("Malformed example embed: expected @embed, a fenced code block, and @end");
  }
  return markdown.replace(embeds, (...args) => {
    const reference = args[args.length - 1] as { file: string; region: string };
    const code = render(reference);
    return `<!-- @embed ${reference.file}#${reference.region} -->\n\n\`\`\`ts\n${code}\n\`\`\`\n\n<!-- @end -->`;
  });
}
export function identifiersIn(code: string): Set<string> {
  const identifiers = new Set<string>();
  // Tokenize strings before comments so https:// cannot erase the next line.
  // Keep template tokens conservatively: interpolations may reference imports.
  const tokens =
    /"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|`(?:\\.|[^`\\])*`|\/\/[^\n]*|\/\*[\s\S]*?\*\/|[A-Za-z_$][A-Za-z0-9_$]*/g;
  for (const [token] of code.matchAll(tokens)) {
    if (token.startsWith('"') || token.startsWith("'") || token.startsWith("/")) continue;
    for (const [identifier] of token.matchAll(/[A-Za-z_$][A-Za-z0-9_$]*/g))
      identifiers.add(identifier);
  }
  return identifiers;
}
