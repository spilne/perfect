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
