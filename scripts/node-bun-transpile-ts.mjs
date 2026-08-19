const source = await new Response(Bun.stdin).text();
const out = new Bun.Transpiler({
  loader: "ts",
  target: "node",
}).transformSync(source);

process.stdout.write(out);
