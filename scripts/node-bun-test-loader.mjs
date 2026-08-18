const BUN_TEST_SHIM = new URL('./node-bun-test-shim.mjs', import.meta.url).href;

export async function resolve(specifier, context, nextResolve) {
  if (specifier === 'bun:test') {
    return {
      url: BUN_TEST_SHIM,
      shortCircuit: true,
    };
  }

  return nextResolve(specifier, context);
}

