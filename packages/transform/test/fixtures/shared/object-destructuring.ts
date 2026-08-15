// expect: 3
export const program = eff(($) => {
  const { a, b } = $(succeed({ a: 1, b: 2 }));
  return a + b;
});
