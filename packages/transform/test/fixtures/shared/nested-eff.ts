// expect: 14
export const inner = eff(($) => {
  const a = $(succeed(3));
  return a + 1;
});
export const program = eff(($) => {
  const x = $(inner);
  const y = $(succeed(10));
  return x + y;
});
