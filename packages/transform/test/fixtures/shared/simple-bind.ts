// expect: 30
export const program = eff(($) => {
  const x = $(succeed(10));
  const y = $(succeed(20));
  return x + y;
});
