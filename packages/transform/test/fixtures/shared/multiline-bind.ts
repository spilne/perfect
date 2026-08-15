// expect: 42
export const program = eff(($) => {
  const x = $(succeed(40));
  const y = $(succeed(2));
  return x + y;
});
