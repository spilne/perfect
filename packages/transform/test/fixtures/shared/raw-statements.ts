// expect: 12
export const program = eff(($) => {
  const x = $(succeed(5));
  const doubled = x * 2;
  $(succeed("side-effect"));
  return doubled + 2;
});
