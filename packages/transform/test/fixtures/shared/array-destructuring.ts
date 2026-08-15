// expect: 4
export const program = eff(($) => {
  const [x, y] = $(succeed([7, 3]));
  return x - y;
});
