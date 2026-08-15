// expect: 7
export const program = eff(($) => {
  $(succeed("ignored"));
  return 7;
});
