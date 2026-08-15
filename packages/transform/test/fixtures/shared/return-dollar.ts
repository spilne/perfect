// expect: "direct"
export const program = eff(($) => {
  const prefix = $(succeed("dir"));
  return $(succeed(prefix + "ect"));
});
