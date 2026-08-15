// expect: "big"
export const program = eff(($) => {
  const x = $(succeed(10));
  if (x > 5) {
    return $(succeed("big"));
  } else {
    return $(succeed("small"));
  }
});
