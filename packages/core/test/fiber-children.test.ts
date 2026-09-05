import { expect, test } from "bun:test";
import { Fiber } from "../src/fiber";

test("completed children detach in reverse order without changing snapshots", () => {
  const parent = new Fiber<number>();
  const children = Array.from({ length: 10_000 }, () => new Fiber<number>());
  for (const child of children) parent.addChild(child);
  const snapshot = parent.childrenSnapshot();
  for (let i = children.length - 1; i >= 0; i--) {
    children[i]!.complete({ ok: true, value: i });
    expect(parent.childCount).toBe(i);
    expect(children[i]!.parent).toBeNull();
  }
  expect(snapshot).toEqual(children);
  expect(parent.childrenSnapshot()).toEqual([]);
});

test("parent completion interrupts every child even when children detach immediately", () => {
  const parent = new Fiber<number>();
  const children = Array.from({ length: 100 }, () => new Fiber<number>());
  for (const child of children) parent.addChild(child);
  children[50]!.complete({ ok: true, value: 50 });
  expect(parent.childrenSnapshot()).toEqual(children.filter((_, i) => i !== 50));
  parent.complete({ ok: true, value: 1 });
  for (const [i, child] of children.entries()) {
    expect(child.status).toBe("done");
    expect(child.interrupted).toBe(i !== 50);
    expect(child.parent).toBeNull();
  }
  expect(parent.childCount).toBe(0);
});
