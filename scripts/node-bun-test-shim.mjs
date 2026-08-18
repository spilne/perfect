import * as nodeTest from 'node:test';
import { expect } from 'expect';

export const {
  describe,
  it,
  test,
  before,
  beforeEach,
  beforeAll,
  after,
  afterEach,
  afterAll,
} = nodeTest;

export { expect };

export const skipIf = (condition) => {
  if (condition) {
    return describe.skip;
  }

  return describe;
};

export const testSkipIf = (condition) => {
  if (condition) {
    return test.skip;
  }

  return test;
};

it.skipIf = testSkipIf;
test.skipIf = testSkipIf;
describe.skipIf = skipIf;

export const setDefaultTimeout = () => {};
