import { helper } from '@ember/component/helper';

export function setHas([set, value]) {
  if (!set) { return; }
  return set.has && set.has(value);
}

export default helper(setHas);

