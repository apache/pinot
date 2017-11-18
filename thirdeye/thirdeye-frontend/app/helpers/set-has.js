import Ember from 'ember';

export function setHas([set, value]) {
  if (!set) { return; }
  return set.has && set.has(value);
}

export default Ember.Helper.helper(setHas);

