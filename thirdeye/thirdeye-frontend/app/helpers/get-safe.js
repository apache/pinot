import Ember from 'ember';

export function getSafe([dict, key]) {
  if (!dict) { return; }
  return dict[key];
}

export default Ember.Helper.helper(getSafe);

