import Ember from 'ember';

export default Ember.Component.extend({
  entities: null, // {}

  selectedUrns: null, // Set

  onSelection: null, // function (Set, state)

  labels: Ember.computed(
    'entities',
    function () {
      const { entities } = this.getProperties('entities');

      const labels = {};
      Object.keys(entities).forEach(urn => labels[urn] = entities[urn].label);
      return labels;
    }
  ),

  actions: {
    toggleSelection(urn) {
      const { selectedUrns, onSelection } = this.getProperties('selectedUrns', 'onSelection');
      if (onSelection) {
        const state = !selectedUrns.has(urn);
        const updates = { [urn]: state };
        onSelection(updates);
      }
    }
  }
});
