import Ember from 'ember';

export default Ember.Component.extend({
  entities: null, // {}

  selectedUrns: null, // Set

  invisibleUrns: null, // Set

  onVisibility: null, // function (Set, bool)

  onSelection: null, // function (Set, bool)

  labels: Ember.computed(
    'entities',
    'selectedUrns',
    function () {
      const { entities, selectedUrns } = this.getProperties('entities', 'selectedUrns');
      const labels = {};
      [...selectedUrns].filter(urn => urn in entities).forEach(urn => labels[urn] = entities[urn].label);
      return labels;
    }
  ),

  actions: {
    toggleVisibility(urn) {
      const { onVisibility, invisibleUrns } = this.getProperties('onVisibility', 'invisibleUrns');
      if (onVisibility) {
        const state = invisibleUrns.has(urn);
        const updates = { [urn]: state };
        onVisibility(updates);
      }
    },
    
    removeUrn(urn) {
      const { onSelection } = this.getProperties('onSelection');
      if (onSelection) {
        const updates = { [urn]: false };
        onSelection(updates);
      }
    }
  }
});
