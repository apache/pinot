import Ember from 'ember';

export default Ember.Component.extend({
  columns: null, // []

  entities: null, // {}

  selectedUrns: null, // Set

  onSelection: null, // function (e)

  data: Ember.computed(
    'entities',
    function () {
      const { entities } = this.getProperties('entities');
      return Object.values(entities);
    }
  ),

  // NOTE: only works on init
  // NOTE: checkboxes do not bind to model, show unselected
  // http://onechiporenko.github.io/ember-models-table/v.1/#/
  preselectedItems: Ember.computed(
    'entities',
    'selectedUrns',
    function () {
      const { entities, selectedUrns } = this.getProperties('entities', 'selectedUrns');
      const selectedEntities = [...selectedUrns].filter(urn => entities[urn]).map(urn => entities[urn]);
      return selectedEntities;
    }
  ),

  actions: {
    displayDataChanged (e) {
      const { entities, selectedUrns, onSelection } = this.getProperties('entities', 'selectedUrns', 'onSelection');
      if (onSelection) {
        const table = new Set(e.selectedItems.map(e => e.urn));
        const added = [...table].filter(urn => !selectedUrns.has(urn));
        const removed = [...selectedUrns].filter(urn => entities[urn] && !table.has(urn));

        const updates = {};
        added.forEach(urn => updates[urn] = true);
        removed.forEach(urn => updates[urn] = false);

        onSelection(updates);
      }
    }
  }
});
