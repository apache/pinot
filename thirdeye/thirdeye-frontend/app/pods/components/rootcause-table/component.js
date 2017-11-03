import Ember from 'ember';

export default Ember.Component.extend({
  columns: null, // []

  entities: null, // {}

  selectedUrns: null, // Set

  onSelect: null, // function (e)

  data: Ember.computed(
    'entities',
    function () {
      const entities = this.get('entities');
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
      const entities = this.get('entities');
      const selectedUrns = this.get('selectedUrns');
      const selectedEntities = [...selectedUrns].filter(urn => entities[urn]).map(urn => entities[urn]);
      return selectedEntities;
    }
  ),

  actions: {
    displayDataChanged (e) {
      const onSelect = this.get('onSelect');
      if (onSelect != null) {
        onSelect(e.selectedItems.map(e => e.urn));
      }
    }
  }
});
