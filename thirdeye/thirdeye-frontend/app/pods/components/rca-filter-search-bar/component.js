import Ember from 'ember';

export default Ember.Component.extend({

  value: '',

  actions: {
    filterResults() {
      this.set('value', this.get('value'));
      const { entities, onSelect } = this.getProperties('entities', 'onSelect');
      debugger;
      const urns = Object.keys(entities).filter(urn => entities[urn].label.includes(this.value.toLowerCase()));
      onSelect(urns);
    }
  }
});
