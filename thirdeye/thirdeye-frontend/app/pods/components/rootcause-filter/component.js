import Ember from 'ember';

export default Ember.Component.extend({
  entities: null, // {}

  onSelect: null, // function (Set)

  actions: {
    selectEventType(eventType) {
      const entities = this.get('entities');
      const onSelect = this.get('onSelect');
      if (onSelect != null) {
        const urns = Object.keys(entities).filter(urn => entities[urn].type == 'event').filter(urn => entities[urn].eventType == eventType)
        onSelect(urns);
      }
    },

    resetEventType() {
      const entities = this.get('entities');
      const onSelect = this.get('onSelect');
      if (onSelect != null) {
        const urns = Object.keys(entities).filter(urn => entities[urn].type == 'event');
        onSelect(urns);
      }
    }
  }
});
