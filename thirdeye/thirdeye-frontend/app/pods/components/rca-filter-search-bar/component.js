import Ember from 'ember';

export default Ember.Component.extend({

  /**
   * Value of the input
   * @type {String}
   */
  value: '',

  actions: {

    /**
     * Filters the results in the events table based on user input in the search bar
     * @method filterResults
     */
    filterResults() {
      const { entities, onSelect } = this.getProperties('entities', 'onSelect');
      this.set('value', this.get('value'));
      debugger;
      const urns = Object.keys(entities).filter(urn => entities[urn].label.includes(this.value.toLowerCase()));
      onSelect(urns);
    }
  }
});
