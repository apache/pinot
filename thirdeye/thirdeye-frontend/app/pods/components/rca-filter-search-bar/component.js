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
      const { activeEventType, filterUrns, value } = this.getProperties('activeEventType', 'filterUrns', 'value');
      this.set('value', this.get('value'));
      filterUrns(activeEventType, null, value);
    }
  }
});
