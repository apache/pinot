/**
 * Filter Bar Component
 * Constructs a filter bar based on a config file
 * @module components/filter-bar
 * @property {Object} config              - array of objects retrieved via API call to construct filter bar
 * @property {Number} maxStrLen           - number of characters for filter name truncation
 * @property {Array}  onFilterSelection   - closure action to bubble to controller on filter selection change
 *
 * @example
 * {{filter-bar
 *   config=filterBarConfig
 *   maxStrLen=25
 *   onSelectFilter=(action "onFilterSelection")}}
 *
 * @exports filter-bar
 */
import Ember from 'ember';

export default Ember.Component.extend({

  /**
   * Mock data for every dropdown options
   * This will be an API call
   */
  options: ['All', 'None'],

  actions: {
    /**
     * Handles selection of filter items.
     * TODO: Write logic to handle filter selection
     * @method onFilterSelection
     */
    onFilterSelection() {
      console.log("on change");
    },

    /**
     * Expands/collapses a filter block
     * @param {Object} clickedBlock - selected filter block object
     * TODO: Write logic to toggle isHidden property
     */
    toggleDisplay(clickedBlock) {
      console.log(`toggle this block: ${clickedBlock}`);
    }
  }
});
