/**
 * Filter Bar Component
 * Constructs a filter bar based on a config file
 * @module components/filter-bar
 * @property {object[]} config            - [required] array of objects (config file) passed in from the route that sets
 *                                          up the filter bar sub-filters
 * @property {number} maxStrLen           - number of characters for filter name truncation
 * @property {array}  onFilterSelection   - [required] closure action to bubble to controller on filter selection change
 *
 * @example
 * {{filter-bar
 *   config=filterBarConfig
 *   filterBlocks=filterBlocks
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

  /**
   * Overwrite the init function
   * Initializes values of the filter blocks
   * Example of a filter block:
   * {
   *  filtersArray: [
   *    {
   *      isActive: false,
   *      name: 'country',
   *      id: 'country'
   *    }
   *  ],
   *  header: 'holiday',
   *  isHidden: true,
   *  inputs: [
   *    {
   *      label: 'country',
   *      type: 'dropdown
   *    }
   *  ]
   * }
   */
  init() {
    this._super(...arguments);
    // Fetch the config file to create sub-filters
    const filterBlocks = this.get('config');

    // Set up filter block object
    filterBlocks.forEach((block, index) => {
      // Show first sub-filter by default but hide the rest
      let isHidden = Boolean(index);
      let filtersArray = [];

      // Generate a name and id for each one based on provided filter keys
      block.inputs.forEach((filter) => {
        filtersArray.push({
          isActive: false,
          name: filter.label,
          id: filter.label.dasherize()
        });
      });

      // Now add new initialized props to block item
      Object.assign(block, { filtersArray, isHidden });
    });
  },

  actions: {
    /**
     * Handles selection of filter items.
     * @method onFilterSelection
     */
    onFilterSelection() {
      // TODO: Write logic to handle filter selection
    },

    /**
     * Expands/collapses a filter block
     * @method filterByEvent
     * @param {Object} clickedBlock - selected filter block object
     */
    filterByEvent(clickedBlock) {
      // Hide all other blocks when one is clicked
      let filterBlocks = this.get('config');
      filterBlocks.forEach(block => {
        Ember.set(block, 'isHidden', true);
      });

      // Note: toggleProperty will not be able to find 'filterBlocks', as it is not an observed property
      // Show clickedBlock
      Ember.set(clickedBlock, 'isHidden', !clickedBlock.isHidden);
      this.attrs.onTabChange(clickedBlock.eventType);
    }
  }
});
