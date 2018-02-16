/**
 * Entity Filtering Component
 * @module components/entity-filter
 * @property {string} title             - title of filter side-bar
 * @property {number} maxStrLen         - number of characters for filter name truncation
 * @property {object} filterBlocks      - properties for each block of filters
 * @property {array}   onSelectFilter   - closure action to bubble to controller on filter selection change
 *
 * @example
 * {{entity-filter
 *   title="Refine results by"
 *   maxStrLen=25
 *   filterBlocks=filterBlocks
 *   onSelectFilter=(action "userDidSelectFilter")}}
 *
 * @exports entity-filter
 */

import { set } from '@ember/object';

import { isPresent } from '@ember/utils';
import Component from '@ember/component';
import _ from 'lodash';

export default Component.extend({
  /**
   * Overwrite the init function
   * @param {Object} args - Attributes for this component
   */
  init() {
    this._super(...arguments);
    const filterBlocks = this.get('filterBlocks');
    const isHidden = false;

    // Set up filter block object
    filterBlocks.forEach((block) => {
      let filtersArray = [];
      let filterKeys = [];

      // Dedupe and remove null or empty values
      filterKeys = Array.from(new Set(block.filterKeys.filter(value => isPresent(value))));
      // Generate a name and Id for each one based on provided filter keys
      filterKeys.forEach((filter) => {
        filtersArray.push({
          isActive: false,
          name: filter,
          id: filter.dasherize()
        });
      });
      // Now add new initialized props to block item
      Object.assign(block, { filtersArray, filterKeys, isHidden });
    });
  },

  /**
   * Array containing the running list of all user-selected filters
   * @type {Array}
   */
  alertFilters: [],

  /**
   * Defined actions for component
   */
  actions: {
    /**
     * Handles selection of filter items. Each time a filter is selected, this component will
     * pass an array of selected filters to its parent.
     * @method onFilterSelection
     * @param {String} category - name of the selected filter's parent block
     * @param {Objedt} filterObj - contains the properties of the selected filter
     * @return {undefined}
     */
    onFilterSelection(category, filterObj) {
      // Note: toggleProperty will not be able to find 'filterObj', as it is not an observed property
      set(filterObj, 'isActive', !filterObj.isActive);

      if (filterObj.isActive) {
        this.get('alertFilters').push({ category, filter: filterObj.name, isActive: filterObj.isActive });
      } else {
        this.set('alertFilters', _.reject(this.get('alertFilters'), function(filter) { return filter.filter === filterObj.name; }));
      }

      // Return the 'userDidSelectFilter' function passed from the parent as the value of onSelectFilter, and invoke it.
      this.get('onSelectFilter')(this.get('alertFilters'));
    },

    /**
     * Handles expand/collapse of the filter block
     * @method toggleDisplay
     * @param {Objedt} clickedBlock - filter block object
     * @return {undefined}
     */
    toggleDisplay(clickedBlock) {
      // Note: toggleProperty will not be able to find 'filterBlocks', as it is not an observed property
      set(clickedBlock, 'isHidden', !clickedBlock.isHidden);
    }
  }
});
