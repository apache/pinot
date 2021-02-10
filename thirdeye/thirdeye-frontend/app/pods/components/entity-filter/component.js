/**
 * Entity Filtering Component
 * This component receives an array of filter definitions (or 'blocks') and renders them according to 'type'.
 * Event objects passed up from here contain arrays of filter names and values, created here as
 * 'multiSelectKeys'. A filter object may look like:
 *  {
 *   application: ['app name a', 'app name b'],
 *   status: ['active'],
 *   owner: ['person1@linkedin.com, person2@linkedin.com'],
 *   type: null
 * }
 * @module components/entity-filter
 * @property {string} title             - title of filter side-bar
 * @property {boolean} isGlobal         - determines type of filtering and expected response
 * @property {string} resetFilters      - any string value to reset filters (trigger re-render)
 * @property {boolean} selectDisabled   - if true, all select fields are disabled
 * @property {number} maxStrLen         - number of characters for filter name truncation
 * @property {object} filterBlocks      - properties for each block of filters
 * @property {action} onSelectFilter    - closure action to bubble to controller on filter selection change
 *
 * @example
 * {{entity-filter
 *   title="Refine results by"
 *   maxStrLen=25
 *   isGlobal=true
 *   resetFilters=resetFiltersVal
 *   selectDisabled=isSelectDisabled
 *   filterBlocks=filterBlocks
 *   onSelectFilter=(action "userDidSelectFilter")}}
 *
 * @exports entity-filter
 */
import { set, get, setProperties } from '@ember/object';
import { isPresent } from '@ember/utils';
import Component from '@ember/component';
import { autocompleteAPI } from 'thirdeye-frontend/utils/api/self-serve';
import { task, timeout } from 'ember-concurrency';
import { checkStatus } from 'thirdeye-frontend/utils/utils';

export default Component.extend({
  /**
   * List of associated classes
   */
  classNames: ['entity-filter'],

  /**
   * Select fields enabled by default
   */
  selectDisabled: false,

  //
  // internal
  //
  // eslint-disable-next-line ember/avoid-leaking-state-in-ember-objects
  mostRecentSearches: {}, // promise

  /**
   * Overwrite the init function
   * @param {Object} args - Attributes for this component
   */
  didReceiveAttrs() {
    this._super(...arguments);
    const filterBlocks = get(this, 'filterBlocks');
    const filterStateObj = get(this, 'currentFilterState');
    let multiSelectKeys = {}; // new filter object

    // Set up filter block object
    filterBlocks.forEach((block) => {
      let filtersArray = [];
      let filterKeys = [];
      let tag = block.name.camelize();
      let matchWidth = block.matchWidth ? block.matchWidth : false;

      // Initially load existing state of selected filters if available
      if (filterStateObj && filterStateObj[tag] && Array.isArray(filterStateObj[tag])) {
        block.selected = filterStateObj[tag];
      }
      // If any pre-selected items, bring them into the new filter object
      multiSelectKeys[tag] = block.selected ? block.selected : null;
      // Dedupe and remove null or empty values
      filterKeys = Array.from(new Set(block.filterKeys.filter((value) => isPresent(value) && value !== 'undefined')));
      // Generate a name and Id for each one based on provided filter keys
      if (block.type !== 'select') {
        filterKeys.forEach((filterName, index) => {
          filtersArray.push({
            name: filterName,
            id: filterName.dasherize(),
            total: block.totals ? block.totals[index] : '',
            isActive: block.selected && block.selected.includes(filterName)
          });
        });
      }
      // Now add new initialized props to block item
      setProperties(block, {
        tag,
        filterKeys,
        matchWidth,
        filtersArray,
        isHidden: false
      });
    });
    set(this, 'multiSelectKeys', multiSelectKeys);
  },

  /**
   * Array containing the running list of all user-selected filters
   * @type {Array}
   */
  // eslint-disable-next-line ember/avoid-leaking-state-in-ember-objects
  alertFilters: [],

  /**
   * Ember concurrency task that triggers various autocomplete calls
   */
  searchTypes: task(function* (type, text) {
    yield timeout(1000);
    switch (type) {
      case 'metric': {
        return fetch(autocompleteAPI.metric(text))
          .then(checkStatus)
          .then((metrics) => [...new Set(metrics.map((m) => m.name))]);
      }
      case 'application': {
        return fetch(autocompleteAPI.application(text))
          .then(checkStatus)
          .then((applications) => applications.map((a) => a.application));
      }
      case 'subscription': {
        return fetch(autocompleteAPI.subscriptionGroup(text))
          .then(checkStatus)
          .then((groups) => groups.map((g) => g.name));
      }
      case 'owner': {
        return fetch(autocompleteAPI.owner(text)).then(checkStatus);
      }
      case 'dataset': {
        return fetch(autocompleteAPI.dataset(text))
          .then(checkStatus)
          .then((datasets) => [...new Set(datasets.map((d) => d.name))]);
      }
      case 'alertName': {
        return fetch(autocompleteAPI.alertByName(text))
          .then(checkStatus)
          .then((detections) => detections.map((d) => d.name));
      }
    }
  }),

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
    onFilterSelection(filterObj, selectedItems) {
      const selectKeys = get(this, 'multiSelectKeys');
      let selectedArr = selectedItems;

      // Handle 'status' field toggling rules
      if (filterObj.tag === 'status' && filterObj.selected) {
        // Toggle selected status
        set(selectedItems, 'isActive', !selectedItems.isActive);
        // Map selected status to array - will add to filter map
        selectedArr = filterObj.filtersArray.filterBy('isActive').mapBy('name');
        // Make sure 'Active' is selected by default when both are un-checked
        if (filterObj.filtersArray.filter((item) => item.isActive).length === 0) {
          selectedArr = ['Active'];
        }
      }
      // Handle 'global' or 'primary' filter field toggling
      if (filterObj.tag === 'primary') {
        filterObj.filtersArray.forEach((filter) => set(filter, 'isActive', false));
        const activeFilter = filterObj.filtersArray.find((filter) => filter.name === selectedItems);
        set(activeFilter, 'isActive', true);
      }

      // // Handle metric filter
      // if (filterObj.tag === 'metric') {
      //   selectedArr = selectedItems.map(filter => filter.name);
      // }

      // Sets the 'alertFilters' object in parent
      set(selectKeys, filterObj.tag, selectedArr);
      set(selectKeys, 'triggerType', filterObj.type);
      // Send action up to parent controller
      this.get('onSelectFilter')(selectKeys);
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
    },

    /**
     * Bubbles clear filters action up to parent
     * @method clearFilters
     * @return {undefined}
     */
    clearFilters() {
      this.get('onClearFilters')();
    },

    /**
     * Performs a search task while cancelling the previous one
     * @param {String} type
     * @param {String} text
     */
    onSearch(type, text) {
      const searchCache = this.get('mostRecentSearches');
      if (searchCache[type]) {
        searchCache[type].cancel();
      }
      const task = this.get('searchTypes');
      const taskInstance = task.perform(type, text);
      set(searchCache, type, taskInstance);

      return taskInstance;
    }
  }
});
