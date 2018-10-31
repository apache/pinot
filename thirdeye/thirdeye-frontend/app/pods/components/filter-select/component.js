/**
 * Select Filter component. A wrapper around power select
 * to display large numbers of items
 * @module components/filter-select
 * @property {Number}  maxNumFilters   - Maximum items to show per filter group
 * @property {Number}  maxTotalFilters - Maximum items to show in aggregate
 * @property {Object}  options         - Object containing all filters
 * @property {Boolean} disabled        - Indicates if the input field should be
 *                                       disabled
 * @property {Object}  onChangeHandler - Closure action to bubble up to parent
 *                                       when a filter was (de)selected
 * @exports filter-select
 * @author yyuen
 */

import { computed } from '@ember/object';

import Component from '@ember/component';
import { task, timeout } from 'ember-concurrency';

/**
 * Parse the filter Object and return a Object of groupName
 * containing Arrays of filter options
 * @param {Object} filters
 */
const buildFilterOptions = (filters) => {
  return Object.keys(filters).map((filterName) => {

    const options = filters[filterName]
      .filter(value => !!value)
      .map((value) => {
        return {
          name: value,
          id: `${filterName}::${value}`
        };
      });

    return {
      groupName: `${filterName}`,
      options
    };
  });
};

/**
 * Converts the query Params JSON into
 * powerselect compatible groups
 * @param {Object} filters
 */
const convertHashToFilters = (filters) => {
  const filterArray = [];

  Object.keys(filters).forEach((filterGroup) => {
    const options = filters[filterGroup].map((option) => {
      return {
        name: option,
        id: `${filterGroup}::${option}`
      };
    });
    filterArray.push(...options);
  });

  return filterArray;
};

/**
 * Converts the powerselect grouped filters into
 * JSON for query Params
 * @param {Object} selectedFilters
 */
const convertFiltersToHash = (selectedFilters) => {
  const filters = selectedFilters.reduce((filterHash, filter) => {
    const [ filterGroup, filterName ] = filter.id.split('::');

    filterHash[filterGroup] = filterHash[filterGroup] || [];
    filterHash[filterGroup].push(filterName);

    return filterHash;
  }, {});

  return JSON.stringify(filters);
};

/**
 * Helper function performing the typeahead search
 * @param   {Object} filterOptions  All Filters
 * @param   {String} filterToMatch  Filter name to match
 * @param   {Number} maxNum         Maximum Number of filters to display
 * @returns {Array}                 Array that contains the ouput of the search
 */
const getSearchResults = (filterOptions, filterToMatch, maxNum) => {
  let count = 0;
  const filters = filterOptions.reduce((foundFilters, filterOption) => {
    if (count > maxNum) {
      return [];
    }
    let options = filterOption.options.filter((el) => {
      return el.id.toLowerCase().includes(filterToMatch.toLowerCase());
    });

    if (options.length) {
      count += options.length;
      foundFilters.push({
        groupName: `${filterOption.groupName} (${options.length})`,
        options: options
      });
    }
    return foundFilters;
  }, []);

  const NoMatchMessage = count
    ? `Too Many results found (${count})`
    : 'No results found.';

  return [filters, NoMatchMessage];
};


export default Component.extend({
  // Maximum filters by filter group
  maxNumFilters: 25,

  // Maximum total filter to display
  maxTotalFilters: 500,

  triggerId: '',
  noMatchesMessage: '',

  classNames: 'filter-select',

  // selected Filters JSON
  selected: JSON.stringify({}),

  // all Filters Object
  options: {},

  disabled: false,

  /**
   * Takes the filters and massage them for the power-select grouping api
   * Currently not showing the whole list because of performance issues
   */
  filterOptions: computed('options', function() {
    const filters = this.get('options') || {};

    return buildFilterOptions(filters);
  }),

  // Selected Filters Serializer
  selectedFilters: computed('selected', {
    get() {
      const filters = JSON.parse(this.get('selected'));

      return convertHashToFilters(filters);
    },
    set(key, value) {
      const filters = convertFiltersToHash(value);
      this.set('selected', filters);

      return value;
    }
  }),

  // Initial filter View (subset of FilterOptions)
  viewFilterOptions: computed(
    'filterOptions.@each',
    'maxNumFilters',
    function() {
      const maxNumFilters = this.get('maxNumFilters');
      return [...this.get('filterOptions')].map((filter) => {
        const viewFilter = Object.assign({}, filter);
        viewFilter.groupName += ` (${viewFilter.options.length})`;

        if (viewFilter.options.length > maxNumFilters) {
          viewFilter.options = viewFilter.options[0];
        }
        return viewFilter;
      });
    }
  ),

  /**
   * Ember concurrency generator helper tasks
   * that performs the typeahead search
   */
  searchByFilterName: task(function* (filter) {
    yield timeout(600);

    const filterOptions = [...this.get('filterOptions')];
    const maxNum = this.get('maxTotalFilters');
    const [ filters, message ] = getSearchResults(filterOptions, filter, maxNum);
    this.set('noMatchesMessage', message);

    return filters;
  }),

  actions: {
    // Action handler for filter Selection/Deselection
    onFilterChange(filters) {
      const onChangeHandler = this.get('onChange');
      this.set('selectedFilters', filters);

      if (onChangeHandler) {
        onChangeHandler(this.get('selected'));
      }
    }
  }
});
