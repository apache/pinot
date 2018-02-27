/**
 * Filter Bar Component
 * Constructs a filter bar based on a config file
 * @module components/filter-bar
 * @property {object[]} config            - [required] array of objects (config file) passed in from the route that sets
 *                                          up the filter bar sub-filters
 * @property {object} entities            - [required] data used to filter
 * @property {array}  onFilterSelection   - [required] closure action to bubble to controller on filter selection
 *
 * @example
 * {{filter-bar
 *   config=filterBarConfig
 *   entities=entities
 *   onFilter=(action "onFilterSelection")}}
 *
 * @exports filter-bar
 */
import Component from "@ember/component";
import {
  get,
  set,
  setProperties,
  computed,
  getProperties,
  observer
} from "@ember/object";
import _ from 'lodash';

export default Component.extend({

  /**
   * Cache for all filters selected (event type + subfilters)
   */
  filterCache: null, // { holiday: { countryCode: 'US' } }

  /**
   * Currently selected event
   */
  eventType: null, // 'holiday'

  /**
   * Mock data for every dropdown options
   * This will be an API call
   */
  options: ['All', 'None'],

  /**
   * Cache for urns, filtered by search criteria
   * @type {Object}
   * @example
   * {
   *  anomaly: {urns1, urns2},
   *  holiday: {urns3, urns4}
   * }
   */
  urnsCache: {},

  /**
   * Set of frameworks currently loading
   * @type {Set}
   */
  loadingFrameworks: {},

  /**
   * Overwrite the init function
   * Initializes values of the filter blocks
   * Example of a filter block:
   * {
   *  eventType: 'holiday',
   *  filtersArray: [
   *    {
   *      isActive: false,
   *      name: 'Country',
   *      id: 'country'
   *    }
   *  ],
   *  header: 'holiday',
   *  inputs: [
   *    {
   *      label: 'country',
   *      type: 'dropdown
   *    }
   *  ]
   *  isHidden: true,
   * }
   */
  init() {
    this._super(...arguments);

    // Fetch the config file to create sub-filters
    const filterBlocks = _.cloneDeep(get(this, 'config'));

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
      setProperties(block, {
        filtersArray,
        isHidden
      });
    });

    const eventType = filterBlocks[0].eventType;

    setProperties(this, { filterBlocks, eventType, filterCache: {} });
  },

  /**
   * Triggered when changes are made to attributes by components outside of filter bar
   * (i.e. changes in search form), filter results based on newly updated filteredUrns
   */
  didReceiveAttrs() {
    const { filteredUrns, onFilter } = getProperties(this, 'filteredUrns', 'onFilter');
    onFilter(filteredUrns);
  },

  /**
   * observer on entities to set default event type when entities is loaded
   * @type {undefined}
   */
  filteredUrnsObserver: observer(
    'eventType',
    'entities',
    'filterCache',
    'filteredUrns',
    function() {
      const { filteredUrns, onFilter } = getProperties(this, 'filteredUrns', 'onFilter');
      onFilter(filteredUrns);
    }
  ),

  /**
   * Returns object that represents mapping between attributes in events and input values in config
   * Example:
   * {
   *  holiday: {
   *    countryCode: ['US', 'IN', 'MX'],
   *    region: ['North America', 'Antarctica']
   *  },
   *  deployment: {
   *    fabric: ['prod', 'local']
   *  }
   * }
   * @type {Object}
   */
  attributesMap: computed(
    'entities',
    function () {
      let entities = get(this, 'entities');
      let map = {};
      if (!_.isEmpty(entities)) {
        // Iterate through each event
        Object.keys(entities).forEach(key => {
          const event = entities[key];
          const eventType = event.eventType;
          if (!(eventType in map)) {
            map[eventType] = {};
          }

          // Iterate through each property of event's attributes object
          Object.keys(event.attributes).forEach(attr => {
            // Check if the attribute is in the config file
            if (this.isInConfig(attr)) {
              if (!(attr in map[eventType])) {
                map[eventType][attr] = new Set();
              }
              const attrSet = map[eventType][attr];

              event.attributes[attr].forEach(val => attrSet.add(val));
            }
          });
        });
      }

      return map;
    }
  ),

  /**
   * Helper method to check whether an attribute is in the config
   * @method isInConfig
   * @param {String} attribute - name of attribute to check against config's input's labelMapping
   */
  isInConfig(attribute) {
    return get(this, 'filterBlocks').some(filterBlock => filterBlock.inputs.some(input => attribute === input.labelMapping));
  },

  /**
   * Computes the filtered urns when there are changes to entities, filterCache, and eventType
   */
  filteredUrns: computed(
    'entities',
    'filterCache',
    'eventType',
    function () {
      const { entities, filterCache, eventType } =
        getProperties(this, 'entities', 'filterCache', 'eventType');

      if (!eventType) { return []; }

      const filters = filterCache[eventType] || {};

      const filteredUrns = Object.keys(entities).filter(urn => {
        const e = entities[urn];
        return e.type == 'event' && e.eventType == eventType && this.applyFilters(e, filters);
      });

      return filteredUrns;
    }
  ),

  /**
   * A mapping of event type and number of events of that type
   * @type {Object}
   */
  eventTypeMap: computed(
    'entities',
    function() {
      let holidayCount = 0;
      let gcnCount = 0;
      let lixCount = 0;
      let informedCount = 0;
      let anomalyCount = 0;
      const entities = get(this, 'entities');

      Object.keys(entities).forEach(urn => {
        const e = entities[urn];
        if (e.type === 'event') {
          switch(e.eventType) {
            case 'holiday':
              holidayCount++;
              break;
            case 'gcn':
              gcnCount++;
              break;
            case 'lix':
              lixCount++;
              break;
            case 'informed':
              informedCount++;
              break;
            case 'anomaly':
              anomalyCount++;
              break;
          }
        }
      });

      return {
        holiday: holidayCount,
        gcn: gcnCount,
        lix: lixCount,
        informed: informedCount,
        anomaly: anomalyCount
      };
    }
  ),

  /**
   * Determines whether to apply filters
   * @param {Object} entity
   * @param {Object} filters - subfilters selected (i.e. {countryCode: 'DE'})
   */
  applyFilters(e, filters) {
    if (_.isEmpty(filters)) {
      return true;
    }

    return Object.keys(filters).every((dimName) => {
      return !filters[dimName].size
        || (e.attributes[dimName] && e.attributes[dimName].some(dimValue => filters[dimName].has(dimValue)));
    });
  },

  actions: {
    /**
     * Closure action, called by sub-component (filter-bar-input) whenever a subfilter is selected
     * @param {String} eventType
     * @param {String} labelMapping
     * @param {String[]} selectedValues
     */
    onFilterChange(eventType, labelMapping, selectedValues) {
      const filterCache = get(this, 'filterCache');

      if (!filterCache[eventType]) {
        filterCache[eventType] = {};
      }

      filterCache[eventType][labelMapping] = new Set(selectedValues);

      setProperties(this, {
        filterCache: Object.assign({}, filterCache),
        eventType });
    },

    /**
     * Triggered when user selects an event type (down arrow in filter bar)
     * @param {String} eventType
     */
    selectEventType(eventType) {
      set(this, 'eventType', eventType);

      let filterBlocks = get(this, 'filterBlocks');

      // Hide all other blocks when one is clicked
      filterBlocks.forEach(block => {
        const isHidden = block.eventType !== eventType;
        set(block, 'isHidden', isHidden);
      });
    }
  }
});
