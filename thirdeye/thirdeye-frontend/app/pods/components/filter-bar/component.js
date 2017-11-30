/**
 * Filter Bar Component
 * Constructs a filter bar based on a config file
 * @module components/filter-bar
 * @property {object[]} config            - [required] array of objects (config file) passed in from the route that sets
 *                                          up the filter bar sub-filters
 * @property {object[]} filterBlock       - [required] array of objects constructed from config to render filter bar
 * @property {number} maxStrLen           - [optional] number of characters for filter name truncation
 * @property {array}  onFilterSelection   - [required] closure action to bubble to controller on filter selection change
 *
 * @example
 * {{filter-bar
 *   config=filterBarConfig
 *   filterBlocks=filterBlocks
 *   entities=entities
 *   maxStrLen=25
 *   onSelectFilter=(action "onFilterSelection")}}
 *
 * @exports filter-bar
 */
import Ember from 'ember';
import _ from 'lodash';

export default Ember.Component.extend({

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
   * Observer on entities to set default event type when new entities are loaded
   * @type {undefined}
   */
  entitiesObserver: Ember.observer(
    'entities',
    function() {
      const { config: filterBlocks, entities } = this.getProperties('config', 'entities');

      // When entities are changed (i.e. from user interacting with the selection form), recompute urns to cache
      filterBlocks.forEach(block => {
        let urns = Object.keys(entities).filter(urn => entities[urn].type == 'event'
                                                  && entities[urn].eventType == block.eventType);
        this.urnsCache[block.eventType] = urns;
      });

      // Have the first block selected by default
      this.send('selectEventType', filterBlocks[0]);
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
  attributesMap: Ember.computed(
    'entities',
    function () {
      let filteredEvents = this.get('entities');
      let map = {};
      if (!_.isEmpty(filteredEvents)) {
        // Iterate through each event
        Object.keys(filteredEvents).forEach(key => {
          const event = filteredEvents[key];
          const eventType = event.eventType;
          // Iterate through each property of event's attributes object
          Object.keys(event.attributes).forEach(attr => {
            // Check if the attribute is in the config file
            if (this.isInConfig(attr)) {
              // If map has a key of eventType (i.e. holidays, GCN, Lix)
              if (map.hasOwnProperty(eventType)) {
                // If eventType object in map doesn't have this attribute, add values to existing value set
                if (map[eventType].hasOwnProperty(attr)) {
                  let attrSet = map[eventType][attr];
                  let values = event.attributes[attr];
                  values.forEach(val => attrSet.add(val));
                }
                // If eventType object in map does have this attribute, create a set with those values
                else {
                  // Create set and add values there
                  map[eventType][attr] = new Set(event.attr);
                }
              }
              // If map doesn't have a key of eventType (i.e. holidays, GCN, Lix)
              else {
                let obj = { [attr]: new Set() };
                map[eventType] = obj;
              }
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
    return this.get('config').some(filterBlock => filterBlock.inputs.some(input => attribute === input.labelMapping));
  },

  actions: {

    /**
     * Expands/collapses a filter block
     * @method filterByEvent
     * @param {Object} clickedBlock - selected filter block object
     */
    selectEventType(clickedBlock) {
      const { entities, onSelect } = this.getProperties('entities', 'onSelect');
      const cachedEventType = this.urnsCache[clickedBlock.eventType];
      let filterBlocks = this.get('config');

      // Hide all other blocks when one is clicked
      filterBlocks.forEach(block => {
        Ember.set(block, 'isHidden', true);
      });

      // Note: toggleProperty will not be able to find 'filterBlocks', as it is not an observed property
      // Show clickedBlock
      Ember.set(clickedBlock, 'isHidden', !clickedBlock.isHidden);

      /*
      * If results were already previously filtered for this filter block (i.e. "anomaly", "holiday"),
      * call onSelect on the cached urns
      */
      if (!Ember.isEmpty(cachedEventType)) {
        onSelect(cachedEventType);
      }
      // If this is the first time results are computed, cache them
      else {
        const urns = Object.keys(entities).filter(urn => entities[urn].type == 'event'
                                                  && entities[urn].eventType == clickedBlock.eventType);
        this.urnsCache[clickedBlock.eventType] = urns;
        onSelect(urns);
      }
    },

    /**
     * Bubbled up action, called by the child component, filter-bar-input component, to update the urns cache
     * @method updateCache
     * @param {String} eventType - type of event of filter block (i.e. "anomaly", "holiday")
     * @param {Array} urns - list of urns that are filtered based on subfilters (i.e. country, region)
     */
    updateCache(eventType, urns) {
      this.urnsCache[eventType] = urns;
    }
  }
});
