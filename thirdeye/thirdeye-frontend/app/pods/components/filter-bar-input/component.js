/**
 * Filter Bar Input Component
 * Component for input fields in a filter bar (i.e. dropdown, checkbox)
 * @module components/filter-bar-input
 * @property {string} eventType     - [required] type of event of subfilter (i.e. "anomaly", "holiday")
 * @property {string} labelMapping  - [required] id for type of sub-filter based on the filter bar config
 * @property {string} label         - [required] label of input (i.e. "country", "region")
 * @property {string} type          - [required] type of input (i.e. dropdown, checkbox)
 * @property {object} attributesMap - [required] mapping between attributes and between attributes in events and input
 *                                      values in config, passed by parent component, filter-bar
 * @property {function} onFilterChange - [required] closure action to bubble up to parent component to update filter
 *                                        cache
 * @example
 * {{filter-bar
 *   eventType=eventType
 *   labelMapping=labelMapping
 *   label=filter.label
 *   type=filter.type
 *   attributesMap=attributesMap
 *   onFilterChange=(action "onFilterChange")}}
 *
 * @exports filter-bar
 */
import { computed } from '@ember/object';

import Component from '@ember/component';

export default Component.extend({

  /**
   * @type Array
   * Default value for selected values in the filter bar input
   */
  selected: [],

  /**
   * options to populate dropdown (required by power-select addon)
   * @type {Array}
   */
  options: computed(
    'labelMapping',
    'attributesMap',
    'eventType',
    function() {
      const { labelMapping, attributesMap, eventType } = this.getProperties('labelMapping', 'attributesMap', 'eventType');

      if (!attributesMap || !attributesMap[eventType] || !labelMapping) {
        return [];
      }

      return Array.from(attributesMap[eventType][labelMapping] || []).sort();
    }
  ),

  actions: {
    /**
     * Handles selection of filter items within an event type.
     * @method onFilterSelection
     * @param {Array} selectedValue - selected value in the input
     */
    onSubfilterSelection(selectedValue) {
      this.set('selected', selectedValue);
      const { eventType, labelMapping, onFilterChange } = this.getProperties('eventType', 'labelMapping', 'onFilterChange');
      onFilterChange(eventType, labelMapping, selectedValue);
    }
  }
});
