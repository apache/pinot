/**
 * Filter Bar Input Component
 * Component for input fields in a filter bar (i.e. dropdown, checkbox)
 * @module components/filter-bar-input
 * @property {string} eventType     - [required] type of event of subfilter (i.e. "anomaly", "holiday")
 * @property {object} config        - [required] config file to construct filter bar, passed by parent component,
 *                                  filter-bar
 * @property {string} label         - [required] label of input (i.e. "country", "region")
 * @property {string} type          - [required] type of input (i.e. dropdown, checkbox)
 * @property {object} attributesMap - [required] mapping between attributes and between attributes in events and input
 *                                      values in config, passed by parent component, filter-bar
 * @property {object} entities      - [required] list of entities from grandparent component, used to filter from
 * @property {function} onSelect    - [required] method passed down by parent component that is called when a filter is
 *                                    selected
 * @property {function} updateCache - [required] updates the urns cache to save filtered urns that is passed to onSelect
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
import _ from 'lodash';
import { findLabelMapping } from 'thirdeye-frontend/helpers/utils';

export default Ember.Component.extend({

  /**
   * @type Array
   * Default value for filter bar input
   */
  selected: [],

  /**
   * options to populate dropdown (required by power-select addon)
   * @type {Array}
   */
  options: Ember.computed(
    'labelMapping',
    'attributesMap',
    'eventType',
    function() {
      const { labelMapping, attributesMap, eventType } = this.getProperties('labelMapping', 'attributesMap', 'eventType');
      let inputValues = [];
      if (attributesMap && attributesMap[eventType] && labelMapping) {
        inputValues = Array.from(attributesMap[eventType][labelMapping]);
      }
      return inputValues;
    }
  ),

  actions: {
    /**
     * Handles selection of filter items within an event type.
     * @method onFilterSelection
     * @param {Array} selectedValue - selected value in the input
     */
    onSubfilterSelection(selectedValue) {
      // const { label, entities, onSelect, eventType, updateCache, config } = this.getProperties('label', 'entities', 'onSelect', 'eventType', 'updateCache', 'config');
      // const labelMapping = findLabelMapping(label, config);
      //
      this.set('selected', selectedValue);
      //
      // if (onSelect) {
      //   let urns;
      //   // If there are no filters, show all entities under that event type
      //   if (!selectedValue.length) {
      //     urns = Object.keys(entities).filter(urn => entities[urn].type == 'event' && entities[urn].eventType == eventType);
      //   } else {
      //     urns = Object.keys(entities).filter(urn => {
      //       if (entities[urn].attributes[labelMapping]) {
      //         return selectedValue.some(value => entities[urn].attributes[labelMapping].includes(value));
      //       }
      //     });
      //   }
      //   // Call parent's onSelect() in the route controller to filter entities based on a list of urns
      //   onSelect(urns);
      //
      //   // Call parent's updateCache() in the filter bar component to update the urns cache
      //   updateCache(eventType, urns);
      // }
      const { eventType, labelMapping, onFilterChange } = this.getProperties('eventType', 'labelMapping', 'onFilterChange');
      onFilterChange(eventType, labelMapping, selectedValue);
    }
  }
});
