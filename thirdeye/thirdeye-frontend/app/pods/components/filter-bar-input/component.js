/**
 * Filter Bar Input Component
 * Component for input fields in a filter bar (i.e. dropdown, checkbox)
 * @module components/filter-bar-input
 * @property {string} header        - [required] header of subfilter (i.e. "Holiday", "Deployment")
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
    'label',
    'attributesMap',
    function() {
      const { label, attributesMap, config, eventType } = this.getProperties('label', 'attributesMap', 'config', 'eventType');
      const labelMapping = findLabelMapping(label, config);
      let inputValues = '';
      if (!_.isEmpty(attributesMap) && labelMapping) {
        inputValues = Array.from(attributesMap[eventType.toLowerCase()][labelMapping]);
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
      const { label, filterUrns, eventType } = this.getProperties('label', 'filterUrns', 'eventType');
      const labelMapping = findLabelMapping(label, this.get('config'));

      // Saves the selected values in UI
      this.set('selected', selectedValue);

      const subFilter = {
        key: labelMapping,
        value: selectedValue
      };

      filterUrns(eventType, subFilter);
    }
  }
});
