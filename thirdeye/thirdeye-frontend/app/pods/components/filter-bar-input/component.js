import Ember from 'ember';
import _ from 'lodash';
import { findLabelMapping } from 'thirdeye-frontend/helpers/utils';

export default Ember.Component.extend({


  /**
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
      const { label, attributesMap, config, header } = this.getProperties('label', 'attributesMap', 'config', 'header');
      const labelMapping = findLabelMapping(label, config);
      let inputValues = '';
      if (!_.isEmpty(attributesMap) && labelMapping) {
        inputValues = Array.from(attributesMap[header.toLowerCase()][labelMapping]);
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
      const { label, entities, onSelect, header } = this.getProperties('label', 'entities', 'onSelect', 'header');
      const labelMapping = findLabelMapping(label, this.get('config'));

      this.set('selected', selectedValue);

      if (onSelect) {
        let urns;
        // If there are no filters, show all events under that event type
        if (selectedValue.length == 0) {
          urns = Object.keys(entities).filter(urn => entities[urn].type == 'event'
                                                    && entities[urn].eventType == header.toLowerCase());
        } else {
          urns = Object.keys(entities).filter(urn => {
            if (entities[urn].attributes[labelMapping]) {
              return selectedValue.some(value => entities[urn].attributes[labelMapping].includes(value));
            }
          });
        }

        onSelect(urns);
      }
    }
  }
});
