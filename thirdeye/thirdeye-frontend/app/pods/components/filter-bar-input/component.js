import Ember from 'ember';
import _ from 'lodash';

export default Ember.Component.extend({

  /**
   * options to populate dropdown (required by power-select addon)
   * @type {Array}
   */
  options: Ember.computed(
    'label',
    'attributesMap',
    function() {
      const { label, attributesMap, config, header } = this.getProperties('label', 'attributesMap', 'config', 'header');
      const labelMapping = this.findLabelMapping(label, config);
      let inputValues = '';
      if (!_.isEmpty(attributesMap) && labelMapping) {
        inputValues = Array.from(attributesMap[header.toLowerCase()][labelMapping]);
      }

      return inputValues;
    }
  ),

  /**
   * finds the corresponding labelMapping field given a label in the filterBarConfig
   * This is only a placeholder since the filterBarConfig is not finalized
   */
  findLabelMapping(label, config) {
    let labelMapping = '';
    config.some(filterBlock => filterBlock.inputs.some(input => {
      if (input.label === label) {
        labelMapping = input.labelMapping;
      }
    }));
    return labelMapping;
  }
});
