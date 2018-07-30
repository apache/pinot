/**
 * Component for "root cause dimension table settings" modal
 * @module components/rootcause-dimension-settings
 * @property {Array} dimensionOptions - field options for dimension selection
 * @property {Object} customTableSettings  - default settings for table custom options
 * These properties are added to the reqeust params in the advanced dimensions call
 *  {
      depth: '3',
      dimensions: [],
      excludedDimensions: [],
      summarySize: 20,
      oneSideError: 'false'
    }
 * @example
    {{rootcause-dimensions-settings
      dimensionOptions=dimensionOptions
      customTableSettings=customTableSettings
    }}
 * @exports rootcause-dimension-settings
 * @author smcclung
 */

import Component from '@ember/component';
import { reads } from '@ember/object/computed';
import { computed, get, set, getProperties, setProperties } from '@ember/object';

export default Component.extend({
  tagName: 'main',
  classNames: ['rootcause-dimensions-settings-modal'],

  // Field defaults
  dimensionOptions: [],
  customTableSettings: {},
  dimensionLevels: ['1', '2', '3'],
  errorOptions: ['false', 'true'],

  // One-way CP to the original incoming value for custom settings
  topContributors: reads('customTableSettings.summarySize'),
  selectedDimensionLevel: reads('customTableSettings.depth'),
  selectedErrorOption: reads('customTableSettings.oneSideError'),
  selectedIncludeDimensions: reads('customTableSettings.dimensions'),
  selectedExcludeDimensions: reads('customTableSettings.excludedDimensions'),

  // Mapping field keys to actual API queryparam keys
  fieldKeyMap: {
    selectedIncludeDimensions: 'dimensions',
    selectedExcludeDimensions: 'excludedDimensions',
    selectedDimensionLevel: 'depth',
    topContributors: 'summarySize',
    selectedErrorOption: 'oneSideError'
  },

  /**
   * Selection options for "include dimensions". Do not include options selected to exclude
   * @returns {Array} Dimension options to include
   */
  dimensionOptionsInclude: computed(
    'dimensionOptions',
    'selectedExcludeDimensions',
    function() {
      const { dimensionOptions, selectedExcludeDimensions } = getProperties(this, 'dimensionOptions', 'selectedExcludeDimensions');
      return dimensionOptions ? dimensionOptions.filter(option => !selectedExcludeDimensions.includes(option)) : [];
    }
  ),

  /**
   * Selection options for "exclude dimensions". Do not include options selected to include
   * @returns {Array} Dimension options to exclude
   */
  dimensionOptionsExclude: computed(
    'dimensionOptions',
    'selectedIncludeDimensions',
    function() {
      const { dimensionOptions, selectedIncludeDimensions } = getProperties(this, 'dimensionOptions', 'selectedIncludeDimensions');
      return dimensionOptions ? dimensionOptions.filter(option => !selectedIncludeDimensions.includes(option)) : [];
    }
  ),

  actions: {

    /**
     * Pipe changing field values into our 'customTableSettings' object given to us by the parent
     * @method onInput
     * @param {String} inputName - field name
     * @param {String} inputValue - field value
     */
    onInput(inputName, inputValue,) {
      const customTableSettings = get(this, 'customTableSettings');
      const apiKey = get(this, 'fieldKeyMap')[inputName];
      // Set shared custom settings object props
      set(this, `customTableSettings.${apiKey}`, inputValue);
      // Set triggered field value
      set(this, inputName, inputValue);
    }
  }
});
