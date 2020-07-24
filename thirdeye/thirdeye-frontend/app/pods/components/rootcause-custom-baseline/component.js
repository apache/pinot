/**
 * Component for "rootcause custom baseline" modal
 * @module components/rootcause-custom-baseline
 * @example
    {{rootcause-custom-baseline}}
 * @exports rootcause-custom-baseline
 * @author hjackson
 */

import Component from '@ember/component';
import {
  set,
  computed,
  getProperties
} from '@ember/object';

const UNIT_OVER_UNIT = 'Unit over x units';
const MEAN_UNIT = 'Mean of x units';
const MEDIAN_UNIT = 'Median of x units';
const MIN_UNIT = 'Min of x units';
const MAX_UNIT = 'Max of x units';

export default Component.extend({
  tagName: 'main',
  classNames: ['rootcause-custom-baseline-modal'],

  selectedBaselineType: 'Unit over x units',

  baselineTypes: [UNIT_OVER_UNIT, MEAN_UNIT, MEDIAN_UNIT, MAX_UNIT, MIN_UNIT],

  selectedTimeUnit: 'Week',

  timeUnits: ['Month', 'Week', 'Day', 'Hour'],

  selectedNumber: 1,

  /**
   * Baseline derived from selections made by user
   * @returns {String} baseline to request
   */
  configuredBaseline: computed(
    'selectedBaselineType',
    'selectedTimeUnit',
    'selectedNumber',
    function() {
      const {
        selectedBaselineType,
        selectedTimeUnit,
        selectedNumber
      } = getProperties(this, 'selectedBaselineType', 'selectedTimeUnit', 'selectedNumber');
      const unitAbbreviation = selectedTimeUnit.charAt(0).toLowerCase();
      switch(selectedBaselineType) {
        case UNIT_OVER_UNIT: {
          return `${unitAbbreviation}o${selectedNumber}${unitAbbreviation}`;
        }
        case MEAN_UNIT: {
          return `mean${selectedNumber}${unitAbbreviation}`;
        }
        case MEDIAN_UNIT: {
          return `median${selectedNumber}${unitAbbreviation}`;
        }
        case MAX_UNIT: {
          return `max${selectedNumber}${unitAbbreviation}`;
        }
        case MIN_UNIT: {
          return `min${selectedNumber}${unitAbbreviation}`;
        }
        default: {
          return 'wo1w';
        }
      }
    }
  ),

  actions: {

    /**
     * @method onSelectBaselineType
     * @param {String} baselineType - the baseline type selected by user
     */
    onSelectBaselineType(baselineType) {
      set(this, 'selectedBaselineType', baselineType);
      this.get('bubbleBaseline')(this.get('configuredBaseline'));
    },

    /**
     * @method onSelectTimeUnit
     * @param {String} timeUnit - the unit of time selected by the user
     */
    onSelectTimeUnit(timeUnit) {
      set(this, 'selectedTimeUnit', timeUnit);
      this.get('bubbleBaseline')(this.get('configuredBaseline'));
    },

    /**
     * @method onInputNumber
     * @param {Number} numericValue - integer
     */
    onInputNumber(numericValue) {
      set(this, 'selectedNumber', numericValue);
      this.get('bubbleBaseline')(this.get('configuredBaseline'));
    }
  }
});
