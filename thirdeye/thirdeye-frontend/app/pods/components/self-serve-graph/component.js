/**
 * Wrapper component for the anomaly-graph component as used in the self-serve flow.
 * It handles presentation of the graph under various loading and error conditions.
 * @module components/self-serve-graph
 * @property {Boolean} isMetricDataLoading    - is primary metric data still loading
 * @property {Boolean} isMetricDataInvalid    - has the metric data been found to be not graphable
 * @property {Boolean} isDimensionFetchDone   - are we done preparing dimension data
 * @property {Boolean} isSecondaryDataLoading - loads spinner if peripheral data load is made
 * @property {Boolean} removeGraphMargin      - render with or without extra margins
 * @property {Object} metricData              - primary metric data
 * @property {Array} topDimensions            - top x dimensions to load aside from primary metric
 * @property {String} componentId             - id for the graph wrapper element
 * @example
    {{self-serve-graph
      isMetricDataLoading=false
      isMetricDataInvalid=false
      isDimensionFetchDone=false
      metricData=metricData
      topDimensions=topDimensions
      componentId='create-alert'
    }}
 * @exports self-serve-graph
 * @author smcclung
 */

import Component from '@ember/component';
import { computed, set } from '@ember/object';

export default Component.extend({
  classNames: ['col-xs-12', 'te-graph-container'],
  classNameBindings: ['removeGraphMargin:te-graph-container--marginless'],
  isMetricDataLoading: true,
  isMetricDataInvalid: false,
  isDimensionFetchDone: false,
  isSecondaryDataLoading: false,
  metricData: {},
  topDimensions: [],
  componentId: '',

  /**
   * Standard legend settings for graph
   */
  legendText: {
    dotted: {
      text: 'WoW'
    },
    solid: {
      text: 'Observed'
    }
  },

  /**
   * All selected dimensions to be loaded into graph
   * @returns {Array}
   */
  selectedDimensions: computed(
    'topDimensions',
    'topDimensions.@each.isSelected',
    function() {
      const topDimensions = this.get('topDimensions');
      return topDimensions ? this.get('topDimensions').filterBy('isSelected') : [];
    }
  ),

  /**
   * Determines pending state of graph
   * @returns {Boolean}
   */
  isMetricDataPending: computed(
    'isMetricSelected',
    'isMetricDataLoading',
    'isMetricDataInvalid',
    function() {
      const {
        isMetricSelected,
        isMetricDataLoading
      } = this.getProperties('isMetricSelected', 'isMetricDataLoading');
      return !this.get('isMetricSelected') || this.get('isMetricDataLoading') || this.get('isMetricDataInvalid');
    }
  ),

  /**
   * Determines whether peripheral data for the graph is loading
   * @returns {Boolean}
   */
  isAnythingLoading: computed.or('isMetricDataLoading', 'isSecondaryDataLoading'),

  /**
   * Sets the message text over the graph placeholder before data is loaded
   * @method graphMessageText
   * @return {String} the appropriate graph placeholder text
   */
  graphMessageText: computed(
    'isMetricDataInvalid',
    function() {
      const defaultMsg = 'Graph will appear here when data is loaded...';
      const invalidMsg = 'Sorry, I\'m not able to load data for this metric';
      return this.get('isMetricDataInvalid') ? invalidMsg : defaultMsg;
    }
  ),

  actions: {
    /**
     * Enable reaction to dimension toggling in graph legend component
     * @method onSelection
     * @return {undefined}
     */
    onSelection(selectedDimension) {
      const { isSelected } = selectedDimension;
      set(selectedDimension, 'isSelected', !isSelected);
    }
  }

});
