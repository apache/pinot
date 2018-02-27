/**
 * Wrapper component for the anomaly-graph component as used in the self-serve flow.
 * It handles presentation of the graph under various loading and error conditions.
 * @module components/self-serve-graph
 * @property {Boolean} isMetricDataLoading  - is primary metric data still loading
 * @property {Boolean} isMetricDataInvalid  - has the metric data been found to be not graphable
 * @property {Boolean} isDimensionFetchDone - are we done preparing dimension data
 * @property {Object} metricData            - primary metric data
 * @property {Array} topDimensions          - top x dimensions to load aside from primary metric
 * @property {String} componentId           - id for the graph wrapper element
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

  isMetricDataLoading: true,
  isMetricDataInvalid: false,
  isDimensionFetchDone: false,
  metricData: {},
  topDimensions: [],
  componentId: '',

  graphMessageText: {
    loading: 'Graph will appear here when data is loaded...',
    error: 'Could not load graph data for this metric...'
  },

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
