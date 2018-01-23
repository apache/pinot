/*    {{self-serve-graph
      isMetricDataLoading=isMetricDataLoading
      isMetricDataInvalid=isMetricDataInvalid
      isDimensionFetchDone=isDimensionFetchDone
      isMetricSelected=isMetricSelected
      metricData=metricData
      topDimensions=topDimensions
      componentId='create-alert'
    }}*/

import Component from '@ember/component';
import { computed } from '@ember/object';

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
    console.log('topDimensions : ', topDimensions);
    const topDimensions = this.get('topDimensions');
    return topDimensions ? this.get('topDimensions').filterBy('isSelected') : [];
  }
),

didReceiveAttrs() {
    this._super(...arguments);



},

actions: {
    /**
     * Enable reaction to dimension toggling in graph legend component
     * @method onSelection
     * @return {undefined}
     */
    onSelection(selectedDimension) {
      const { isSelected } = selectedDimension;
      Ember.set(selectedDimension, 'isSelected', !isSelected);
    },

    /**
     * Handles the primary metric selection in the alert creation
     */
    onPrimaryMetricToggle() {
      return;
    }
}

});
