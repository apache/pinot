import Ember from 'ember';
import { connect } from 'ember-redux';
import _ from 'lodash';

const colors = ['orange', 'teal', 'purple', 'red', 'green', 'pink'];
/**
 * Assigns colors to metric in the front end 
 * @param {Object} elem metric
 * @param {Number} index 
 */
const assignColor = (elem, index) => {
  elem.color = colors[index % colors.length];
  return elem;
};

/**
 * Determines if a metric should be filtered out
 * @param {Object} metric 
 * @returns {Boolean}
 */
const filterMetric = (metric) => {
  return metric 
  && metric.subDimensionContributionMap['All'].currentValues
  && metric.subDimensionContributionMap['All'].currentValues.reduce((total, val) => {
    return total + val;
  },0)
}

function select(store) {
  const {
    loading,
    loaded,
    failed,
    relatedMetricEntities = {},
    relatedMetricIds,
    regions,
    primaryMetricId,
    compareMode,
    granularity,
  } = store.metrics;

  const uiRelatedMetric = _.merge({}, relatedMetricEntities, regions);

  return {
    loading,
    loaded,
    failed,
    compareMode,
    granularity,
    primaryMetric: uiRelatedMetric[primaryMetricId],
    relatedMetrics: relatedMetricIds
      .map(id => uiRelatedMetric[id])
      .filter(filterMetric)
      .map(assignColor)
  };
}

function actions() {
  return {};
}

export default connect(select, actions)(Ember.Component.extend({
}));
