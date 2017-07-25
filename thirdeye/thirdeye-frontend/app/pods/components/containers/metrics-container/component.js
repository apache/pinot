import Ember from 'ember';
import { connect } from 'ember-redux';
import _ from 'lodash';

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
    granularity
  } = store.metrics;

  const {
    selectedMetricIds
  } = store.primaryMetric;

  const uiRelatedMetric = _.merge({}, relatedMetricEntities, regions);

  return {
    loading,
    loaded,
    failed,
    compareMode,
    granularity,
    primaryMetric: uiRelatedMetric[primaryMetricId],
    relatedMetrics: relatedMetricIds
      .map((id) => {
        const relatedMetric = uiRelatedMetric[id];

        if (selectedMetricIds.includes(id)) {
          relatedMetric.isSelected = true;
        }

        return relatedMetric;
      }).filter(metric => metric && metric.metricName)
  };
}

// no action defined for this container
function actions() {
  return {};
}

export default connect(select, actions)(Ember.Component.extend({
}));
