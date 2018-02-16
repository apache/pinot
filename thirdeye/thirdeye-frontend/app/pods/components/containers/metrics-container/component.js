import Component from '@ember/component';
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
    granularity,
    regionStart,
    regionEnd
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
    regionStart,
    regionEnd,
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

export default connect(select, actions)(Component.extend({
}));
