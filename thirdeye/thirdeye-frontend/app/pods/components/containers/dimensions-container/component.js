import Component from '@ember/component';
import { connect } from 'ember-redux';


function select(store) {
  const {
    keys,
    loading,
    loaded,
    failed,
    timeseries,
    dimensions,
    selectedDimension,
    heatmapData,
    regionStart,
    regionEnd,
    heatmapLoaded
  } = store.dimensions;

  const {
    granularity,
    selectedDimensions
  } = store.primaryMetric;

  const dimensionKeys = Object.keys(timeseries.subDimensionContributionMap || {});

  return {
    keys,
    loading,
    loaded,
    failed,
    regionStart,
    regionEnd,
    subdimensions: dimensionKeys
      .filter(key => key.length && key !== 'All')
      .map((key) => {
        const keyName = `${selectedDimension}-${key}`;
        const subDimension = Object.assign({}, dimensions[keyName]);

        if (subDimension && selectedDimensions.includes(keyName)) {
          subDimension.isSelected = true;
        }
        return subDimension;
      }),
    dimensionKeys,
    granularity,
    heatmapData,
    heatmapLoaded
  };
}

function actions() {
  return {};
}

export default connect(select, actions)(Component.extend({
}));

