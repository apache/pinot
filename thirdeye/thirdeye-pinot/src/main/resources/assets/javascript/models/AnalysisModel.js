function AnalysisModel() {
  this.metric;
  this.metricId;
  this.timeRange;
  this.granularity;
  this.dimension;
  this.filters;
  this.currentStart;
  this.currentEndl;
  this.baselineStart;
  this.baselineEnd;
}

AnalysisModel.prototype = {
  init: function (params) {
  },

  update: function (params) {
    if (params.metricId) {
      this.metricId = params.metricId;
      this.metricName = this.fetchMetricName(params.metricId).name;
    }
    if (params.timeRange) {
      this.timeRange = params.timeRange;
    }
    if (params.granularity) {
      this.granularity = params.granularity;
    }
    if (params.dimension) {
      this.dimension = params.dimension;
    }
    if (params.filters) {
      this.filters = params.filters;
    }
    if (params.currentStart) {
      this.currentStart = params.currentStart;
    }
    if (params.currentEnd) {
      this.currentEnd = params.currentEnd;
    }
    if (params.baselineStart) {
      this.baselineStart = params.baselineStart;
    }
    if (params.baselineEnd) {
      this.baselineEnd = params.baselineEnd;
    }
  },

  fetchMetricName(metricId) {
    return dataService.fetchMetricByMetricId(metricId);
  },

  fetchGranularityForMetric(metricId) {
    return dataService.fetchGranularityForMetric(metricId);
  },

  fetchDimensionsForMetric(metricId) {
    return dataService.fetchDimensionsForMetric(metricId);
  },

  fetchFiltersForMetric(metricId) {
    return dataService.fetchFiltersForMetric(metricId);
  },

  fetchAnalysisOptionsData(metricId, spinArea) {
    const target = document.getElementById(spinArea);
    const spinner = new Spinner();
    spinner.spin(target);
    return this.fetchGranularityForMetric(metricId).then((result) => {
      this.granularityOptions = result;
      return this.fetchDimensionsForMetric(metricId);
    }).then((result) => {
      this.dimensionOptions = result;
      return this.fetchFiltersForMetric(metricId);
    }).then((result) => {
      this.filtersOptions = result;
      return result;
    }).then(() => {
      spinner.stop();
      return this;
    });
  }
};
