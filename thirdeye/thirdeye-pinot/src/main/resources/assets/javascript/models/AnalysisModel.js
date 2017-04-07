function AnalysisModel() {
  this.metric;
  this.metricId;
  this.timeRange;
  this.granularity;
  this.dimension;
  this.filters;
  this.currentStart;
  this.currentEnd;
  this.baselineStart;
  this.baselineEnd;
  this.compareMode;
}

AnalysisModel.prototype = {
  init: function () {
    this.metric = null;
    this.metricId = null;
    this.timeRange = null;
    this.granularity = null;
    this.dimension = null;
    this.filters = null;
    this.currentEnd = moment().subtract(3, 'hours').endOf('hour');
    this.currentStart = moment().subtract(2, 'hours').subtract(24, 'hours').startOf('hour');
    this.baselineStart = null;
    this.baselineEnd = null;
    this.compareMode = constants.DEFAULT_COMPARE_MODE ;
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
      this.filters = Object.assign({}, params.filters);
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
      this.granularity = result[0] || constants.DEFAULT_ANALYSIS_GRANULARITY;
      this.setDefaultCurrentDateRange(this.granularity);
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
  },

  setDefaultCurrentDateRange(granularity) {
    if (granularity === constants.GRANULARITY_DAY) {
      this.currentStart = moment().subtract(29, 'days').startOf('day');
      this.currentEnd =  moment().endOf('day');
    }
    this.granularity = granularity;
    this.baselineStart = this.currentStart.clone().subtract(7, 'days');
    this.baselineEnd = this.currentEnd.clone().subtract(7, 'days');
  }
};
