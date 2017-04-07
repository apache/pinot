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
}

AnalysisModel.prototype = {
  init: function () {
    this.metric = null;
    this.metricId = null;
    this.timeRange = null;
    this.granularity = null;
    this.dimension = null;
    this.filters = null;
    this.currentStart = moment().subtract(1, 'days');
    this.currentEnd = moment();
    this.baselineStart = null;
    this.baselineEnd = null;
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

  /**
   * Fetches the data for the analysis options template
   * @param {number} metricId The metric's id
   * @param {string} spinArea The id of the spinner element
   */
  fetchAnalysisOptionsData(metricId, spinArea) {
    const target = document.getElementById(spinArea);
    const spinner = new Spinner();
    spinner.spin(target);
    return this.fetchGranularityForMetric(metricId).then((granularity) => {
      this.granularityOptions = granularity;
      this.granularity = granularity[0] || constants.DEFAULT_ANALYSIS_GRANULARITY;
      this.setDefaultCurrentDateRange(this.granularity);
      return this.fetchDimensionsForMetric(metricId);
    }).then((dimensions) => {
      this.dimensionOptions = dimensions;
      return this.fetchFiltersForMetric(metricId);
    }).then((filter) => {
      this.filtersOptions = filter;
      return filter;
    }).done(() => {
      spinner.stop();
      return this;
    });
  },

  setDefaultCurrentDateRange(granularity) {
    if (granularity === constants.GRANULARITY_DAY) {
      this.currenStart = moment().subtract(30, 'days').startOf('day');
    }
    this.granularity = granularity;
    this.baselineStart = this.currentStart.clone().subtract(7, 'days');
    this.baselineEnd = this.currentEnd.clone().subtract(7, 'days');
  }
};
