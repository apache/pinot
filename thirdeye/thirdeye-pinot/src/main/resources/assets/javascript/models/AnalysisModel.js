function AnalysisModel() {
  this.metric;
  this.metricName;
  this.dataset;
  this.metricId;
  this.timeRange;
  this.dateRange;
  this.granularity;
  this.dimension;
  this.filters;
  this.currentStart;
  this.currentEnd;
  this.baselineStart;
  this.baselineEnd;
  this.compareMode;

  this.rootCauseData;
  this.rootCausePipeline;
  this.rootCauseEnabled;
}

AnalysisModel.prototype = {
  init() {
    this.metric = null;
    this.metricId = null;
    this.timeRange = null;
    this.granularity = null;
    this.dimension = null;
    this.filters = null;
    this.currentEnd = null;
    this.currentStart = null;
    this.baselineStart = null;
    this.baselineEnd = null;
    this.metricName = null;
    this.dataset = null;
    this.compareMode = constants.DEFAULT_COMPARE_MODE ;
    this.rootCauseData = null;
    this.rootCauseEnabled = false;
  },

  update: function (params) {
    if (params.metricId) {
      const { name, dataset } = this.fetchMetricData(params.metricId);
      this.metricName = name;
      this.dataset = dataset;
      this.metricId = params.metricId;
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
    if (params.rootCauseEnabled) {
      this.rootCauseEnabled = params.rootCauseEnabled;
    }
  },

  fetchMetricData(metricId) {
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

  fetchMaxTimeForMetric(metricId) {
    return dataService.fetchMaxTimeForMetric(metricId);
  },

  fetchRootCauseData() {
    const windowSize = this.currentEnd - this.currentStart;
    const urn = `thirdeye:metric:${this.metricId}`;
    return dataService.fetchRootCauseData(this.currentStart, this.baselineStart, windowSize, urn);
  },

  /**
   * fetch the analysis form options for a metric
   * @param  {number} metricId the ID of the searched Metric
   * @param  {string} spinArea id selector of the spinner
   */
  fetchAnalysisOptionsData(metricId, spinArea) {
    const target = document.getElementById(spinArea);
    const spinner = new Spinner();
    spinner.spin(target);

    return this.fetchMaxTimeForMetric(metricId).then((maxTime)=> {
      const maxTimeMoment = moment(maxTime);
      this.maxTime = maxTimeMoment.isValid() ? maxTimeMoment : moment();
      this.setEndDateMaxTime();
      return this.fetchGranularityForMetric(metricId);
    }).then((result) => {
      this.granularityOptions = result;
      this.granularity = result[0] || constants.DEFAULT_ANALYSIS_GRANULARITY;
      this.setDefaultDateRange(this.granularity);
      return this.fetchDimensionsForMetric(metricId);
    }).then((result) => {
      this.dimensionOptions = result;
      return this.fetchFiltersForMetric(metricId);
    }).then((result) => {
      this.filtersOptions = result;
      if (this.rootCauseEnabled) {
        return this.fetchRootCauseData();
      } else {
        return null;
      }
    }).then((result) => {
      this.rootCauseData = result;
      return result;
    }).then(() => {
      spinner.stop();
      return this;
    });
  },


  /**
   * Makes sure that the time inputted by the user are not
   * beyond the metric's max time
   */
  setEndDateMaxTime() {
    const {
      maxTime,
      currentEnd,
      baselineEnd
    } = this;

    if (currentEnd) {
      this.currentEnd = moment.min(currentEnd, maxTime).clone();
    }

    if (baselineEnd) {
      this.baselineEnd = moment.min(baselineEnd, maxTime).clone();
    }
  },

  /**
   * Initialize the date range based on the metric's granularity
   * @param {string} granularity granularity of the searched metric
   */
  setDefaultDateRange(granularity) {
    if (this.currentEnd && this.currentStart && this.baselineStart && this.baselineEnd) return;
    const maxTime = this.maxTime;
    if (granularity === constants.GRANULARITY_DAY) {
      this.currentStart = moment().subtract(29, 'days').startOf('day');
    } else {
      this.currentStart = moment().clone().subtract(24, 'hours').startOf('hour');
    }
    this.currentEnd = maxTime.clone();
    this.granularity = granularity;
    this.baselineStart = this.currentStart.clone().subtract(7, 'days');
    this.baselineEnd = this.currentEnd.clone().subtract(7, 'days').endOf('day');
  }
};
