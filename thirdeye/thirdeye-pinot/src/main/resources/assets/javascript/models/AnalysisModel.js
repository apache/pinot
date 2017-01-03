function AnalysisModel() {
  this.metric;
  this.metricId;
  this.timeRange;
  this.granularity;
  this.dimension;
  this.filters;

  var currentTime = moment();
  currentTime = currentTime - (currentTime % 3600000) - 2 * 3600000;  // 2 hours behind current time
  this.currentStart = moment(currentTime).subtract(4, 'days');
  this.currentEnd = moment(currentTime);
  this.baselineStart= moment(currentTime).subtract(10, 'days');
  this.baselineEnd = moment(currentTime).subtract(6, 'days');
}

AnalysisModel.prototype = {
  init: function (params) {
  },

  update: function (params) {
    console.log("Logging hash params");
    console.log(params);
    if (params.metricId) {
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

  fetchGranularityForMetric: function (metricId) {
    return dataService.fetchGranularityForMetric(metricId);
  },

  fetchDimensionsForMetric : function(metricId) {
    return dataService.fetchDimensionsForMetric(metricId);
  },

  fetchFiltersForMetric : function(metricId) {
    return dataService.fetchFiltersForMetric(metricId);
  }
}
