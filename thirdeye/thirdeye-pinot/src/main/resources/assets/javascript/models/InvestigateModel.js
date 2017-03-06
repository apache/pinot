function InvestigateModel() {
  this.metricId;
}

InvestigateModel.prototype = {
  init: function (params) {
    if (params.metricId) {
      this.metricId = params.metricId;
    }
  },

  update: function (params) {
    if (params.metricId) {
      this.metricId = params.metricId;
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
};
