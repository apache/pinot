function InvestigateModel() {
  this.metricId;
  this.startDate = moment().subtract(1, 'days').startOf('day');
  this.endDate = moment().subtract(0, 'days').startOf('day');
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
