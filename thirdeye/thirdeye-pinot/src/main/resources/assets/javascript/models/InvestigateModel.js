function InvestigateModel() {
  this.anomalyId;
  this.startDate = moment().subtract(1, 'days').startOf('day');
  this.endDate = moment().subtract(0, 'days').startOf('day');
  this.pageNumber = 1;
  this.functionName = '';
  this.renderViewEvent = new Event();
}

InvestigateModel.prototype = {
  init: function ({ anomalyId }) {
    this.anomalyId = anomalyId;
    this.fetchMetricInformation();
  },

  update: function (params) {
    if (params.metricId) {
      this.metricId = params.metricId;
    }
  },

  fetchMetricInformation() {
    dataService.fetchAnomaliesForAnomalyIds(
          this.startDate, this.endDate, this.pageNumber, this.anomalyId, this.functionName, this.updateModelAndNotifyView.bind(this));
  },

  getAnomaliesWrapper : function() {
    return this.anomaliesWrapper;
  },

  updateModelAndNotifyView(anomaliesWrapper) {
    this.anomaliesWrapper = anomaliesWrapper;
    this.renderViewEvent.notify();
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
