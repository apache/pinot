function DataService() {
  this.URL_SEPARATOR = '/';
}

DataService.prototype = {

    // Make synchronous get call
    getDataSynchronous: function(url, data)  {
      console.log("request url:", url)
      var results = undefined;
      $.ajax({
        url: url,
        data: data,
        type: 'get',
        dataType: 'json',
        async: false,
        success: function(data) {
          results = data;
        },
        error: function(e) {
          console.log(e);
        }
      });
      return results;
    },
    // Make asynchronous get call
    getDataAsynchronous: function(url, data, callback)  {
      console.log("request url:", url)
      $.ajax({
        url: url,
        data: data,
        type: 'get',
        dataType: 'json',
        success: function(data) {
          results = data;
        },
        error: function(e) {
          console.log(e);
        }
      }).done(function(data) {
        callback(data);
      });
    },

    // Make post call
    postData: function(url, data) {
      console.log("request url:", url)
      return $.ajax({
        url: url,
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json'
        },
        type: 'post',
        dataType: 'json',
        data: data
      });
    },
    fetchMetricSummary: function(dashboard, timeRange, callback) {
      var url = constants.METRIC_SUMMARY;
      var data = {
          dashboard : dashboard,
          timeRange : timeRange
      };
      this.getDataAsynchronous(url, data, callback);
    },
    fetchAnomalySummary: function(dashboard, timeRanges, callback) {
      var url = constants.ANOMALY_SUMMARY;
      var data = {
          dashboard : dashboard,
          timeRanges : timeRanges.join()
      };
      this.getDataAsynchronous(url, data, callback);
    },
    // Fetch anomalies for metric ids in array in time range
    fetchAnomaliesForMetricIds : function(startTime, endTime, metricIds, functionName, callback) {
      var url = constants.SEARCH_ANOMALIES_METRICIDS + startTime + this.URL_SEPARATOR + endTime;
      var data = {
          metricIds : metricIds.join(),
          functionName : functionName
      };
      this.getDataAsynchronous(url, data, callback);
    },
    // Fetch anomalies for dashboard id in time range
    fetchAnomaliesForDashboardId : function(startTime, endTime, dashboardId, functionName, callback) {
      var url = constants.SEARCH_ANOMALIES_DASHBOARDID + startTime + this.URL_SEPARATOR + endTime;
      var data = {
          dashboardId : dashboardId,
          functionName : functionName
      };
      this.getDataAsynchronous(url, data, callback);
    },
    // Fetch anomalies for anomaly ids in array in time range
    fetchAnomaliesForAnomalyIds : function(startTime, endTime, anomalyIds, functionName, callback) {
      var url = constants.SEARCH_ANOMALIES_ANOMALYIDS + startTime + this.URL_SEPARATOR + endTime;
      var data = {
          anomalyIds : anomalyIds.join(),
          functionName : functionName
      };
      this.getDataAsynchronous(url, data, callback);
    },
    // Update anomaly feedback for anomaly id
    updateFeedback : function(anomalyId, feedbackType) {
      var url = constants.UPDATE_ANOMALY_FEEDBACK + anomalyId;
      var data = '{ "feedbackType": "' + feedbackType + '","comment": ""}';
      var response = this.postData(url, data);
      console.log("Updated backend feedback " + feedbackType);
    },

    fetchGranularityForMetric: function (metricId) {
      var url = "/data/agg/granularity/metric/"+metricId;
      return this.getDataSynchronous(url);
    },

    fetchDimensionsForMetric : function(metricId) {
      var url = "/data/autocomplete/dimensions/metric/"+metricId;
      return this.getDataSynchronous(url);
    },

    fetchFiltersForMetric : function(metricId) {
      var url = "/data/autocomplete/filters/metric/" + metricId;
      return this.getDataSynchronous(url);
    },

    fetchTimeseriesCompare: function (metricId, currentStart, currentEnd, baselineStart, baselineEnd,
        dimension, filters, granularity) {
      var url = "/timeseries/compare/" + metricId + "/" + currentStart + "/" + currentEnd + "/"
          + baselineStart + "/" + baselineEnd + "?dimension=" + dimension + "&filters="
          + JSON.stringify(filters) + "&granularity=" + granularity;
      console.log("timeseries data fetch URL ----> ");
      console.log(url);
      return this.getDataSynchronous(url);
    }

};
