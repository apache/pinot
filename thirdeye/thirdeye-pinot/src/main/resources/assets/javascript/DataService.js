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
    getDataAsynchronous: function(url)  {
      console.log("request url:", url)

      return $.ajax({
        url: url,
        type: 'get',
        dataType: 'json',

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
    // Fetch anomalies for metric ids in array in time range
    fetchAnomaliesForMetricIds : function(startTime, endTime, metricIds, functionName) {
      var url = constants.SEARCH_ANOMALIES_METRICIDS + startTime + this.URL_SEPARATOR + endTime;
      var data = {
          metricIds : metricIds.join(),
          functionName : functionName
      };
      var anomalies = this.getDataSynchronous(url, data);
      return anomalies;
    },
    // Fetch anomalies for dashboard id in time range
    fetchAnomaliesForDashboardId : function(startTime, endTime, dashboardId, functionName) {
      var url = constants.SEARCH_ANOMALIES_DASHBOARDID + startTime + this.URL_SEPARATOR + endTime;
      var data = {
          dashboardId : dashboardId,
          functionName : functionName
      };
      var anomalies = this.getDataSynchronous(url, data);
      return anomalies;
    },
    // Fetch anomalies for anomaly ids in array in time range
    fetchAnomaliesForAnomalyIds : function(startTime, endTime, anomalyIds, functionName) {
      var url = constants.SEARCH_ANOMALIES_ANOMALYIDS + startTime + this.URL_SEPARATOR + endTime;
      var data = {
          anomalyIds : anomalyIds.join(),
          functionName : functionName
      };
      var anomalies = this.getDataSynchronous(url, data);
      return anomalies;
    },
    // Update anomaly feedback for anomaly id
    updateFeedback : function(anomalyId, feedbackType) {
      var url = constants.UPDATE_ANOMALY_FEEDBACK + anomalyId;
      var data = '{ "feedbackType": "' + feedbackType + '","comment": ""}';
      var response = this.postData(url, data);
      console.log("Updated backend feedback " + feedbackType);
    },

    fetchTimeseriesCompare: function (metricId, currentStart, currentEnd, baselineStart, baselineEnd,
        dimensions, filters) {
      // TODO : set dimensions and filters
      var url = "/timeseries/compare/" + metricId + "/" + currentStart + "/" + currentEnd + "/"
          + baselineStart + "/" + baselineEnd;
      return this.getDataSynchronous(url);
    }

};
