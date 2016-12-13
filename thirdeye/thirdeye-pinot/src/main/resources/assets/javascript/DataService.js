function DataService() {

}

DataService.prototype = {

    getSynchronousData: function(url, params)  {
      console.log("request url:", url)

      var results = undefined;
       $.ajax({
        url: url,
        data: params,
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
    getAsynchronousData: function(url)  {
      console.log("request url:", url)

      return $.ajax({
        url: url,
        type: 'get',
        dataType: 'json',

      });
    },
    fetchAnomaliesForMetricIds(startTime, endTime, metricIds, functionName) {
      var url = constants.SEARCH_ANOMALIES_METRICIDS + startTime + '/' + endTime;
      var params = {
          metricIds : metricIds.join(),
          functionName : functionName
      };
      var anomalies = this.getSynchronousData(url, params);
      return anomalies;
    },
    fetchAnomaliesForDashboardId(startTime, endTime, dashboardId, functionName) {
      var url = constants.SEARCH_ANOMALIES_DASHBOARDID + startTime + '/' + endTime;
      var params = {
          dashboardId : dashboardId,
          functionName : functionName
      };
      var anomalies = this.getSynchronousData(url, params);
      return anomalies;
    },
    fetchAnomaliesForAnomalyIds(startTime, endTime, anomalyIds, functionName) {
      var url = constants.SEARCH_ANOMALIES_ANOMALYIDS + startTime + '/' + endTime;
      var params = {
          anomalyIds : anomalyIds.join(),
          functionName : functionName
      };
      var anomalies = this.getSynchronousData(url, params);
      return anomalies;
    }
};
