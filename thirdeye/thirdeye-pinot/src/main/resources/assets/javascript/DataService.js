function DataService() {

}

DataService.prototype = {

    getSynchronousData: function(url)  {
      console.log("request url:", url)

      var results = undefined;
       $.ajax({
        url: url,
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
    fetchAnomalyWrappers: function(dataset, metric, startTime, endTime) {
      var url = '/anomalies/wrapper/' + dataset + '/' + metric + '/' + startTime + '/' + endTime;
      return this.getSynchronousData(url);
    },
    fetchAnomalies: function(metricAliases, startTime, endTime) {
      var anomalies = []; // array of AnomalyObject

      // for each selected metric alias
      for (var i = 0; i < metricAliases.length; i++) {

        var tokens = metricAliases[i].split("::");
        var dataset = tokens[0];
        var metric = tokens[1];

        // fetch anomaly wrappers
        var anomalyWrappers = this.fetchAnomalyWrappers(dataset, metric, startTime, endTime);
        console.log("Anomaly Wrappers");
        console.log(anomalyWrappers);
        anomalyWrappers.forEach( function (anomalyWrapper) {
            anomalies.push(anomalyWrapper);
        });

      }
      console.log("returning anomalies");
      console.log(anomalies);
      return anomalies;
    }

};
