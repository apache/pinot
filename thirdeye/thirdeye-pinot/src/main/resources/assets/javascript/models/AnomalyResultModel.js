function AnomalyResultModel(){

  this.metricAliases = ["feed_sessions_additive::engaged_feed_session_count"];
  this.startDate = moment().subtract(6, 'days').startOf('day');
  this.endDate = moment().subtract(0, 'days').startOf('day');

  this.anomalies = [];

}

AnomalyResultModel.prototype = {
  init: function (params) {
  },

  update : function() {

  },
  getAnomaliesList : function() {

    var anomaliesList = dataService.fetchAnomalies(this.metricAliases, this.startDate, this.endDate);
    console.log(anomaliesList);
    return anomaliesList;
  }

}

function AnomalyWrapper() {
  this.anomalyId = "101";
  this.metric = "feed_sessions_additive";
  this.dataset = "engaged_feed_session_count"

  this.dates = ['2016-01-01', '2016-01-02', '2016-01-03', '2016-01-04', '2016-01-05', '2016-01-06', '2016-01-07'];
  this.currentEnd ='Jan 7 2016';
  this.currentStart = 'Jan 1 2016';
  this.baselineEnd = 'Dec 31 2015';
  this.baselineStart = 'Dec 25 2015';
  this.baselineValues = [35, 225, 200, 600, 170, 220, 70];
  this.currentValues = [30, 200, 100, 400, 150, 250, 60];
  this.current = '1000';
  this.baseline = '2000';

  this.anomalyRegionStart = '2016-01-03';
  this.anomalyRegionEnd = '2016-01-05';
  this.anomalyFunctionId = 5;
  this.anomalyFunctionName = 'efs_wow_country';
  this.anomalyFunctionType = 'wow_rule';
  this.anomalyFunctionProps = 'props,props,props';
  this.anomalyFunctionDimension = 'country:US';
  this.anomalyFeedback = "Confirmed Anomaly";
}

