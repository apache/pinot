function AnomalyResultModel(){

  this.startDate = moment().subtract(6, 'days');
  this.endDate = moment().subtract(0, 'days');

  // list of anomalies
  // get these from backend call
  this.anomaly1 = new AnomalyResult(101);
  this.anomaly2 = new AnomalyResult(102);
  this.anomalies = [];
  this.anomalies.push(this.anomaly1);
  this.anomalies.push(this.anomaly2);

  // list of metrics
  this.metrics = [ "m1", "m2" ];

}

AnomalyResultModel.prototype = {
  init: function (params) {
  },

  update : function() {

  }
};

function AnomalyResult(anomalyId) {
  this.anomalyId = anomalyId;
  this.metric = "feed_session_count";

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
