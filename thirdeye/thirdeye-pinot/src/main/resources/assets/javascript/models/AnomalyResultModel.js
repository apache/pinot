function AnomalyResultModel(){

  this.startDate = moment().subtract(6, 'days');
  this.endDate = moment().subtract(0, 'days');

  // list of anomalies
  this.anomaly1 = new AnomalyResult(101);
  this.anomaly2 = new AnomalyResult(102);
  this.anomalies = [];
  this.anomalies.push(this.anomaly1);
  this.anomalies.push(this.anomaly2);
  /*this.anomalies = [
                      [
                        [ 'date',  ],
                        [ 'current', 30, 200, 100, 400, 150, 250, 60 ],
                        [ 'baseline', 35, 225, 200, 600, 170, 220, 70 ],
                        // other things like anomaly function details
                        // anomaly date range
                      ],
                      [
                        [ 'date', '2016-01-01', '2016-01-2', '2016-01-3', '2016-01-4', '2016-01-05', '2016-01-06', '2016-01-07' ],
                        [ 'current', 30, 200, 100, 400, 150, 250, 60 ],
                        [ 'baseline', 35, 225, 200, 600, 170, 220, 70 ],
                        // other things like anomaly function details
                        // anomaly date range
                      ]

                   ];*/
  // list of metrics
  this.metrics = [ "m1", "m2" ];
}

AnomalyResultModel.prototype = {
  init: function (ctx) {

  },

  update : function() {

  }
};

function AnomalyResult(anomalyId) {
  this.anomalyId = anomalyId;
  this.dates = ['2016-01-01', '2016-01-02', '2016-01-03', '2016-01-04', '2016-01-05', '2016-01-06', '2016-01-07'];
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
}
