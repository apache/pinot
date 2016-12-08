function PercentageChangeTableModel() {
  this.showDetailsChecked = false;
  this.showCumulativeChecked = false;
  this.params;

  this.startTime = moment().subtract(7, "days");
  this.endTime = moment();
  this.timestamps = [];
  this.metricTable = [];
  this.metricDimansionTable = [];
  this.dimensionName;
}

PercentageChangeTableModel.prototype = {

  init : function(params) {
    this.params = params;
    this.buildSampleData();
  },

  rebuild : function() {
  // TODO: fetch relevant data from backend
  },

  buildSampleData: function() {
    var row1 = new PercentageChangeTableRow();
    row1.metricName = "Overall";
    row1.data = ["+8%","+21%","-50%","Over","All","1%","2%","3%","4%","5%","6%","7%","8%","9%","10%","11%","12%","13%","14%","15%","1%","2%","3%","4%"];

    var row2 = new PercentageChangeTableRow();
    row2.metricName = "USA";
    row2.data = ["+8%","+20%","-50%","11%","12%","1%","2%","3%","4%","USA","6%","7%","8%","9%","10%","11%","12%","13%","14%","15%","1%","2%","3%","4%"];

    var row3 = new PercentageChangeTableRow();
    row3.metricName = "TW";
    row3.data = ["+8%","+20%","-50%","11%","12%","1%","2%","3%","4%","TW","6%","7%","8%","9%","10%","11%","12%","13%","14%","15%","1%","2%","3%","4%"];

    var row4 = new PercentageChangeTableRow();
    row4.metricName = "IN";
    row4.data = ["+8%","+20%","-50%","11%","12%","1%","2%","3%","4%","IN","6%","7%","8%","9%","10%","11%","12%","13%","14%","15%","1%","2%","3%","4%"];

    this.metricTable = [];
    this.metricTable.push(row1);
    this.metricDimansionTable = [];
    this.metricDimansionTable.push(row2);
    this.metricDimansionTable.push(row3);
    this.metricDimansionTable.push(row4);

    this.timestamps = [];
    var time = moment().subtract(24, 'hours');
    for (var i = 0; i < 24; ++i) {
      var date = new Date(time);
      this.timestamps.push(date);
      time.add(1, 'hours');
    }

    this.dimensionName = "Country";
  }


};

function PercentageChangeTableRow() {
  this.metricName = "N/A";
  this.timeGranularity = "HOURS";
  this.timestamps = [];
  this.data = [];
}
