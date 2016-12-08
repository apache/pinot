function DimensionTreeMapModel() {
  this.currentStartTime = moment();
  this.currentEndTime = moment().subtract("1", "days");
  this.baselineStartTime = moment().subtract("7", "days");
  this.baselineEndTime = moment().subtract("6", "days");
  this.currentTotal = 50000;
  this.baselineTotal = 100000;
  this.absoluteChange = this.currentTotal - this.baselineTotal;
  this.percentChange = (this.currentTotal - this.baselineTotal) *100/this.baselineTotal;
  this.dimensions = [ "browser", "country", "device" ];
  this.treeMapData = [ {
    "t" : "0",
    "children" : [ {
      "t" : "010",
      "value" : 100
    }, {
      "t" : "011",
      "value" : 50
    }, {
      "t" : "012",
      "value" : 5
    }, {
      "t" : "013",
      "value" : 25
    } ]
  }, {
    "t" : "0",
    "children" : [ {
      "t" : "010",
      "value" : 100
    }, {
      "t" : "011",
      "value" : 50
    }, {
      "t" : "012",
      "value" : 5
    }, {
      "t" : "013",
      "value" : 25
    } ]
  }, {
    "t" : "0",
    "children" : [ {
      "t" : "010",
      "value" : 100
    }, {
      "t" : "011",
      "value" : 50
    }, {
      "t" : "012",
      "value" : 5
    }, {
      "t" : "013",
      "value" : 25
    } ]
  } ];

}

DimensionTreeMapModel.prototype = {

  update : function() {

  }
}