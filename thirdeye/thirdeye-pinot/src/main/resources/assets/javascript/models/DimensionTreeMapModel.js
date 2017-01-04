function DimensionTreeMapModel() {
  this.metricId;
  this.metricName;
  this.heatmapFilters;
  this.currentStart;
  this.currentEnd;
  this.baselineStart;
  this.baselineEnd;

  this.heatmapData;

  this.currentTotal = 50000;
  this.baselineTotal = 100000;
  this.absoluteChange = this.currentTotal - this.baselineTotal;
  this.percentChange = (this.currentTotal - this.baselineTotal) * 100 / this.baselineTotal;
  this.dimensions = ["browser", "country", "device"];

  this.treeMapData = [{
    "t": "0", "children": [{
      "t": "Chrome (10 %)", "value": 100
    }, {
      "t": "011", "value": 50
    }, {
      "t": "012", "value": 55
    }, {
      "t": "013", "value": 25
    }]
  }, {
    "t": "0", "children": [{
      "t": "010", "value": 10
    }, {
      "t": "011", "value": 25
    }, {
      "t": "012", "value": 50
    }, {
      "t": "013", "value": 25
    }]
  }, {
    "t": "0", "children": [{
      "t": "010", "value": 100
    }, {
      "t": "011", "value": 50
    }, {
      "t": "012", "value": 5
    }, {
      "t": "013", "value": 55
    }]
  }];

}

DimensionTreeMapModel.prototype = {
  init: function (params) {
    if (params) {
      if (params.metric) {
        this.metricId = params.metric.id;
        this.metricName = params.metric.name;
      }
      if (params.currentStart) {
        this.currentStart = params.currentStart;
      }
      if (params.currentEnd) {
        this.currentEnd = params.currentEnd;
      }
      if (params.baselineStart) {
        this.baselineStart = params.baselineStart;
      }
      if (params.baselineEnd) {
        this.baselineEnd = params.baselineEnd;
      }
      if (params.granularity) {
        this.granularity = params.granularity;
      }
      if (params.heatmapFilters) {
        this.heatmapFilters = params.heatmapFilters;
      } else if (params.filters) {
        this.heatmapFilters = params.filters;
      }
    }
  },

  update: function () {
    if (this.metricId) {
      var heatMapData = dataService.fetchHeatmapData(this.metricId, this.currentStart, this.currentEnd, this.baselineStart, this.baselineEnd, this.heatmapFilters);
      this.heatmapData = heatMapData;
    }
  },

  transformResponseData : function(heatMapData) {
    if (heatMapData) {
      // TODO: transform
    }
  }
}
