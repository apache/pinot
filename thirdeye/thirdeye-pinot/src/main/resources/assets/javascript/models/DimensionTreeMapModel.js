function DimensionTreeMapModel() {
  this.metricId;
  this.metricName;
  this.heatmapFilters;
  this.currentStart;
  this.currentEnd;
  this.baselineStart;
  this.baselineEnd;

  this.heatmapData;

  this.heatmapMode = "percentChange";

  this.currentTotal = 0;
  this.baselineTotal = 0;
  this.absoluteChange = 0;
  this.percentChange = 0;
  this.dimensions = [];
  this.treeMapData = [];
}

DimensionTreeMapModel.prototype = {
  init: function (params) {
    if (params) {
      this.metricId = params.metricId;
      this.metricName = params.metricName;

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

      if (params.heatmapMode) {
        this.heatmapMode = params.heatmapMode;
      }
    }
  },

  update: function () {
    if (this.metricId) {
      dataService.fetchHeatmapData(
        this.metricId,
        this.currentStart,
        this.currentEnd,
        this.baselineStart,
        this.baselineEnd,
        this.heatmapFilters
      ).then((heatmapData) => {
        this.heatmapData = heatMapData;
        this.transformResponseData(heatMapData);
      });
    }
  },

  transformResponseData: function (heatMapData) {
    if (heatMapData) {
      if (heatMapData.dimensions) {
        this.dimensions = heatMapData.dimensions;
        var treeMapData = [];
        for (var i in heatMapData.dimensions) {
          var dimension = heatMapData.dimensions[i];
          var dataKey = this.metricName + "." + dimension;
          var row = {"t": "0", "children": []};
          if (heatMapData.data && heatMapData.data[dataKey]) {
            var dimensionValueIndex = heatMapData.data[dataKey].schema.columnsToIndexMapping['dimensionValue'];
            var percentageChangeIndex = heatMapData.data[dataKey].schema.columnsToIndexMapping['percentageChange'];
            var currentValueIndex = heatMapData.data[dataKey].schema.columnsToIndexMapping['currentValue'];
            var contributionToOverallChangeIndex = heatMapData.data[dataKey].schema.columnsToIndexMapping['contributionToOverallChange'];
            var contributionChangeIndex = heatMapData.data[dataKey].schema.columnsToIndexMapping['contributionDifference'];
            for (var j in heatMapData.data[dataKey].responseData) {
              var record = heatMapData.data[dataKey].responseData[j];
              var item = {
                "t": record[dimensionValueIndex],
                "value": record[currentValueIndex],
                "percentageChange": record[percentageChangeIndex],
                "contributionChange": record[contributionChangeIndex],
                "contributionToOverallChange": record[contributionToOverallChangeIndex]
              };
              row.children.push(item);
            }
          }
          treeMapData.push(row);
        }
        this.treeMapData = treeMapData;
      }
      this.currentTotal = heatMapData.summary.simpleFields.currentTotal;
      this.baselineTotal = heatMapData.summary.simpleFields.baselineTotal;
      this.percentChange = heatMapData.summary.simpleFields.deltaPercentage;
      this.absoluteChange = heatMapData.summary.simpleFields.deltaChange;
    }
  }
}
