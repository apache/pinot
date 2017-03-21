function TimeSeriesCompareView(timeSeriesCompareModel) {
  this.timeSeriesCompareModel = timeSeriesCompareModel;

  var timeseries_contributor_template = $("#timeseries-contributor-template").html();
  this.timeseries_contributor_template_compiled = Handlebars.compile(
      timeseries_contributor_template);
  this.timeseries_contributor_placeHolderId = "#timeseries-contributor-placeholder";

  var timeseries_subdimension_legend_template = $(
      "#timeseries-subdimension-legend-template").html();
  this.timeseries_subdimension_legend_template_compiled = Handlebars.compile(
      timeseries_subdimension_legend_template);
  this.timeseries_subdimension_legend_placeHolderId = "#analysis-chart-legend";

  var contributor_table_template = $("#contributor-table-details-template").html();
  this.contributor_table_template_compiled = Handlebars.compile(contributor_table_template);
  this.contributor_table_placeHolderId = "#contributor-table-placeholder";
  this.heatmapRenderEvent = new Event(this);

  this.viewParams;
}

TimeSeriesCompareView.prototype = {
  render: function () {
    var self = this;
    if (this.timeSeriesCompareModel.subDimensionContributionDetails) {
      this.renderChartSection();
      this.renderPercentageChangeSection();
      this.setupListenersForDetailsAndCumulativeCheckBoxes();

      // set initial view params for rendering heatmap
      this.viewParams = {
        metricId: self.timeSeriesCompareModel.metricId,
        currentStart: self.timeSeriesCompareModel.currentStart,
        currentEnd: self.timeSeriesCompareModel.currentEnd,
        baselineStart: self.timeSeriesCompareModel.baselineStart,
        baselineEnd: self.timeSeriesCompareModel.baselineEnd,
        heatmapFilters: self.timeSeriesCompareModel.filters
      };
    }
  },

  renderChartSection: function () {
    // render chart
    var timeseriesContributorViewResult = this.timeseries_contributor_template_compiled(
        this.timeSeriesCompareModel);
    $(this.timeseries_contributor_placeHolderId).html(timeseriesContributorViewResult);
    this.loadChart(
        this.timeSeriesCompareModel.subDimensionContributionDetails.contributionMap['All']);

    // render chart legend
    var timeseriesSubDimensionsHtml = this.timeseries_subdimension_legend_template_compiled(
        this.timeSeriesCompareModel);
    $(this.timeseries_subdimension_legend_placeHolderId).html(timeseriesSubDimensionsHtml);
    this.setupListenerForChartSubDimension();
  },

  renderPercentageChangeSection: function () {
    var contributorTableResult = this.contributor_table_template_compiled(
        this.timeSeriesCompareModel);
    $(this.contributor_table_placeHolderId).html(contributorTableResult);
    $('#show-details').checked = this.timeSeriesCompareModel.showDetailsChecked;
    $('#show-cumulative').checked = this.timeSeriesCompareModel.showCumulativeChecked;
    this.setupListenersForPercentageChangeCells();
  },

  loadChart: function (timeSeriesObject) {
    // CHART GENERATION
    var chart = c3.generate({
      bindto: '#analysis-chart',
      data: {
        x: 'date', columns: timeSeriesObject.columns, type: 'line'
      },
      legend : {
        position : 'inset',
        inset: {
          anchor: 'top-right',
        }
      },
      axis: {
        y: {
          show: true
        },
        x: {
          type: 'timeseries', show: true, tick: {
            "culling": {"max": 100},
            "count": 10, // "rotate":30,   // this will rotate the x axis display values
            "fit": true,
            "format": "%m-%d %H:%M"
          }
        }
      }
    });
  },

  setupListenersForDetailsAndCumulativeCheckBoxes: function () {
    var self = this;
    $('#show-details').change(function () {
      self.timeSeriesCompareModel.showDetailsChecked = !self.timeSeriesCompareModel.showDetailsChecked;
      self.renderPercentageChangeSection();
    });
    $('#show-cumulative').change(function () {
      self.timeSeriesCompareModel.showCumulativeChecked = !self.timeSeriesCompareModel.showCumulativeChecked;
      self.renderPercentageChangeSection();
    });
  },

  setupListenersForPercentageChangeCells: function () {
    var self = this;
    // setting up listeners for percentage change cells
    for (var i in self.timeSeriesCompareModel.subDimensions) {
      for (var j in
          self.timeSeriesCompareModel.subDimensionContributionDetails.timeBucketsCurrent) {
        var tableCellId = i + "-" + j;
        $("#" + tableCellId).click(function (e) {
          var subDimensionBucketIndexArr = e.target.attributes[0].value.split("-");
          var subDimension = self.timeSeriesCompareModel.subDimensions[subDimensionBucketIndexArr[0]];
          var bucketIndex = subDimensionBucketIndexArr[1];
          self.collectAndUpdateViewParamsForHeatMapRendering(subDimension, bucketIndex);
          self.heatmapRenderEvent.notify();
        });
      }
    }
  },

  setupListenerForChartSubDimension: function () {
    var self = this;
    for (var i in self.timeSeriesCompareModel.subDimensions) {
      $('#a-sub-dimension-' + i + ' a').click(self, function (e) {
        var index = e.currentTarget.getAttribute('id');
        var subDimension = self.timeSeriesCompareModel.subDimensions[index];
        self.loadChart(
            self.timeSeriesCompareModel.subDimensionContributionDetails.contributionMap[subDimension]);
      });
    }
  },

  collectAndUpdateViewParamsForHeatMapRendering: function (subDimension, bucketIndex) {
    var self = this;

    var currentStart = self.timeSeriesCompareModel.subDimensionContributionDetails.timeBucketsCurrent[bucketIndex];
    var baselineStart = self.timeSeriesCompareModel.subDimensionContributionDetails.timeBucketsBaseline[bucketIndex];

    var delta = 5 * 60 * 60 * 1000; // 1 hour
    if (this.timeSeriesCompareModel.granularity == 'DAYS') {
      delta = 86400 * 1000;
    }
    if (self.timeSeriesCompareModel.subDimensionContributionDetails.timeBucketsCurrent.length > 1) {
      delta = self.timeSeriesCompareModel.subDimensionContributionDetails.timeBucketsCurrent[1]
          - self.timeSeriesCompareModel.subDimensionContributionDetails.timeBucketsCurrent[0];
    }
    var currentEnd = currentStart + delta;
    var baselineEnd = baselineStart + delta;

    var heatmapFilters = self.timeSeriesCompareModel.filters;
    if (!(subDimension.toUpperCase() === 'ALL')) {
      heatmapFilters[self.timeSeriesCompareModel.dimension] = [subDimension];
    }

    this.viewParams = {
      metricId: self.timeSeriesCompareModel.metricId,
      currentStart: moment(currentStart),
      currentEnd: moment(currentEnd),
      baselineStart: moment(baselineStart),
      baselineEnd: moment(baselineEnd),
      heatmapFilters: heatmapFilters
    };
  }
}

