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

  this.viewParams = {};
  this.chart;
}

TimeSeriesCompareView.prototype = {
  render() {
    if (this.timeSeriesCompareModel.subDimensionContributionDetails) {
      const heatmapCurrentStart = this.timeSeriesCompareModel.heatMapCurrentStart;
      const heatmapCurrentEnd = this.timeSeriesCompareModel.heatMapCurrentEnd;

      this.renderChartSection();
      if (heatmapCurrentStart && heatmapCurrentEnd) {
        this.loadAnomalyRegion(heatmapCurrentStart, heatmapCurrentEnd);
      }
      this.renderPercentageChangeSection();
      this.setupListenersForDetailsAndCumulativeCheckBoxes();

      // set initial view params for rendering heatmap
      this.viewParams = {
        metricId: this.timeSeriesCompareModel.metricId,
        heatmapCurrentStart: this.timeSeriesCompareModel.heatmapCurrentStart || this.timeSeriesCompareModel.currentStart,
        heatmapCurrentEnd: this.timeSeriesCompareModel.heatMapCurrentEnd || this.timeSeriesCompareModel.currentEnd,
        heatmapBaselineStart: this.timeSeriesCompareModel.baselineStart,
        heatmapBaselineEnd: this.timeSeriesCompareModel.baselineEnd,
        heatMapFilters: Object.assign({}, this.timeSeriesCompareModel.filters)
      };
    }
  },

  destroy() {
    $('#timeseries-contributor-placeholder, #contributor-table-placeholder').children().remove();
  },

  renderChartSection: function () {
    // render chart
    var timeseriesContributorViewResult = this.timeseries_contributor_template_compiled(
        this.timeSeriesCompareModel);
    $(this.timeseries_contributor_placeHolderId).html(timeseriesContributorViewResult);

    // render chart legend
    var timeseriesSubDimensionsHtml = this.timeseries_subdimension_legend_template_compiled(
        this.timeSeriesCompareModel);
    $(this.timeseries_subdimension_legend_placeHolderId).html(timeseriesSubDimensionsHtml);

    // this needs to be called last so that the chart is rendered after the legend
    // otherwise, we run into overflow issues
    this.loadChart(
        this.timeSeriesCompareModel.subDimensionContributionDetails.contributionMap[constants.DEFAULT_ANALYSIS_DIMENSION]);
    this.loadAnomalyRegion();
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

  loadChart: function (timeSeriesObject, regions = []) {
    // CHART GENERATION
    this.timeSeriesObject = timeSeriesObject;
    this.chart = c3.generate({
      bindto: '#analysis-chart',
      data: {
        x: 'date',
        xFormat : '%Y-%m-%d %H:%M',
        columns: timeSeriesObject.columns,
        type: 'line'
      },
      legend : {
        position : 'inset',
        inset: {
          anchor: 'top-right',
        }
      },
      axis: {
        y: {
          show: true,
          tick: {
            format: d3.format('.2s')
          }
        },
        x: {
          type: 'timeseries', show: true, tick: { // "rotate":30,   // this will rotate the x axis display values
            fit: false
          }
        }
      },
      zoom: {
        enabled: true
      },
      regions
    });
  },

  loadAnomalyRegion(heatMapCurrentStart, heatMapCurrentEnd) {
    if (!(heatMapCurrentStart && heatMapCurrentEnd)) return;
    const regions = {
        axis: 'x',
        start: heatMapCurrentStart.format(constants.TIMESERIES_DATE_FORMAT),
        end: heatMapCurrentEnd.format(constants.TIMESERIES_DATE_FORMAT),
        class: 'anomaly-region',
        tick: {
          format: '%m %d %Y'
        }
      };

    this.chart.regions([regions]);
    this.chart.load({});
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

  setupListenersForPercentageChangeCells() {
    // setting up listeners for percentage change cells
    for (var i in this.timeSeriesCompareModel.subDimensions) {
      for (var j in
          this.timeSeriesCompareModel.subDimensionContributionDetails.timeBucketsCurrent) {
        var tableCellId = i + "-" + j;
        $("#" + tableCellId).click((e) => {
          var subDimensionBucketIndexArr = e.target.attributes[0].value.split("-");
          var subDimension = this.timeSeriesCompareModel.subDimensions[subDimensionBucketIndexArr[0]];
          var bucketIndex = subDimensionBucketIndexArr[1];
          this.collectAndUpdateViewParamsForHeatMapRendering(subDimension, bucketIndex);
          const { heatMapCurrentStart, heatMapCurrentEnd } = this.viewParams;
          this.loadAnomalyRegion(heatMapCurrentStart, heatMapCurrentEnd);
          this.heatmapRenderEvent.notify();
        });
      }
    }
  },

  setupListenerForChartSubDimension: function () {
    $('#chart-dimensions :checkbox').change((event) => {
      const subDimensionIndex =  event.currentTarget.getAttribute('id');
      const subDimension = this.timeSeriesCompareModel.subDimensions[subDimensionIndex];
      const checked = event.currentTarget.checked;
      this.updateSubDimension(subDimension, checked);
    });
  },

  updateSubDimension(subDimension, checked) {
    const { columns } = this.timeSeriesCompareModel.subDimensionContributionDetails.contributionMap[subDimension];
    const [ _, current, baseline ] = columns;
    if (checked) {
      this.chart.load({
        columns: [current, baseline]
      });
    } else {
      this.chart.load({
        unload: [current[0], baseline[0]]
      });
    }
  },

  calculateDelta() {
    let delta = 5 * 60 * 60 * 1000; // 1 hour
    if (this.timeSeriesCompareModel.granularity === 'DAYS') {
      delta = 86400 * 1000;
    }
    if (this.timeSeriesCompareModel.subDimensionContributionDetails.timeBucketsCurrent.length > 1) {
      delta = this.timeSeriesCompareModel.subDimensionContributionDetails.timeBucketsCurrent[1]
          - this.timeSeriesCompareModel.subDimensionContributionDetails.timeBucketsCurrent[0];
    }
    return delta;
  },

  collectAndUpdateViewParamsForHeatMapRendering(subDimension, bucketIndex) {
    const currentStart = this.timeSeriesCompareModel.subDimensionContributionDetails.timeBucketsCurrent[bucketIndex];
    const baselineStart = this.timeSeriesCompareModel.subDimensionContributionDetails.timeBucketsBaseline[bucketIndex];
    const delta = this.calculateDelta();
    const currentEnd = currentStart + delta;
    const baselineEnd = baselineStart + delta;
    const heatMapFilters = Object.assign({}, this.timeSeriesCompareModel.filters || {});
    const {
      dimension : metricDimension,
      metricId
    } = this.timeSeriesCompareModel;

    if (subDimension !== constants.DEFAULT_ANALYSIS_DIMENSION) {
      heatMapFilters[metricDimension] = [subDimension];
    } else if (metricDimension !== constants.DEFAULT_ANALYSIS_DIMENSION) {
      const allSubdimensions = this.timeSeriesCompareModel.subDimensions
        .filter(subDimension => subDimension !== constants.DEFAULT_ANALYSIS_DIMENSION);
      heatMapFilters[metricDimension] = allSubdimensions;
    }

    this.viewParams = {
      metricId,
      heatMapCurrentStart: moment(currentStart),
      heatMapCurrentEnd: moment(currentEnd),
      heatMapBaselineStart: moment(baselineStart),
      heatMapBaselineEnd: moment(baselineEnd),
      heatMapFilters,
    };
  },

  clearHeatMapViewParams() {
    this.viewParams = {
      heatMapCurrentStart: null,
      heatMapCurrentEnd: null,
      heatMapBaselineStart: null,
      heatMapBaselineEnd: null,
      heatMapFilters: null
    };
  }
};

