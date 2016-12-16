function TimeSeriesCompareView(timeSeriesCompareModel) {
  this.timeSeriesCompareModel = timeSeriesCompareModel;

  var timeseries_contributor_template = $("#timeseries-contributor-template").html();
  this.timeseries_contributor_template_compiled = Handlebars.compile(timeseries_contributor_template);
  this.timeseries_contributor_placeHolderId = "#timeseries-contributor-placeholder";

  var timeseries_subdimension_legend_template = $("#timeseries-subdimension-legend-template").html();
  this.timeseries_subdimension_legend_template_compiled = Handlebars.compile(timeseries_subdimension_legend_template);
  this.timeseries_subdimension_legend_placeHolderId = "#analysis-chart-legend";

  var wow_metric_table_template = $("#wow-metric-table-template").html();
  this.wow_metric_table_template_compiled = Handlebars.compile(wow_metric_table_template);
  this.wow_metric_table_placeHolderId = "#wow-metric-table-placeholder";

  var wow_metric_dimension_table_template = $("#wow-metric-dimension-table-template").html();
  this.wow_metric_dimension_table_template_compiled = Handlebars.compile(wow_metric_dimension_table_template);
  this.wow_metric_dimension_table_placeHolderId = "#wow-metric-dimension-table-placeholder";

  this.checkboxClickEvent = new Event();
}

TimeSeriesCompareView.prototype = {
  init: function () {
  },

  render: function () {
    var timeseriesContributorViewResult = this.timeseries_contributor_template_compiled(this.model);
    $(this.timeseries_contributor_placeHolderId).html(timeseriesContributorViewResult);

    var wowMetricTableResult = this.wow_metric_table_template_compiled(
        this.model);
    $(this.wow_metric_table_placeHolderId).html(wowMetricTableResult);

    var wowMetricDimensionTableResult = this.wow_metric_dimension_table_template_compiled(
        this.model);
    $(this.wow_metric_dimension_table_placeHolderId).html(wowMetricDimensionTableResult);

    if (this.timeSeriesCompareModel.subDimensionContributionMap) {
      this.loadChart(this.timeSeriesCompareModel.subDimensionContributionMap['All']);
    }
    // TODO: remove this
    else {
      this.loadChart({
            'start': '2016-01-3',
            'end': '2016-01-5',
            'columns': [['date', '2016-01-01', '2016-01-2', '2016-01-3', '2016-01-4', '2016-01-05',
             '2016-01-06', '2016-01-07'], ['current', 30, 200, 100, 400, 150, 250, 60],
             ['baseline', 35, 225, 200, 600, 170, 220, 70]]
         })
    }

    this.loadSubDimensions(this.timeSeriesCompareModel.subDimensions);

    this.setupListeners();
  },

  loadChart : function (timeSeriesObject) {
    // CHART GENERATION
    var chart = c3.generate({
      bindto : '#analysis-chart',
      data : {
        x : 'date',
        columns : timeSeriesObject.columns,
        type : 'spline'
      },
      legend : {
        show : false,
        position : 'top'
      },
      axis : {
        y : {
          show : true
        },
        x : {
          type : 'timeseries',
          show : true
        }
      },
      regions : [ {
        start : timeSeriesObject.start,
        end : timeSeriesObject.end
      } ]
    });

  },

  loadSubDimensions : function() {
    var timeseriesSubDimensionsHtml = this.timeseries_subdimension_legend_template_compiled(this.timeSeriesCompareModel);
    $(this.timeseries_subdimension_legend_placeHolderId).html(timeseriesSubDimensionsHtml);
    console.log(timeseriesSubDimensionsHtml);
  },

  dataEventHandler: function(e) {
    if (Object.is(e.target.type, "checkbox")) {
      this.checkboxClickEvent.notify(e.target);
    }
  },

  setupListeners : function() {
    $('#show-details').change(this.dataEventHandler.bind(this));
    $('#show-cumulative').change(this.dataEventHandler.bind(this));
  },

  setupListenerForSubDimension : function () {

  }
}

