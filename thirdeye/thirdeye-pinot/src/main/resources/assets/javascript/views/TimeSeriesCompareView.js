function TimeSeriesCompareView(timeSeriesCompareModel) {
  this.timeSeriesCompareModel = timeSeriesCompareModel;

  var timeseries_contributor_template = $("#timeseries-contributor-template").html();
  this.timeseries_contributor_template_compiled = Handlebars.compile(timeseries_contributor_template);
  this.timeseries_contributor_placeHolderId = "#timeseries-contributor-placeholder";

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


    // CHART GENERATION
    var chart = c3.generate({
      bindto : '#analysis-chart',
      data : {
        x : 'date',
        columns : this.timeSeriesCompareModel.timeSeriesObject.columns,
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
        start : this.timeSeriesCompareModel.timeSeriesObject.start,
        end : this.timeSeriesCompareModel.timeSeriesObject.end
      } ]
    });


    this.setupListeners();
  },

  dataEventHandler: function(e) {
    if (Object.is(e.target.type, "checkbox")) {
      this.checkboxClickEvent.notify(e.target);
    }
  },

  setupListeners : function() {
    $('#show-details').change(this.dataEventHandler.bind(this));
    $('#show-cumulative').change(this.dataEventHandler.bind(this));
  }
}

