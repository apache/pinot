function TimeSeriesCompareView(timeSeriesCompareModel) {
  this.timeSeriesCompareModel = timeSeriesCompareModel;

  var timeseries_contributor_template = $("#timeseries-contributor-template").html();
  this.timeseries_contributor_template_compiled = Handlebars.compile(timeseries_contributor_template);
  this.timeseries_contributor_placeHolderId = "#timeseries-contributor-placeholder";

  var timeseries_subdimension_legend_template = $("#timeseries-subdimension-legend-template").html();
  this.timeseries_subdimension_legend_template_compiled = Handlebars.compile(timeseries_subdimension_legend_template);
  this.timeseries_subdimension_legend_placeHolderId = "#analysis-chart-legend";

  var contributor_table_template = $("#contributor-table-details-template").html();
  this.contributor_table_template_compiled = Handlebars.compile(contributor_table_template);
  this.contributor_table_placeHolderId = "#contributor-table-placeholder";
  this.checkboxClickEvent = new Event();
}

TimeSeriesCompareView.prototype = {
  init: function () {
  },

  render: function () {
    if (this.timeSeriesCompareModel.subDimensionContributionDetails) {
      // render chart
      var timeseriesContributorViewResult = this.timeseries_contributor_template_compiled(this.timeSeriesCompareModel);
      $(this.timeseries_contributor_placeHolderId).html(timeseriesContributorViewResult);
      this.loadChart(this.timeSeriesCompareModel.subDimensionContributionDetails.contributionMap['All']);

      // render chart legned
      var timeseriesSubDimensionsHtml = this.timeseries_subdimension_legend_template_compiled(this.timeSeriesCompareModel);
      $(this.timeseries_subdimension_legend_placeHolderId).html(timeseriesSubDimensionsHtml);
      this.setupListenerForSubDimension();

      // render contributor table
      var contributorTableResult = this.contributor_table_template_compiled(this.timeSeriesCompareModel);
      $(this.contributor_table_placeHolderId).html(contributorTableResult);

      // this.setupListeners();
    }
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
          show : true,
          tick:{
            "culling":{"max":100},
            "count":10,
            // "rotate":30,   // this will rotate the x axis values
            "fit":true,
            "format":"%m-%d %H:%M"}
        }
      }
    });
  },

  dataEventHandler: function (e) {
    if (Object.is(e.target.type, "checkbox")) {
      this.checkboxClickEvent.notify(e.target);
    }
  },

  // setupListeners: function () {
  //   $('#show-details').change(this.dataEventHandler.bind(this));
  //   $('#show-cumulative').change(this.dataEventHandler.bind(this));
  // },

  setupListenerForSubDimension: function () {
    var self = this;
    for (var i in this.timeSeriesCompareModel.subDimensions) {
      $("#a-sub-dimension-" + i + " a").click(self, function (e) {
        var index = e.currentTarget.getAttribute('id');
        var subDimension = self.timeSeriesCompareModel.subDimensions[index];
        self.loadChart(self.timeSeriesCompareModel.subDimensionContributionDetails.contributionMap[subDimension]);
      });
    }
  }
}

