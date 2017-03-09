function InvestigateView(investigateModel) {
  // Compile template
  const investigate = $("#investigate-template").html();
  this.investigate_template_compiled = Handlebars.compile(investigate);
  this.investigateModel = investigateModel;

  this.viewContributionClickEvent = new Event(this);

  this.investigateModel.renderViewEvent.attach(this.renderViewEventHandler.bind(this));
}

InvestigateView.prototype = {
  init(params = {}) {
    const { anomalyId } = params;
    this.anomalyId = anomalyId;
  },

  renderViewEventHandler() {
    this.render();
  },

  render () {
    const anomaliesWrapper = this.investigateModel.getAnomaliesWrapper();
    const anomaly = anomaliesWrapper.anomalyDetailsList[0];
    const template_with_anomaly = this.investigate_template_compiled(anomaly);
    $("#investigate-place-holder").html(template_with_anomaly);
    this.renderAnomalyChart(anomaly);
    this.setupListenerOnViewContributionLink();
  },

  setupListenerOnViewContributionLink() {
    const investigateParams = this.investigateModel;
    $('.thirdeye-link').click((e) => {
      const wowType = e.target.id;

      console.log(investigateParams);
      console.log(e);
      this.viewContributionClickEvent.notify(investigateParams);
      // var anomaliesSearchMode = $('#anomalies-search-mode').val();
      // var metricIds = undefined;
      // var dashboardId = undefined;
      // var anomalyIds = undefined;

      // var functionName = $('#anomaly-function-dropdown').val();
      // var startDate = $('#anomalies-time-range-start').data('daterangepicker').startDate;
      // var endDate = $('#anomalies-time-range-start').data('daterangepicker').endDate;

      // var anomaliesParams = {
      //   anomaliesSearchMode : anomaliesSearchMode,
      //   startDate : startDate,
      //   endDate : endDate,
      //   pageNumber : 1,
      //   functionName : functionName
      // }

      // if (anomaliesSearchMode == constants.MODE_METRIC) {
      //   anomaliesParams.metricIds = $('#anomalies-search-metrics-input').val().join();
      // } else if (anomaliesSearchMode == constants.MODE_DASHBOARD) {
      //   anomaliesParams.dashboardId = $('#anomalies-search-dashboard-input').val();
      // } else if (anomaliesSearchMode == constants.MODE_ID) {
      //   anomaliesParams.anomalyIds = $('#anomalies-search-anomaly-input').val().join();
      //   delete anomaliesParams.startDate;
      //   delete anomaliesParams.endDate;
      // }

      // this.applyButtonEvent.notify(anomaliesParams);
    });
  },

  renderAnomalyChart(anomaly){
      const date = ['date'].concat(anomaly.dates);
      const currentValues = ['current'].concat(anomaly.currentValues);
      const baselineValues = ['baseline'].concat(anomaly.baselineValues);
      const chartColumns = [ date, currentValues, baselineValues ];
      const showPoints = date.length <= constants.MAX_POINT_NUM;


      // CHART GENERATION
      var chart = c3.generate({
        bindto : '#anomaly-investigate-chart',
        data : {
          x : 'date',
          xFormat : '%Y-%m-%d %H:%M',
          columns : chartColumns,
          type : 'line'
        },
        point: {
          show: showPoints,
        },
        legend : {
          position : 'inset',
          inset: {
            anchor: 'top-right',
          }
        },
        axis : {
          y : {
            show : true
          },
          x : {
            type : 'timeseries',
            show : true,
            tick: {
              fit: false,
            }
          }
        },
        regions : [ {
          axis : 'x',
          start : anomaly.anomalyRegionStart,
          end : anomaly.anomalyRegionEnd,
          class: 'anomaly-region',
          tick : {
            format : '%m %d %Y'
          }
        } ]
      });
  }

};
