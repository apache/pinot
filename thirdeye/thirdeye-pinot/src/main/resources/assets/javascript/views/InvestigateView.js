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
    const anomaly = this.investigateModel.getAnomaly();
    const wowData = this.investigateModel.getWowData();
    const currentValue = wowData.currentVal;
    wowData.compareResults.forEach((result) => {
      result.change *= 100;
    });
    const [ wow, wow2, wow3 ] = wowData.compareResults;

    const investigateData = {
      anomaly,
      currentValue,
      wow,
      wow2,
      wow3
    };

    const template_with_anomaly = this.investigate_template_compiled(investigateData);
    $("#investigate-place-holder").html(template_with_anomaly);
    this.renderAnomalyChart(anomaly);
    this.setupListenerOnViewContributionLink();
  },

  setupListenerOnViewContributionLink() {
    const anomaly = this.investigateModel.getAnomaly();
    $('.thirdeye-link').click((e) => {
      const wowType = e.target.id;
      const wowMapping = {
        wow1: 7,
        wow2: 14,
        wow3: 21
      };
      const offset = wowMapping[wowType] || 7;
      const currentStart = moment(anomaly.currentStart);
      const currentEnd = moment(anomaly.currentEnd);
      const granularity = this.getGranularity(currentStart, currentEnd);

      const analysisParams = {
        metricId: anomaly.metricId,
        currentStart,
        currentEnd,
        granularity,
        baselineStart: currentStart.clone().subtract(offset, 'days'),
        baselineEnd: currentEnd.clone().subtract(offset, 'days')
      };

      // if (anomaly.anomalyFunctionDimension.length) {
      //   const dimension = JSON.parse(anomaly.anomalyFunctionDimension);
      //   analysisParams.dimension = Object.keys(dimension)[0];
      // }
      this.viewContributionClickEvent.notify(analysisParams);
    });
  },

  getGranularity(start, end) {
    const hoursDiff = end.diff(start, 'hours');
    if (hoursDiff < 3) {
      return '5_MINUTES'
    } else if (hoursDiff < 120) {
      return 'HOURS';
    } else {
      return 'DAYS';
    }
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
