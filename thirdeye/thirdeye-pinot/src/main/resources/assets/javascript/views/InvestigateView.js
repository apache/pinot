function InvestigateView(investigateModel) {
  // Compile template
  const investigate = $("#investigate-template").html();
  this.investigate_template_compiled = Handlebars.compile(investigate);
  this.investigateModel = investigateModel;

  this.anomalyID;
  this.anomaly;
  this.wowData;
  this.investigateData;
  this.viewContributionClickEvent = new Event(this);

  this.investigateModel.renderViewEvent.attach(this.renderViewEventHandler.bind(this));
}

InvestigateView.prototype = {
  init(params = {}) {
    const { anomalyId } = params;
    if (this.anomalyId == anomalyId) {
      this.render();
    } else {
      this.destroy();
    }
    this.anomalyId = anomalyId;
  },

  renderViewEventHandler() {
    this.setViewData();
    this.render();
  },

  setViewData() {
    const anomaly = this.investigateModel.getAnomaly();
    const wowData = this.investigateModel.getWowData();
    const currentValue = wowData.currentVal;
    const wowResults = this.formatWowResults(wowData.compareResults, anomaly);
    const externalUrls = anomaly.externalUrl ? JSON.parse(anomaly.externalUrl) : null;

    this.investigateData = {
      anomaly,
      currentValue,
      externalUrls,
      wowResults,
    };
  },

  formatWowResults( wowResults, args = {}){
    const { anomalyStart, anomalyEnd, dataset, metricId, timeUnit, currentStart, currentEnd} = args;
    const granularity = timeUnit === 'MINUTES' ? '5_MINUTES' : timeUnit;
    const filters = {}
    const start = moment(currentStart);
    const end = moment(currentEnd);
    const heatMapCurrentStart = moment(anomalyStart);
    const heatMapCurrentEnd = moment(anomalyEnd);

    return wowResults
      .filter(wow => wow.compareMode !== 'Wo4W')
      .map((wow) => {
        const offset = constants.WOW_MAPPING[wow.compareMode];
        const baselineStart = start.clone().subtract(offset, 'days');
        const baselineEnd = end.clone().subtract(offset, 'days');
        const heatMapBaselineStart = heatMapCurrentStart.clone().subtract(offset, 'days');
        const heatMapBaselineEnd = heatMapCurrentEnd.clone().subtract(offset, 'days');
        wow.change *= 100;
        wow.url = `thirdeye#analysis?currentStart=${start.valueOf()}&currentEnd=${end.valueOf()}&` +
            `baselineStart=${baselineStart.valueOf()}&baselineEnd=${baselineEnd.valueOf()}&` +
            `compareMode=${wow.compareMode}&metricId=${metricId}&filters={}&granularity=${granularity}&` +
            `dimension=All&heatMapCurrentStart=${heatMapCurrentStart.valueOf()}&` +
            `heatMapCurrentEnd=${heatMapCurrentEnd.valueOf()}&heatMapBaselineStart=${heatMapBaselineStart.valueOf()}&` +
            `heatMapBaselineEnd=${heatMapBaselineEnd.valueOf()}`;
        return wow;
      });
  },



  render() {
    const template_with_anomaly = this.investigate_template_compiled(this.investigateData);
    $("#investigate-place-holder").html(template_with_anomaly);

    const anomalyFeedback = this.investigateModel.getFeedbackType();

    if (anomalyFeedback) {
      $(`input[name=feedback-radio][value=${anomalyFeedback}]`).prop('checked', true);
    }
    this.renderAnomalyChart(this.investigateModel.getAnomaly());
    // this.setupListenerOnViewContributionLink();
    this.setupListenerOnUserFeedback();
    this.signalPhantomJs();
  },

  destroy() {
    $("#investigate-place-holder").children().remove();
  },

  signalPhantomJs() {
    if (typeof window.callPhantom === 'function') {
      window.callPhantom({message: 'ready'});
    }
  },

  setupListenerOnUserFeedback() {
    $("input[name=feedback-radio]").change((event) => {
      const userFeedback = event.target.value;
      const feedbackString = this.investigateModel.getFeedbackString(userFeedback);
      this.investigateModel.updateFeedback(userFeedback);

      $('#anomaly-feedback').html(`Resolved (${feedbackString})`);
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
        padding : {
          left : 100,
          right : 100
        },
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
            show : true,
            tick: {
              format: d3.format('.2s')
            }
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
