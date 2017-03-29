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
    const { anomalyRegionStart, anomalyRegionEnd, dataset, metric } = args;
    const start = moment(anomalyRegionStart);
    const end = moment(anomalyRegionEnd);
    const wowMapping = {
      WoW: 7,
      Wo2W: 14,
      Wo3W: 21,
      Wo4W: 28
    };

    return wowResults
      .filter(wow => wow.compareMode !== 'Wo4W')
      .map((wow) => {
        const offset = wowMapping[wow.compareMode];
        const baselineStart = start.clone().subtract(offset, 'days');
        const baselineEnd = end.clone().subtract(offset, 'days');
        wow.change *= 100;
        wow.url = `dashboard#view=compare&dataset=${dataset}&compareMode=WoW&aggTimeGranularity=aggregateAll&currentStart=${start.valueOf()}&currentEnd=${end.valueOf()}&baselineStart=${baselineStart.valueOf()}&baselineEnd=${baselineEnd.valueOf()}&metrics=${metric}`;
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

  setupListenerOnUserFeedback() {
    $("input[name=feedback-radio]").change((event) => {
      const userFeedback = event.target.value;
      const feedbackString = this.investigateModel.getFeedbackString(userFeedback);
      this.investigateModel.updateFeedback(userFeedback);

      $('#anomaly-feedback').html(`Resolved (${feedbackString})`);
    });
  },

  getGranularity(start, end) {
    const hoursDiff = end.diff(start, 'hours');
    if (hoursDiff < 3) {
      return '5_MINUTES';
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
              format: d3.format('.2f')
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
