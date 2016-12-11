function AnomalyResultController(parentController) {
  this.parentController = parentController;
  this.anomalyResultModel = new AnomalyResultModel();
  this.anomalyResultView = new AnomalyResultView(this.anomalyResultModel);

  this.anomalyResultView.metricChangeEvent.attach(this.metricChangeEventHandler.bind(this));
  this.anomalyResultView.hideDataRangePickerEvent.attach(this.hideDataRangePickerEventHandler.bind(this));
  this.anomalyResultView.rootCauseAnalysisButtonClickEvent.attach(this.rootCauseAnalysisButtonClickEventHandler.bind(this));
  this.anomalyResultView.showDetailsLinkClickEvent.attach(this.showDetailsLinkClickEventHandler.bind(this));
  this.anomalyResultView.anomalyFeedbackSelectEvent.attach(this.anomalyFeedbackSelectEventHandler.bind(this));
}

AnomalyResultController.prototype = {
  handleAppEvent: function (hashParams) {
    console.log("Inside handle app event of AnomalyResultController");
    this.anomalyResultModel.init(hashParams);
    this.anomalyResultModel.update();
    this.anomalyResultView.init();
    this.anomalyResultView.render();
  },
  metricChangeEventHandler: function(sender, args) {
    console.log("Inside button click event of Metric for AnomalyResuleController");
    console.log(args);
  },
  hideDataRangePickerEventHandler: function(sender, args) {
    console.log("received hide date range picker event");
    console.log(args);
    if (this.anomalyResultModel.startDate != args.startDate ||
        this.anomalyResultModel.endDate != args.endDate) {
      this.anomalyResultModel.startDate = args.startDate;
      this.anomalyResultModel.endDate = args.endDate;
      console.log("Date changed:");
      console.log(this.anomalyResultModel.startDate);
      console.log(this.anomalyResultModel.endDate);
      this.handleAppEvent();
    }
  },
  rootCauseAnalysisButtonClickEventHandler: function (sender, args) {
    console.log("received root cause analysis button click event at AnomalyResultController");
    console.log(args);

    // Send this event and the args to parent controller, to route to AnalysisController
  },
  showDetailsLinkClickEventHandler: function (sender, args) {
    console.log("received show details link click event at AnomalyResultController");
    console.log(args);

    // Send this event and the args to parent controller, to route to details page
  },
  anomalyFeedbackSelectEventHandler: function(sender, args) {
    console.log("received anomaly feedback select event at AnomalyResultController");
    console.log(args);

    // Change the model's feedback
    // Model updates backend?
  }
};
