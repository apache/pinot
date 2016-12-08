function DashboardController(parentController) {
  this.parentController = parentController;
  this.dashboardModel = new DashboardModel();
  this.dashboardView = new DashboardView(this.dashboardModel);

  this.anomalySummaryController = new AnomalySummaryController(this);
  this.woWSummaryController = new WoWSummaryController(this);

  this.dashboardView.tabClickEvent.attach(this.onTabClickEventHandler.bind(this));
  this.dashboardView.hideDataRangePickerEvent.attach(this.hideDataRangePickerEventHandler.bind(this));
}

DashboardController.prototype = {
  //TODO: figure out how to invoke this function from page callback
  handleAppEvent : function(hashParams) {
    hashParams = hashParams.hashParams;
    console.log("hashParams:");
    console.log(hashParams);
    this.dashboardModel.init(hashParams);
    this.dashboardModel.update();
    this.dashboardView.init(hashParams);
    this.dashboardView.render();
    if (this.dashboardModel.mode == "AnomalySummary") {
      this.anomalySummaryController.handleAppEvent(hashParams)
    } else if (this.dashboardModel.mode == "WoWSummary") {
      this.woWSummaryController.handleAppEvent(hashParams)
    }

  },

  init : function() {

  },

  onTabClickEventHandler : function(sender, args) {
    var params = this.dashboardModel.hashParams;
    if (args.previousTab != args.targetTab) {
      if (args.targetTab == "#anomaly-summary-tab") {
        this.anomalySummaryController.handleAppEvent(params);
        this.dashboardModel.mode = "AnomalySummary";
      } else if (args.targetTab == "#wow-summary-tab") {
        this.woWSummaryController.handleAppEvent(params);
        this.dashboardModel.mode = "WoWSummary";
      }
    }
  },

  hideDataRangePickerEventHandler: function(sender, args) {
    var dataRangePicker = args.dataRangePicker;
    if (this.dashboardModel.getStartTime() != dataRangePicker.startDate ||
        this.dashboardModel.getEndTime() != dataRangePicker.endDate) {
      this.dashboardModel.setStartTime(dataRangePicker.startDate);
      this.dashboardModel.setEndTime(dataRangePicker.endDate);

      if (this.dashboardModel.mode == "AnomalySummary") {
        this.anomalySummaryController.handleAppEvent(this.dashboardModel.hashParams);
      } else if (this.dashboardModel.mode == "WoWSummary") {
        this.woWSummaryController.handleAppEvent(this.dashboardModel.hashParams);
      }
    }

  }

};
