function DashboardController(parentController) {
  this.parentController = parentController;
  this.dashboardModel = new DashboardModel();
  this.dashboardView = new DashboardView(this.dashboardModel);

  this.anomalySummaryController = new AnomalySummaryController(this);
  this.woWSummaryController = new WoWSummaryController(this);

  this.dashboardView.tabClickEvent.attach(this.onSubTabSelectionEventHandler.bind(this));
  this.dashboardView.hideDataRangePickerEvent.attach(this.hideDataRangePickerEventHandler.bind(this));
  this.dashboardView.onDashboardSelectionEvent.attach(this.onDashboardSelectionEventHandler.bind(this));
}

DashboardController.prototype = {
  //TODO: figure out how to invoke this function from page callback
  handleAppEvent : function(hashParams) {
    hashParams = hashParams.hashParams;
    console.log("hashParams:");
    console.log(hashParams);
    this.refreshView(hashParams);
  },

  init : function(hashParams) {
    this.dashboardModel.init(hashParams);
    this.dashboardView.init(hashParams);
  },

  refreshView: function(hashParams) {
    this.dashboardModel.update(hashParams);
    this.dashboardView.render();
    if (hashParams.dashboardName) {
      if (this.dashboardModel.mode == "AnomalySummary") {
        $('#anomaly-summary-tab').click();
      } else if (this.dashboardModel.mode == "WoWSummary") {
        $('#wow-summary-tab').click();
      } else {
        $('#dashboard-tabs a:first').click();
      }
    }
  },

  onSubTabSelectionEventHandler : function(sender, args) {
    var params = this.dashboardModel.hashParams;
    if (args.previousTab != args.targetTab) {
      if (args.targetTab == "#anomaly-summary-tab") {
        this.dashboardModel.mode = "AnomalySummary";
        this.anomalySummaryController.handleAppEvent(params);
      } else if (args.targetTab == "#wow-summary-tab") {
        this.dashboardModel.mode = "WoWSummary";
        this.woWSummaryController.handleAppEvent(params);
      }
    }
  },

  hideDataRangePickerEventHandler: function(sender, args) {
    var dataRangePicker = args.dataRangePicker;
    if (this.dashboardModel.getStartTime() != dataRangePicker.startDate ||
        this.dashboardModel.getEndTime() != dataRangePicker.endDate) {
      // Copy date range  to local model for checking if new date range needs update
      this.dashboardModel.setStartTime(dataRangePicker.startDate);
      this.dashboardModel.setEndTime(dataRangePicker.endDate);
      // Copy date range to global hash params for updating other modules
      var hashParams = this.dashboardModel.hashParams;
      hashParams.startTime = dataRangePicker.startDate;
      hashParams.endTime = dataRangePicker.endDate;

      this.refreshView(hashParams);
    }

  },

  onDashboardSelectionEventHandler: function(sender, args) {
    this.dashboardModel.hashParams.dashboardName = args.dashboardName;
    this.dashboardModel.hashParams.dashboardId = args.dashboardId;
    console.log(this.dashboardModel.hashParams);
    this.refreshView(this.dashboardModel.hashParams);
  }

};
