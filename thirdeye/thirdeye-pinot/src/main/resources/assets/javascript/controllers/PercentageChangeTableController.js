function PercentageChangeTableController(parentController) {
  this.parentController = parentController;
  this.percentageChangeTableModel = new PercentageChangeTableModel();
  this.percentageChangeTableView = new PercentageChangeTableView(this.percentageChangeTableModel);

  // Register listener
  this.percentageChangeTableView.checkboxClickEvent.attach(this.onCheckboxClickEventHandler.bind(this));

  this.params
}

PercentageChangeTableController.prototype = {
  init: function () {

  },

  handleAppEvent: function (params) {
    this.update(params);
  },

  onCheckboxClickEventHandler: function (sender, args) {
    if (Object.is(args.id, "show-details")) {
      this.percentageChangeTableModel.showDetailsChecked = args.checked;
    } else if (Object.is(args.id, "show-cumulative")) {
      this.percentageChangeTableModel.showCumulativeChecked = args.checked;
    }

    params = this.percentageChangeTableModel.params;
    // TODO: update params according to the event
    this.update(params);
  },

  update: function(params) {
    this.percentageChangeTableModel.init(params);
    this.percentageChangeTableModel.rebuild();
    this.percentageChangeTableView.render();
  }
};
