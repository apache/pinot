function InvestigateController(parentController) {
  this.parentController = parentController;
  this.investigateModel = new InvestigateModel();
  this.investigateView = new InvestigateView(this.investigateModel);
}

InvestigateController.prototype = {
  handleAppEvent: function () {
    const hashParams = HASH_SERVICE.getParams();
    this.investigateModel.init(hashParams);
    this.investigateView.init(hashParams);
  },
};

