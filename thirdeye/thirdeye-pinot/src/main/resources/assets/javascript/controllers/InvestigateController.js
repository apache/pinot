function InvestigateController(parentController) {
  this.parentController = parentController;
  this.investigateModel = new InvestigateModel();
  this.investigateView = new InvestigateView(this.investigateModel);
  this.investigateView.viewContributionClickEvent.attach(this.viewContributionEventHandler.bind(this));
}

InvestigateController.prototype = {
  handleAppEvent() {
    const hashParams = HASH_SERVICE.getParams();
    this.investigateModel.init(hashParams);
    this.investigateView.init(hashParams);
  },

  viewContributionEventHandler(sender, args) {
    console.log(args);
  },
};

