function WoWSummaryController(parentController){
  this.parentController = parentController;
  this.wowSummaryModel = new WoWSummaryModel();
  this.wowSummaryView = new WoWSummaryView(this.wowSummaryModel);
}


WoWSummaryController.prototype ={
    handleAppEvent: function(){
      this.wowSummaryModel.reset();
      this.wowSummaryModel.setParams();
      this.wowSummaryModel.rebuild();
    }
};
