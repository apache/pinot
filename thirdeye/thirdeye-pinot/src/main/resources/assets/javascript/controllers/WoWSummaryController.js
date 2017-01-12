function WoWSummaryController(parentController){
  this.parentController = parentController;
  this.wowSummaryModel = new WoWSummaryModel();
  this.wowSummaryView = new WoWSummaryView(this.wowSummaryModel);
}


WoWSummaryController.prototype ={

    handleAppEvent: function(params){
      //var params = HASH_SERVICE.getParams();
      this.wowSummaryModel.reset();
      this.wowSummaryModel.setParams();
      this.wowSummaryModel.rebuild();
    },

    init:function(){

    }


};
