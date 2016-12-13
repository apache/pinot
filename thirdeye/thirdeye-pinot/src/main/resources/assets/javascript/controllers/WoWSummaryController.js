function WoWSummaryController(parentController){
  this.parentController = parentController;
  this.wowSummaryModel = new WoWSummaryModel();
  this.wowSummaryView = new WoWSummaryView(this.wowSummaryModel);
}


WoWSummaryController.prototype ={

    handleAppEvent: function(){
      var params = HASH_SERVICE.getParams();
      this.wowSummaryModel.init(params);
      this.wowSummaryModel.rebuild();
      this.wowSummaryView.render();
    },

    init:function(){

    }


};
