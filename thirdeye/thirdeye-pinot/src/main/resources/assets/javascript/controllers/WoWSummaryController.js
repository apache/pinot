function WoWSummaryController(parentController){
  this.parentController = parentController;
  this.woWSummaryModel = new WoWSummaryModel();
  this.woWSummaryView = new WoWSummaryView(this.woWSummaryModel);
}


WoWSummaryController.prototype ={

    handleAppEvent: function(params){
      this.woWSummaryModel.init(params);
      this.woWSummaryModel.rebuild();
      this.woWSummaryView.render();
    },
    onDashboardInputChange: function(){

    },

    init:function(){

    }


};
