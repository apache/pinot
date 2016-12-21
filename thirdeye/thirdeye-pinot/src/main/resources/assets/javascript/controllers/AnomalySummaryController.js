function AnomalySummaryController(parentController){
  this.parentController = parentController;
  this.anomalySummaryModel = new AnomalySummaryModel();
  this.anomalySummaryView = new AnomalySummaryView(this.anomalySummaryModel);
}


AnomalySummaryController.prototype ={

    handleAppEvent: function(params){
      //var params = HASH_SERVICE.getParams();
      this.anomalySummaryModel.reset();
      this.anomalySummaryModel.setParams(params);
      this.anomalySummaryModel.rebuild();
    },
    onDashboardInputChange: function(){

    },

    init:function(){

    }


}
