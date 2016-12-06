function AnomalySummaryController(parentController){
  this.parentController = parentController;
  this.anomalySummaryModel = new AnomalySummaryModel();
  this.anomalySummaryView = new AnomalySummaryView(this.anomalySummaryModel);
}


AnomalySummaryController.prototype ={
    
    handleAppEvent: function(params){
      this.anomalySummaryModel.init(params);
      this.anomalySummaryModel.rebuild();
      this.anomalySummaryView.refresh();
    },
    onDashboardInputChange: function(){
      
    },
    
    init:function(){
      
    }
    
    
}