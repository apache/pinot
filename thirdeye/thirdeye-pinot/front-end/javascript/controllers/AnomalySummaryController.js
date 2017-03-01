function AnomalySummaryController(parentController){
  this.parentController = parentController;
  this.anomalySummaryModel = new AnomalySummaryModel();
  this.anomalySummaryView = new AnomalySummaryView(this.anomalySummaryModel);
}


AnomalySummaryController.prototype ={
    handleAppEvent: function(){
      console.log('anomaly summary controller');
      this.anomalySummaryModel.reset();
      this.anomalySummaryModel.setParams();
      this.anomalySummaryModel.rebuild();
    }
}
