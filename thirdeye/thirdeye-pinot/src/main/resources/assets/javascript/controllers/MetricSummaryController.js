function MetricSummaryController(parentController){
  this.parentController = parentController;
  this.metricSummaryModel = new MetricSummaryModel();
  this.metricSummaryView = new MetricSummaryView(this.metricSummaryModel);
}

MetricSummaryController.prototype ={
    handleAppEvent: function(){
      console.log("handleAppEvent of metricSummary");
      this.metricSummaryModel.reset();
      this.metricSummaryModel.setParams();
      this.metricSummaryModel.rebuild();
    }
}
