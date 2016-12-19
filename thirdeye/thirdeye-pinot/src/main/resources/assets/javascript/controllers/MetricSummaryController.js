function MetricSummaryController(parentController){
  this.parentController = parentController;
  this.metricSummaryModel = new MetricSummaryModel();
  this.metricSummaryView = new MetricSummaryView(this.metricSummaryModel);
}


MetricSummaryController.prototype ={

    handleAppEvent: function(){
      var params = HASH_SERVICE.getParams();
      this.metricSummaryModel.init(params);
      this.metricSummaryModel.rebuild();
      this.metricSummaryView.render();
    },
    onDashboardInputChange: function(){

    },

    init:function(){

    }


}
