function PercentageChangeTableController(parentController){
  this.parentController = parentController;
  this.contributorTableModel = new PercentageChangeTableModel();
  this.contributorTableView = new PercentageChangeTableView(this.contributorTableModel);
}


PercentageChangeTableController.prototype ={

    handleAppEvent: function(params){
      this.contributorTableModel.init(params);
      this.contributorTableModel.rebuild();
      this.contributorTableView.render();
      console.log("PercentageTable");
    },
    onDashboardInputChange: function(){

    },

    init:function(){

    }


}
