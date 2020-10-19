function AppModel() {
  this.tabSelected = "anomalies";
  this.hashParams = new HashParams();
  this.hashParams.startTime = moment().subtract(7, "days");
  this.hashParams.endTime = moment();
}
AppModel.prototype = {

  init : function(hashParams) {
    this.hashParams = hashParams;
  },

  getSelectedTab : function() {
    return this.tabSelected;
  }

};
