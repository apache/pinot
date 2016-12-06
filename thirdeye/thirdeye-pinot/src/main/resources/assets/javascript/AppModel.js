function AppModel() {
  this.tabSelected = "dashboard"
}
AppModel.prototype = {

  init : function(hash) {
  
  },

  getSelectedTab : function() {
    return this.tabSelected;
  }

}