function AppView(appModel) {
  this.appModel = appModel;
  this.tabClickEvent = new Event(this);

}
AppView.prototype = {

  init : function(hash) {
    var tabSelectionEventHandler = function(e) {
      this.tabClickEvent.notify(e);
    }
    $('#main-tabs a[data-toggle="tab"]').on('shown.bs.tab', tabSelectionEventHandler);
    $('#admin-tabs a[data-toggle="tab"]').on('shown.bs.tab', tabSelectionEventHandler);
    $('#global-navbar a').on('shown.bs.tab', tabSelectionEventHandler);
    //compile thirdeye.ftl
  },

  render : function() {
    //compiledHtml
  }
  
}