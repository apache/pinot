function AppView(appModel) {
  this.appModel = appModel;
  this.tabClickEvent = new Event(this);

}
AppView.prototype = {

  init : function() {
    var self = this;
    var tabSelectionEventHandler = function (e) {
      console.log(e);
      var targetTab = $(e.target).attr('href');
      var previousTab = $(e.relatedTarget).attr('href');
      var args = {targetTab: targetTab, previousTab: previousTab};
      self.tabClickEvent.notify(args);
    };
    $('#main-tabs a[data-toggle="tab"]').on('shown.bs.tab', tabSelectionEventHandler);
    $('#admin-tabs a[data-toggle="tab"]').on('shown.bs.tab', tabSelectionEventHandler);
    $('#global-navbar a').on('shown.bs.tab', tabSelectionEventHandler);
    //compile thirdeye.ftl
  },

  render : function() {
    //compiledHtml
  }

}
