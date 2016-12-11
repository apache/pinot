function AppView(appModel) {
  this.appModel = appModel;
  this.tabClickEvent = new Event(this);
  this.currentActiveTab = undefined;
}
AppView.prototype = {

  init : function() {
    var self = this;
    var tabSelectionEventHandler = function(e) {
      e.preventDefault();
      var targetTab = $(e.target).attr('href');
      var previousTab = $(e.relatedTarget).attr('href');
      var args = {
        targetTab : targetTab,
        previousTab : previousTab
      };
      //don't notify if the tab is already active
      if(!$(e.target).parent().hasClass('active')) {
        self.tabClickEvent.notify(args);
      }
    };
    $('#main-tabs').click(tabSelectionEventHandler);
    $('#admin-tabs').click(tabSelectionEventHandler);
    // compile thirdeye.ftl
  },

  render : function() {
    console.log("Switching to tab:" + this.appModel.tabSelected);
    switch (this.appModel.tabSelected) {
    case "dashboard":
    case "anomalies":
    case "analysis":
      $('#main-tabs a[href=#' + this.appModel.tabSelected + "]").tab('show');
      break;
    }
    // compiledHtml
  }

}
