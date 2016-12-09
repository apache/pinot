function AppView(appModel) {
  this.appModel = appModel;
  this.tabClickEvent = new Event(this);

}
AppView.prototype = {

  init : function() {
    var self = this;
    var tabSelectionEventHandler = function(e) {
      e.preventDefault();
      console.log(e);
      var targetTab = $(e.target).attr('href');
      var previousTab = $(e.relatedTarget).attr('href');
      var args = {
        targetTab : targetTab,
        previousTab : previousTab
      };
      self.tabClickEvent.notify(args);
    };
    $('#main-tabs').click(tabSelectionEventHandler);
    $('#admin-tabs').click(tabSelectionEventHandler);
    $('#global-navbar a').click(tabSelectionEventHandler);
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
