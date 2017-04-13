function AppView(appModel) {
  this.appModel = appModel;
  this.tabClickEvent = new Event(this);
  this.currentActiveTab = undefined;
}
AppView.prototype = {
  init() {
    var tabSelectionEventHandler = (e) => {
      e.preventDefault();
      var targetTab = $(e.target).attr('href');
      var previousTab = $(e.relatedTarget).attr('href');
      var args = {
        targetTab : targetTab,
        previousTab : previousTab
      };
      //don't notify if the tab is already active
      if(!$(e.target).parent().hasClass('active')) {
        HASH_SERVICE.clear();
        this.tabClickEvent.notify(args);
      }
    };

    $('#main-tabs').click(tabSelectionEventHandler);
    $('#admin-tabs').click(tabSelectionEventHandler);
    // compile thirdeye.ftl
  },

  render() {
    switch (this.appModel.tabSelected) {
    case "dashboard":
    case "analysis":
      $('#main-tabs a[href=#' + this.appModel.tabSelected + "]").tab('show');
      break;
    case "anomalies":
      $('#main-tabs a[href=#anomalies]').tab('show');
      $('#anomalies-place-holder').show();
      $('#investigate-place-holder').hide();
      break;
    case "investigate":
      $('#main-tabs a[href=#investigate]').tab('show');
      $('#anomalies-place-holder').hide();
      $('#investigate-place-holder').show();
      break;
    }
  }
};
