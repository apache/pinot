function DashboardView(dashboardModel) {
  this.dashboardModel = dashboardModel;
  this.tabClickEvent = new Event(this);
  this.onDashboardSelectionEvent = new Event(this);

  // Compile HTML template
  var dashboard_template = $("#dashboard-template").html();
  this.dashboard_template_compiled = Handlebars.compile(dashboard_template);
  this.inited = false;
}

DashboardView.prototype = {
  render : function() {
    if(!this.inited){
      this.init();
      this.inited = false;
    }
  },
  init : function() {
    var self = this;
    var result = self.dashboard_template_compiled(self.dashboardModel);

    // autocomplete
    $("#dashboard-place-holder").html(result);
    if(self.dashboardModel.dashboardName != null) {
      $("#dashboard-content").show();
    }
    console.log("this.dashboardModel.tabSelected:" + self.dashboardModel.tabSelected);
    $('#dashboard-tabs a[href="#' + self.dashboardModel.tabSelected + '"]').tab('show');

    // DASHBOARD SELECTION
    $('#dashboard-name-input').select2({
      theme : "bootstrap",
      placeholder : "Search for Dashboard",
      ajax : {
        url : constants.DASHBOARD_AUTOCOMPLETE_ENDPOINT,
        minimumInputLength : 3,
        delay : 250,
        data : function(params) {
          var query = {
            name : params.term,
            page : params.page
          }
          // Query paramters will be ?name=[term]&page=[page]
          return query;
        },
        processResults : function(data) {
          var results = [];
          $.each(data, function(index, item) {
            results.push({
              id : item.id,
              text : item.name
            });
          });
          return {
            results : results
          };
        }
      }
    }).on("select2:select", function(e) {
      var selectedElement = $(e.currentTarget);
      var selectedData = selectedElement.select2("data")[0];
      console.log("Selected data:" + JSON.stringify(selectedData))
      var selectedDashboardName = selectedData.text;
      var selectedDashboardId = selectedData.id;
      console.log('You selected: ' + selectedDashboardName);
      console.log(e);
      var args = {
        dashboardName : selectedDashboardName,
        dashboardId : selectedDashboardId
      };

      if (self.dashboardModel.dashboardName != selectedDashboardName) {
       self.onDashboardSelectionEvent.notify(args);
      }
    });

    this.setupListeners();
  },

  setupListeners : function() {
    var self = this;
    var tabSelectionEventHandler = function(e) {
      console.log("Switching tabs")
      var targetTab = $(e.target).attr('href');
      var previousTab = $(e.relatedTarget).attr('href');
      var args = {
        targetTab : targetTab,
        previousTab : previousTab
      };
      self.tabClickEvent.notify(args);
      e.preventDefault();
    };
    $('#dashboard-tabs a').click(tabSelectionEventHandler);
  }
};
