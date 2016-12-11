function DashboardView(dashboardModel) {
  this.dashboardModel = dashboardModel;

  this.tabClickEvent = new Event(this);
  this.hideDataRangePickerEvent = new Event(this);
  this.onDashboardSelectionEvent = new Event(this);

  this.timeRangeConfig = {
    startDate : this.dashboardModel.startTime,
    endDate : this.dashboardModel.endTime,
    dateLimit : {
      days : 60
    },
    showDropdowns : true,
    showWeekNumbers : true,
    timePicker : true,
    timePickerIncrement : 5,
    timePicker12Hour : true,
    ranges : {
      'Last 24 Hours' : [ moment(), moment() ],
      'Yesterday' : [ moment().subtract(1, 'days'), moment().subtract(1, 'days') ],
      'Last 7 Days' : [ moment().subtract(6, 'days'), moment() ],
      'Last 30 Days' : [ moment().subtract(29, 'days'), moment() ],
      'This Month' : [ moment().startOf('month'), moment().endOf('month') ],
      'Last Month' : [ moment().subtract(1, 'month').startOf('month'), moment().subtract(1, 'month').endOf('month') ]
    },
    buttonClasses : [ 'btn', 'btn-sm' ],
    applyClass : 'btn-primary',
    cancelClass : 'btn-default'
  };

  // Compile HTML template
  var dashboard_template = $("#dashboard-template").html();
  this.dashboard_template_compiled = Handlebars.compile(dashboard_template);
}

DashboardView.prototype = {
  init : function() {

  },
  render : function() {
    var self = this;
    result = this.dashboard_template_compiled(this.dashboardModel);
    $("#dashboard-place-holder").html(result);
    console.log("this.dashboardModel.tabSelected:" + this.dashboardModel.tabSelected);
    $('#dashboard-tabs a[href="#' + this.dashboardModel.tabSelected + '"]').tab('show');

    // DASHBOARD SELECTION
    var self = this;
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
    }).val(this.dashboardModel.dashboardName);

    // TIME RANGE SELECTION
    this.timeRangeConfig.startDate = this.dashboardModel.startTime;
    this.timeRangeConfig.endDate = this.dashboardModel.endTime;

    function dashboard_range_cb(start, end) {
      $('#dashboard-time-range span').addClass("time-range").html(start.format('MMM D, ') + start.format('hh:mm a') + '  &mdash;  ' + end.format('MMM D, ') + end.format('hh:mm a'));
    }

    $('#dashboard-time-range').daterangepicker(this.timeRangeConfig, dashboard_range_cb);

    dashboard_range_cb(this.timeRangeConfig.startDate, this.timeRangeConfig.endDate);

    this.setupListeners();
  },

  setupListeners : function() {
    var self = this;
    var tabSelectionEventHandler = function(e) {
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

    var hideDataRangePickerEventHandler = function(e, dataRangePicker) {
      var args = {
        e : e,
        dataRangePicker : dataRangePicker
      };
      self.hideDataRangePickerEvent.notify(args);
    };
    $('#dashboard-time-range').on('hide.daterangepicker', hideDataRangePickerEventHandler);
  }
};
