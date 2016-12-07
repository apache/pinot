function DashboardView(dashboardModel) {
  this.dashboardModel = dashboardModel;
  this.tabClickEvent = new Event(this);
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
  init: function () {



  },

  render: function () {
    $("#dashboard-place-holder").html(this.dashboard_template_compiled(this.dashboardModel));
    $('#dashboard-tabs a:first').click();

    // DASHBOARD SELECTION
    var countries = [ {
      value : 'Andorra',
      data : 'AD'
    }, {
      value : 'Zimbabwe',
      data : 'ZZ'
    } ];
    $('#dashboard-input').autocomplete({
      lookup : countries,
      onSelect : function(suggestion) {
        alert('You selected: ' + suggestion.value + ', ' + suggestion.data);
      }
    });

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
    $('#dashboard-tabs a').click(function (e) {
      e.preventDefault();
      $(this).tab('show');
    });

    var self = this;
    var tabSelectionEventHandler = function (e) {
      var targetTab = $(e.target).attr('href');
      var previousTab = $(e.relatedTarget).attr('href');
      var args = {targetTab: targetTab, previousTab: previousTab};
      self.tabClickEvent.notify(args);
    };
    $('#dashboard-tabs a[data-toggle="tab"]').on('shown.bs.tab', tabSelectionEventHandler);
  }

};

