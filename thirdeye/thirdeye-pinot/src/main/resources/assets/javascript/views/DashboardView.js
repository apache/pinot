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
  init: function (hashParams) {

  },

  render: function () {
    var self = this;

    $("#dashboard-place-holder").html(this.dashboard_template_compiled(this.dashboardModel));
    $('#dashboard-tabs a:first').click();

    // DASHBOARD SELECTION
    console.log(this.dashboardModel.hashParams.dashboardName);

    // TODO : set selected dashboard in the input
    if (this.dashboardModel.hashParams.dashboardName) {
      $("#selected-dashboard").html(this.dashboardModel.hashParams.dashboardName);
    }

    $('#dashboard-input').autocomplete({
      minChars: 1,  // TODO : make this 3
      serviceUrl : constants.dashboardAutocompleteEndpoint,
      paramName : constants.dashboardAutocompleteQueryParam,
      transformResult : function(response) {
        return {
          suggestions : $.map($.parseJSON(response), function(item) {
            return {
              value: item.name,
              data : item.id
            };
          })
        };
      },
      onSelect: function (suggestion) {
        $('#dashboard-input').val(suggestion.value);
        console.log(suggestion);

        // TODO : send event to render summary
        var args = {dashboardName: suggestion.value, dashboardId: suggestion.data};
        self.onDashboardSelectionEvent.notify(args);
      }
    });


    // TIME RANGE SELECTION
    this.timeRangeConfig.startDate = this.dashboardModel.getStartTime();
    this.timeRangeConfig.endDate = this.dashboardModel.getEndTime();

    function dashboard_range_cb(start, end) {
      $('#dashboard-time-range span').addClass("time-range").html(start.format('MMM D, ') + start.format('hh:mm a') + '  &mdash;  ' + end.format('MMM D, ') + end.format('hh:mm a'));
    }

    $('#dashboard-time-range').daterangepicker(this.timeRangeConfig, dashboard_range_cb);

    dashboard_range_cb(this.timeRangeConfig.startDate, this.timeRangeConfig.endDate);

    this.setupListeners();
  },

  setupListeners : function() {
    var self = this;
    $('#dashboard-tabs a').click(function (e) {
      e.preventDefault();
      $(this).tab('show');
    });

    var tabSelectionEventHandler = function (e) {
      var targetTab = $(e.target).attr('href');
      var previousTab = $(e.relatedTarget).attr('href');
      var args = {targetTab: targetTab, previousTab: previousTab};
      self.tabClickEvent.notify(args);
    };
    $('#dashboard-tabs a[data-toggle="tab"]').on('shown.bs.tab', tabSelectionEventHandler);

    var hideDataRangePickerEventHandler = function(e, dataRangePicker) {
      var args = {e: e, dataRangePicker: dataRangePicker}
      self.hideDataRangePickerEvent.notify(args);
    };
    $('#dashboard-time-range').on('hide.daterangepicker', hideDataRangePickerEventHandler);
  }
};

