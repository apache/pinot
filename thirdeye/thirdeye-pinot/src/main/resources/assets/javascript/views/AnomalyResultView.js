function AnomalyResultView() {

}

AnomalyResultView.prototype = {
  init : function() {

  },

  render : function() {
    var result_anomalies_template_compiled = anomalies_template_compiled({});
    $("#anomalies-place-holder").html(result_anomalies_template_compiled);
    renderAnomaliesTab();
  }

}

function renderAnomaliesTab() {

  // METRIC SELECTION
  var metrics = [ "m1", "m2" ];
  $('#metric-search-input').tokenfield({
    autocomplete : {
      source : metrics,
      delay : 100
    },
    showAutocompleteOnFocus : true
  })

  // TIME RANGE SELECTION
  var start = moment().subtract(6, 'days');
  var end = moment();

  function cb(start, end) {
    $('#anomalies-time-range span').addClass("time-range").html(start.format('MMM D, ') + start.format('hh:mm a') + '  &mdash;  ' + end.format('MMM D, ') + end.format('hh:mm a'));
  }

  $('#anomalies-time-range').daterangepicker({
    startDate : start,
    endDate : end,
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
  }, cb);

  cb(start, end);

  // CHART GENERATION
  var chart = c3.generate({
    bindto : '#anomaly-chart',
    data : {
      x : 'date',
      columns : [ [ 'date', '2016-01-01', '2016-01-2', '2016-01-3', '2016-01-4', '2016-01-05', '2016-01-06', '2016-01-07' ], [ 'current', 30, 200, 100, 400, 150, 250, 60 ],
          [ 'baseline', 35, 225, 200, 600, 170, 220, 70 ] ],
      type : 'spline'
    },
    legend : {
      show : false,
      position : 'top'
    },
    axis : {
      y : {
        show : true
      },
      x : {
        type : 'timeseries',
        show : true
      }
    },
    regions : [ {
      start : '2016-01-3',
      end : '2016-01-5'
    } ]
  });

}