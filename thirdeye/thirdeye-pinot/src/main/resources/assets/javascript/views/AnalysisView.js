function AnalysisView(analysisModel) {
  // Compile template
  var analysis_template = $("#analysis-template").html();
  this.analysis_template_compiled = Handlebars.compile(analysis_template);
  this.analysisModel = analysisModel;
}

AnalysisView.prototype = {

  init : function() {

  },

  render : function() {
    $("#analysis-place-holder").html(this.analysis_template_compiled);

    renderAnalysisTab();
  }
};

function renderAnalysisTab() {

  // METRIC SELECTION
  var self = this;
  $('#analysis-metric-input').select2({
    theme : "bootstrap",
    placeholder : "search for Metric(s)",
    ajax : {
      url : constants.METRIC_AUTOCOMPLETE_ENDPOINT,
      delay : 250,
      data : function(params) {
        var query = {
          name : params.term,
          page : params.page
        }
        // Query paramters will be ?search=[term]&page=[page]
        return query;
      },
      processResults : function(data) {
        var results = [];
        $.each(data, function(index, item) {
          results.push({
            id : item.id,
            text : item.alias
          });
        });
        return {
          results : results
        };
      }
    }
  }).on("select2:select", function(e) {
    var selectedElement = $(e.currentTarget);
    var selectedData = selectedElement.select2("data");
    console.log("Selected data:" + JSON.stringify(selectedData));
    var selectedMetricIds = selectedData.map(function(e) {return e.id})
    console.log('Selected Metric Ids: ' + selectedMetricIds);
    self.metricChangeEvent.notify(selectedMetricIds);
    //TODO: handle this event
  });

  // TIME RANGE SELECTION
  var current_start = moment().subtract(1, 'days');
  var current_end = moment();

  var baseline_start = moment().subtract(6, 'days');
  var baseline_end = moment().subtract(6, 'days');

  function current_range_cb(start, end) {
    $('#current-range span').addClass("time-range").html(start.format('MMM D, ') + start.format('hh:mm a') + '  &mdash;  ' + end.format('MMM D, ') + end.format('hh:mm a'));
  }
  function baseline_range_cb(start, end) {
    $('#baseline-range span').addClass("time-range").html(start.format('MMM D, ') + start.format('hh:mm a') + '  &mdash;  ' + end.format('MMM D, ') + end.format('hh:mm a'));
  }

  $('#current-range').daterangepicker({
    startDate : current_start,
    endDate : current_end,
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
  }, current_range_cb);

  $('#baseline-range').daterangepicker({
    startDate : baseline_start,
    endDate : baseline_end,
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
  }, baseline_range_cb);

  current_range_cb(current_start, current_end);
  baseline_range_cb(baseline_start, baseline_end);
}
