
function AnomalyResultView(anomalyResultModel) {

  this.anomalyResultModel = anomalyResultModel;

  this.timeRangeConfig = {
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
  var anomalies_template = $("#anomalies-template").html();
  this.result_anomalies_template_compiled = Handlebars.compile(anomalies_template);
}

AnomalyResultView.prototype = {
  init : function() {

  },

  render : function() {


    // METRIC SELECTION
    var metrics = this.anomalyResultModel.metrics;
    console.log("metrics");
    console.log(metrics);

    $('#metric-search-input').autocomplete({
      lookup : metrics,
      onSelect : function(suggestion) {
        alert('You selected: ' + suggestion.value + ', ' + suggestion.data);
      }
    });



    // TIME RANGE SELECTION
    this.timeRangeConfig.startDate = this.anomalyResultModel.startDate;
    this.timeRangeConfig.endDate = this.anomalyResultModel.endDate;

    function cb(start, end) {
      $('#anomalies-time-range span').addClass("time-range").html(start.format('MMM D, ') + start.format('hh:mm a') + '  &mdash;  ' + end.format('MMM D, ') + end.format('hh:mm a'));
    }

    $('#anomalies-time-range').daterangepicker(this.timeRangeConfig, cb);

    cb(this.timeRangeConfig.startDate, this.timeRangeConfig.endDate);

    var anomalies = this.anomalyResultModel.anomalies;
    console.log("anomalies");
    console.log(anomalies);

    $("#anomalies-place-holder").html(this.result_anomalies_template_compiled);
    this.renderAnomaliesTab(anomalies);
  },

  renderAnomaliesTab: function (anomalies) {
    for (var i = 0; i < anomalies.length; i++) {
      var anomaly = anomalies[i];
      console.log(anomaly);
      var date = ['date'].concat(anomaly.dates);
      var current = ['current'].concat(anomaly.currentValues);
      var baseline = ['baseline'].concat(anomaly.baselineValues);
      var chartColumns = [ date, current, baseline];
      var regionStart = anomaly.anomalyRegionStart;
      var regionEnd = anomaly.anomalyRegionEnd;
      var current = anomaly.current;
      var baseline = anomaly.baseline;
      var dimension = anomaly.anomalyFunctionDimension;
      console.log(date);
      console.log(current);
      console.log(baseline);
      console.log(chartColumns);

      $("#current-value-"+i).html(current);
      $("#baseline-value-"+i).html(baseline);
      $("#region-"+i).html(regionStart + " - " + regionEnd)
      $("#dimension-"+i).html(dimension)

      // CHART GENERATION
      var chart = c3.generate({
        bindto : '#anomaly-chart-'+i,
        data : {
          x : 'date',
          columns : chartColumns,
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
          axis : 'x',
          start : regionStart,
          end : regionEnd
        } ]
      });
    }

  }
};
