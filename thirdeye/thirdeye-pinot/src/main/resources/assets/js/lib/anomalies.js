function getAnomalies() {
  dashboardName = "Default_All_Metrics_Dashboard";
  baselineStart = moment(parseInt(hash.currentStart)).add(-7, 'days')
  baselineEnd = moment(parseInt(hash.currentEnd)).add(-7, 'days')
  aggTimeGranularity = "HOURS";
  
  
  var timeSeriesUrl = "/dashboard/data/tabular?" + window.location.hash.substring(1)  //
  + "&baselineStart=" + baselineStart + "&baselineEnd=" + baselineEnd   //
  + "&aggTimeGranularity=" + aggTimeGranularity ;
  
  
  var anomaliesUrl = "/anomaly-results/collection/" + hash.dataset;
  
  getData(anomaliesUrl).done(function(anomalyData) {
    getData(timeSeriesUrl).done(function(timeSeriesData) {
      renderAnomalyTabular(timeSeriesData, anomalyData);
      renderAnomalies(anomalyData);
    });
  });
};

function renderAnomalies(data) {

  console.log("anomalies data")
  console.log(data)
  $("#"+  hash.view  +"-display-table-section").empty();

  /* Handelbars template for funnel table */
  var result_anomalies_template = HandleBarsTemplates.template_anomalies(data);
  $("#" + hash.view + "-display-table-section").append(result_anomalies_template);

}

function renderAnomalyTabular(timeSeriesData, anomalyData) {
  $("#"+  hash.view  +"-display-chart-section").empty();

  /* Handelbars template for time series legend */
  var result_metric_time_series_section = HandleBarsTemplates.template_metric_time_series_section_anomaly(timeSeriesData);
  $("#" + hash.view + "-display-chart-section").append(result_metric_time_series_section);

  drawAnomalyTimeSeries(timeSeriesData, anomalyData);
}

var lineChart;
function drawAnomalyTimeSeries(ajaxData, anomalyData) {

  // Metric(s)
  var metrics = ajaxData["metrics"]
  var lineChartData = {};
  var barChartData = {};
  var dateTimeformat = (hash.hasOwnProperty("aggTimeGranularity") && hash.aggTimeGranularity.toLowerCase().indexOf("days") > -1) ? "MM-DD" : "h a";
  var xTickFormat = dateTimeformat;
  var xTicksBaseline = [];
  var xTicksCurrent = [];
  var colors = {};
  var chartTypes = {};
  var axes = {};
  var regions = [];
  for (var t = 0, len = ajaxData["timeBuckets"].length; t < len; t++) {
    var timeBucket = ajaxData["timeBuckets"][t]["currentStart"]
    var currentEnd = ajaxData["timeBuckets"][t]["currentEnd"]
    xTicksBaseline.push(timeBucket)
    xTicksCurrent.push(timeBucket)
  }
  for(var i=0;i<anomalyData.length;i++){
    anomaly = anomalyData[i];
    anomalyStart = anomaly.startTimeUtc
    anomalyEnd = anomaly.endTimeUtc
    regions.push({axis: 'x', start: anomalyStart, end: anomalyEnd, class: 'regionX'})
  }
  lineChartData["time"] = xTicksCurrent;
  barChartData["time"] = xTicksCurrent;
  var colorArray;
  if (metrics.length < 10) {
    colorArray = d3.scale.category10().range();
  } else if (metrics.length < 20) {
    colorArray = d3.scale.category20().range();
  } else {
    colorArray = [];
    for (i = 0, len = metrics.length; i < len; i++) {
      colorArray.push(colorByName(metrics[i]));
    }
  }

  for (var i = 0, mlen = metrics.length; i < mlen; i++) {
    var metricBaselineData = [];
    var metricCurrentData = [];
    var deltaPercentageData = [];
    for (var t = 0, len = ajaxData["timeBuckets"].length; t < len; t++) {
      var baselineValue = ajaxData["data"][metrics[i]]["responseData"][t][0];
      var currentValue = ajaxData["data"][metrics[i]]["responseData"][t][1];
      var deltaPercentage = parseInt(ajaxData["data"][metrics[i]]["responseData"][t][2] * 100);
      metricBaselineData.push(baselineValue);
      metricCurrentData.push(currentValue);
      deltaPercentageData.push(deltaPercentage);
    }
    lineChartData[metrics[i] + "-baseline"] = metricBaselineData;
    lineChartData[metrics[i] + "-current"] = metricCurrentData;
    barChartData[metrics[i] + "-delta"] = deltaPercentageData;
    colors[metrics[i] + "-baseline"] = colorArray[i];
    colors[metrics[i] + "-current"] = colorArray[i];
    colors[metrics[i] + "-delta"] = colorArray[i];
  }

  lineChart = c3.generate({
    bindto : '#anomaly-linechart-placeholder',
    padding : {
      top : 0,
      right : 100,
      bottom : 0,
      left : 100
    },
    data : {
      x : 'time',
      json : lineChartData,
      type : 'spline',
      colors : colors
    },
    axis : {
      x : {
        type : 'timeseries'
      }
    },
    regions: regions,
    legend : {
      show : false
    },
    grid : {
      x : {
        show : false
      },
      y : {
        show : false
      }
    }, 
    point: {
      show: false
    }
  });
  var barChart = c3.generate({

    bindto : '#anomaly-barchart-placeholder',
    padding : {
      top : 0,
      right : 100,
      bottom : 0,
      left : 100
    },
    data : {
      x : 'time',
      json : barChartData,
      type : 'spline',
      colors : colors,
    },
    axis : {
      x : {
        label : {
          text : "Time"
        },
        type : 'timeseries'
      },
      y : {
        label : {
          text : "% change",
          position : 'outer-middle'
        }
      }
    },
    legend : {
      show : false
    },
    grid : {
      x : {

      },
      y : {
        lines : [ {
          value : 0
        } ]
      }
    },
    bar : {
      width : {
        ratio : .5
      }
    }
  });

  ids = [];
  for (var i = 0; i < metrics.length; i++) {
    ids.push(i);
  }

  lineChart.hide();
  barChart.hide();
  // Clicking the checkbox of the timeseries legend will redraw the timeseries
  // with the selected elements
  $("#anomalies-time-series-legend").on("click", '.anomalies-time-series-checkbox', function() {
    var checkbox = this;
    var checkboxObj = $(checkbox);
    metricName = checkboxObj.val();
    if (checkboxObj.is(':checked')) {

      lineChart.show(metricName + "-current");
      lineChart.show(metricName + "-baseline");
      barChart.show(metricName + "-delta");
    } else {
      lineChart.hide(metricName + "-current");
      lineChart.hide(metricName + "-baseline");
      barChart.hide(metricName + "-delta");
    }
  });

  // Select all / deselect all metrics option
  $("#main-view").on("change", ".anomalies-time-series-select-all-checkbox", function() {

    // if select all is checked
    if ($(this).is(':checked')) {
      // trigger click on each unchecked checkbox
      $(".anomalies-time-series-checkbox").each(function(index, checkbox) {
        if (!$(checkbox).is(':checked')) {
          $(checkbox).click();
        }
      })
    } else {
      // trigger click on each checked checkbox
      $(".anomalies-time-series-checkbox").each(function(index, checkbox) {
        if ($(checkbox).is(':checked')) {
          $(checkbox).click();
        }
      })
    }
  });

  // Preselect first metric
  $($(".anomalies-time-series-checkbox")[0]).click();
}
