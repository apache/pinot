function getAnomalies() {
  dashboardName = "Default_All_Metrics_Dashboard";
  baselineStart = moment(parseInt(hash.currentStart)).add(-7, 'days')
  baselineEnd = moment(parseInt(hash.currentEnd)).add(-7, 'days')
  aggTimeGranularity = "HOURS";
  
  
  var timeSeriesUrl = "/dashboard/data/tabular?" + window.location.hash.substring(1)  //
  + "&baselineStart=" + baselineStart + "&baselineEnd=" + baselineEnd   //
  + "&aggTimeGranularity=" + aggTimeGranularity ;
  
  var currentStartISO = moment(parseInt(hash.currentStart)).toISOString();
  var currentEndISO = moment(parseInt(hash.currentEnd)).toISOString();
  var anomaliesUrl = "/anomaly-results/collection/" + hash.dataset + "/" + currentStartISO + "/" + currentEndISO;
  
  getData(anomaliesUrl).done(function(anomalyData) {
    getData(timeSeriesUrl).done(function(timeSeriesData) {

        //Error handling when data is falsy (empty, undefined or null)
        if(!timeSeriesData){
            $("#"+  hash.view  +"-chart-area-error").empty()
            var warning = $('<div></div>', { class: 'uk-alert uk-alert-warning' })
            warning.append($('<p></p>', { html: 'Something went wrong. Please try and reload the page. Error: metric timeseries data =' + timeSeriesData  }))
            $("#"+  hash.view  +"-chart-area-error").append(warning)
            $("#"+  hash.view  +"-chart-area-error").show()
            return
        }else{
            $("#"+  hash.view  +"-chart-area-error").hide()
        }
      renderAnomalyLineChart(timeSeriesData, anomalyData);
      renderAnomalyTable(anomalyData);
    });
  });
};

function renderAnomalyLineChart(timeSeriesData, anomalyData) {
  $("#"+  hash.view  +"-display-chart-section").empty();

  /* Handelbars template for time series legend */
  var result_metric_time_series_section = HandleBarsTemplates.template_metric_time_series_section(timeSeriesData);
  $("#" + hash.view + "-display-chart-section").append(result_metric_time_series_section);

  drawAnomalyTimeSeries(timeSeriesData, anomalyData);
}

var lineChart;
function drawAnomalyTimeSeries(ajaxData, anomalyData) {

  var currentView = $("#" + hash.view + "-display-chart-section");
  var lineChartPlaceholder = $("#linechart-placeholder", currentView)[0];
  // Metric(s)
  var metrics = ajaxData["metrics"];
  var lineChartData = {};
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

  var colorArray;
  if (metrics.length < 10) {
    colorArray = d3.scale.category10().range();
  } else if (metrics.length < 20) {
    colorArray = d3.scale.category20().range();
  } else {
      colorArray = colorScale(metrics.length)
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

    colors[metrics[i] + "-baseline"] = colorArray[i];
    colors[metrics[i] + "-current"] = colorArray[i];

  }

  lineChart = c3.generate({
    bindto : lineChartPlaceholder,
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


  lineChart.hide();


    //EventListeners

    // Clicking the checkbox of the timeseries legend will redraw the timeseries
    // with the selected elements
    currentView.on("click",'.time-series-metric-checkbox', function() {

        var checkbox = this;
        var checkboxObj = $(checkbox);
        metricName = checkboxObj.val();
        if (checkboxObj.is(':checked')) {

            lineChart.show(metricName + "-current");
            lineChart.show(metricName + "-baseline");
        } else {

            lineChart.hide(metricName + "-current");
            lineChart.hide(metricName + "-baseline");
        }
    });

    //Select all / deselect all metrics option
    currentView.on("click",".time-series-metric-select-all-checkbox", function(){

        //if select all is checked
        if($(this).is(':checked')){
            //trigger click on each unchecked checkbox
            $(".time-series-metric-checkbox", currentView).each(function(index, checkbox) {
                if (!$(checkbox).is(':checked')) {
                    $(checkbox).click();
                }
            })
        }else{
            //trigger click on each checked checkbox
            $(".time-series-metric-checkbox", currentView).each(function(index, checkbox) {
                if ($(checkbox).is(':checked')) {
                    $(checkbox).click();
                }
            })
        }
    });

    //Preselect first metric
    $($(".time-series-metric-checkbox", currentView)[0]).click();
}

function renderAnomalyTable(data) {
    //Error handling when data is falsy (empty, undefined or null)
    if(!data){
        $("#"+  hash.view  +"-chart-area-error").empty()
        var warning = $('<div></div>', { class: 'uk-alert uk-alert-warning' })
        warning.append($('<p></p>', { html: 'Something went wrong. Please try and reload the page. Error: anomalies data =' + data  }))
        $("#"+  hash.view  +"-chart-area-error").append(warning)
        $("#"+  hash.view  +"-chart-area-error").show()
        return
    }

    /* Handelbars template for table */
    var result_anomalies_template = HandleBarsTemplates.template_anomalies(data);
    $("#" + hash.view + "-display-chart-section").append(result_anomalies_template);

}
