function getAnomalies(tab) {

  dashboardName = "Default_Dashboard"; //Change it to be the first option:  $(".dashboard-option:first-child a").html().trim()
  baselineStart = moment(parseInt(hash.currentStart)).add(-7, 'days')
  baselineEnd = moment(parseInt(hash.currentEnd)).add(-7, 'days')
  aggTimeGranularity = (window.datasetConfig.dataGranularity) ? window.datasetConfig.dataGranularity : "HOURS";


  var timeSeriesUrl = "/dashboard/data/tabular?" + window.location.hash.substring(1)  //
  + "&baselineStart=" + baselineStart + "&baselineEnd=" + baselineEnd   //
  + "&aggTimeGranularity=" + aggTimeGranularity;

  var currentStartISO = moment(parseInt(hash.currentStart)).toISOString();
  var currentEndISO = moment(parseInt(hash.currentEnd)).toISOString();
  var anomaliesUrl = "/dashboard/anomalies/view?dataset=" + hash.dataset + "&startTimeIso=" + currentStartISO + "&endTimeIso=" + currentEndISO + "&metric=" + hash.metrics + "&filters=" + hash.filters;

  getData(anomaliesUrl).done(function(anomalyData) {

    getData(timeSeriesUrl).done(function(timeSeriesData) {

        //Error handling when data is falsy (empty, undefined or null)
        if(!timeSeriesData){
            $("#"+  tab  +"-chart-area-error").empty()
            var warning = $('<div></div>', { class: 'uk-alert uk-alert-warning' })
            warning.append($('<p></p>', { html: 'Something went wrong. Please try and reload the page. Error: metric timeseries data =' + timeSeriesData  }))
            $("#"+  tab  +"-chart-area-error").append(warning)
            $("#"+  tab  +"-chart-area-error").show()
            return
        }else{
            $("#"+  tab  +"-chart-area-error").hide()
        }
      renderAnomalyLineChart(timeSeriesData, anomalyData, tab);
      renderAnomalyTable(anomalyData, tab);
    });
  });
};

function renderAnomalyLineChart(timeSeriesData, anomalyData, tab) {
  $("#"+  tab  +"-display-chart-section").empty();

  /* Handelbars template for time series legend */
  var result_metric_time_series_section = HandleBarsTemplates.template_metric_time_series_section(timeSeriesData);
  $("#" + tab + "-display-chart-section").append(result_metric_time_series_section);

  drawAnomalyTimeSeries(timeSeriesData, anomalyData, tab);
}

var lineChart;
function drawAnomalyTimeSeries(timeSeriesData, anomalyData, tab) {

  var currentView = $("#" + tab + "-display-chart-section");
  var dateTimeFormat = "%I:%M %p";
  if(hash.hasOwnProperty("aggTimeGranularity") && hash.aggTimeGranularity == "DAYS"){
        dateTimeFormat = "%m-%d"
  }

  var lineChartPlaceholder = $("#linechart-placeholder", currentView)[0];
  // Metric(s)
  var metrics = timeSeriesData["metrics"];
  var lineChartData = {};
  var dateTimeformat = (hash.hasOwnProperty("aggTimeGranularity") && hash.aggTimeGranularity.toLowerCase().indexOf("days") > -1) ? "MM-DD" : "h a";
  var xTickFormat = dateTimeformat;
  var xTicksBaseline = [];
  var xTicksCurrent = [];
  var colors = {};
  var chartTypes = {};
  var axes = {};
  var regions = [];

  for (var t = 0, len = timeSeriesData["timeBuckets"].length; t < len; t++) {
    var timeBucket = timeSeriesData["timeBuckets"][t]["currentStart"]
    var currentEnd = timeSeriesData["timeBuckets"][t]["currentEnd"]
    xTicksBaseline.push(timeBucket)
    xTicksCurrent.push(timeBucket)
  }
  for(var i=0;i<anomalyData.length;i++){
    var anomaly = anomalyData[i];

    var anomalyStart = anomaly.startTimeUtc
    var anomalyEnd = anomaly.endTimeUtc
    var anomayID = "anomaly-id-" + anomaly.id;
    regions.push({'axis': 'x', 'start': anomalyStart, 'end': anomalyEnd, 'class': 'regionX ' + anomayID, 'id': 'testing-id' })
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
    for (var t = 0, len = timeSeriesData["timeBuckets"].length; t < len; t++) {
      var baselineValue = timeSeriesData["data"][metrics[i]]["responseData"][t][0];
      var currentValue = timeSeriesData["data"][metrics[i]]["responseData"][t][1];
      var deltaPercentage = parseInt(timeSeriesData["data"][metrics[i]]["responseData"][t][2] * 100);
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
        type : 'timeseries',
        tick: {
            format: dateTimeFormat
        }
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
    var numAnomalies = anomalyData.length
    var regionColors;
    if(parseInt(numAnomalies) < 10){
        regionColors = d3.scale.category10().range();

    } else if (numAnomalies < 20) {
        regionColors = d3.scale.category20().range();
    }else{
        regionColors = colorScale(numAnomalies);
    }


    //paint the anomaly regions based on anomaly id
    for(var i= 0;i< numAnomalies;i++) {
        var anomalyId = "anomaly-id-" + anomalyData[i]["id"];
        d3.select("." + anomalyId +" rect")
        .style("fill", regionColors[i])
    }


    //EventListeners

    // Clicking the checkbox of the timeseries legend will redraw the timeseries
    // with the selected elements
    currentView.on("click",'.time-series-metric-checkbox', function() {

        var checkbox = this;
        var checkboxObj = $(checkbox);
        metricName = checkboxObj.val();
        if (checkboxObj.is(':checked')) {
            //Show metric's lines on timeseries
            lineChart.show(metricName + "-current");
            lineChart.show(metricName + "-baseline");

            //show related ranges on timeserie and related rows in the tabular display
            $(".anomaly-table-checkbox input").each(function(){

                if($(this).attr("data-value") ==  metricName){
                    var tableRow = $(this).closest("tr");
                    tableRow.show()
                    //check the related input boxes
                    $("input", tableRow).attr('checked', 'checked');
                    $("input", tableRow).prop('checked', true);
                    //show the related timeranges
                    var anomalyId = "anomaly-id-" + $(this).attr("id");
                    $("." + anomalyId).show();
                }
            })

        } else {
            //Hide metric's lines on timeseries
            lineChart.hide(metricName + "-current");
            lineChart.hide(metricName + "-baseline");

            //hide related ranges on timeserie and related rows in the tabular display
            $(".anomaly-table-checkbox input").each(function(){

                if($(this).attr("data-value") ==  metricName){
                    $(this).closest("tr").hide();
                    var anomalyId = "anomaly-id-" + $(this).attr("id");
                    $("." + anomalyId).hide();
                }
            })


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


    //licking a checkbox in the table toggles the region of that timerange on the timeseries chart
    currentView.on("change",".anomaly-table-checkbox input", function(){

        var anomalyId = ".anomaly-id-" + $(this).attr("id");
        if ($(this).is(':checked')){
            $(anomalyId).show();
        }else{
            $(anomalyId).hide();
        }


    });

    //Preselect first metric
    $($(".time-series-metric-checkbox", currentView)[0]).click();
}

function renderAnomalyTable(data, tab) {
    //Error handling when data is falsy (empty, undefined or null)
    if(!data){
        $("#"+  tab +"-chart-area-error").empty()
        var warning = $('<div></div>', { class: 'uk-alert uk-alert-warning' })
        warning.append($('<p></p>', { html: 'Something went wrong. Please try and reload the page. Error: anomalies data =' + data  }))
        $("#"+  tab +"-chart-area-error").append(warning)
        $("#"+  tab  +"-chart-area-error").show()
        return
    }

    /* Handelbars template for table */
    var result_anomalies_template = HandleBarsTemplates.template_anomalies(data);
    $("#" + tab + "-display-chart-section").append(result_anomalies_template);

    $("#anomalies-table").on("click", ".select-all-checkbox", function() {


        var currentTable = $(this).closest("table");

        if ($(this).is(':checked')) {
            $("input[type='checkbox']", currentTable).attr('checked', 'checked');
            $("input[type='checkbox']", currentTable).prop('checked', true);
            $("input[type='checkbox']", currentTable).change();

        } else {
            $("input[type='checkbox']", currentTable).removeAttr('checked');
            $("input[type='checkbox']", currentTable).prop('checked', false);
            $("input[type='checkbox']", currentTable).change();

        }
    })

}
