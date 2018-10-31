var anomaliesDisplayData = "";
var metricLineChart = "";
var metricChangeChart = ""

// This can be used on anomaly detail page
var timeSeriesDataForAllAnomalies = {};

function getFeedbackTypeString(feedbackType) {
    switch (feedbackType) {
        case 'ANOMALY':
            return "Confirmed Anomaly";
        case 'NOT_ANOMALY':
            return 'False Alarm';
        case 'ANOMALY_NEW_TREND':
            return 'Confirmed - New Trend';
        default:
            return feedbackType;
    }
}

function getFeedbackTypeFromString(feedbackTypeStr) {
    switch (feedbackTypeStr) {
        case 'Confirmed Anomaly':
            return "ANOMALY";
        case 'False Alarm':
            return 'NOT_ANOMALY';
        case 'Confirmed - New Trend':
            return 'ANOMALY_NEW_TREND';
        default:
            return feedbackTypeStr;
    }
}

function getAnomalies(tab) {

    //Get dataset anomaly metrics
    var url = "/dashboard/anomalies/metrics?dataset=" + hash.dataset;
    getData(url).done(function (anomalyMetricList) {

        //Define query params for timeseries and anomaly result query
        var compareMode = hash.fnCompareWeeks ?  hash.fnCompareWeeks : 1;
        var baselineStart = moment(parseInt(hash.currentStart)).subtract(compareMode, 'week');
        var baselineEnd = moment(parseInt(hash.currentEnd)).subtract(compareMode, 'week');
        var aggTimeGranularity = calcAggregateGranularity(hash.currentStart,hash.currentEnd);
        var dataset = hash.dataset;
        var currentStart = hash.currentStart;
        var currentEnd = hash.currentEnd;
        var metrics = hash.metrics;
        var timeSeriesUrl = "/dashboard/data/tabular?dataset=" + dataset //
            + "&currentStart=" + currentStart + "&currentEnd=" + currentEnd  //
            + "&baselineStart=" + baselineStart + "&baselineEnd=" + baselineEnd   //
            + "&aggTimeGranularity=" + aggTimeGranularity + "&metrics=" + metrics;

        var currentStartISO = moment(parseInt(currentStart)).toISOString();
        var currentEndISO = moment(parseInt(currentEnd)).toISOString();

        //If the dataset has anomaly function set up for the selected metric, query the anomaly results
        if(anomalyMetricList && anomalyMetricList.indexOf(metrics)> -1){

            var urlParams = "dataset=" + hash.dataset + "&startTimeIso=" + currentStartISO + "&endTimeIso=" + currentEndISO + "&metric=" + hash.metrics;
            urlParams += hash.filter ? "&filters=" + hash.filters : "";
            urlParams += hash.hasOwnProperty("anomalyFunctionId")  ?   "&id=" + hash.anomalyFunctionId : "";
            var anomaliesUrl = "/dashboard/anomalies/view?" + urlParams;

            //AJAX for anomaly result data
            getData(anomaliesUrl).done(function (anomalyData) {
               for (var i in anomalyData) {
                    if (anomalyData[i].feedback) {
                        if (anomalyData[i].feedback.feedbackType) {
                            anomalyData[i].feedback.feedbackType = getFeedbackTypeString(anomalyData[i].feedback.feedbackType);
                        }
                    }
                }
                anomaliesDisplayData = anomalyData;

                var metricConfigUrlParams = "dataset=" + hash.dataset + "&metric=" + hash.metrics;
                var metricUrl = "/thirdeye-admin/metric-config/view?" + metricConfigUrlParams;

                //AJAX for get metric config
                var extSourceLinkInfo;
                getData(metricUrl).done(function (metricConfig) {
                  extSourceLinkInfo = metricConfig.extSourceLinkInfo;
                  getTimeseriesData(anomaliesDisplayData, extSourceLinkInfo);
                });

            });


        } else {
            getTimeseriesData();
        }

        function getTimeseriesData(anomalyData, extSourceLinkInfo) {
          if(anomalyData) {
             var anomalyMetric = true;
          }
          $("#" + tab + "-display-chart-section").empty();
          if (anomalyMetric) {
            $(".anomaly-metric-tip").hide();
            renderAnomalyThumbnails(anomalyData, extSourceLinkInfo, tab);
          } else{
            tipToUser(tab)
          }
          delete hash.anomalyFunctionId;
          delete hash.fnCompareWeeks;

//            if(anomalyData){
//                var anomalyMetric = true;
//            }
//            var  anomalyData = anomalyData || [];
//            getData(timeSeriesUrl).done(function (timeSeriesData) {
//                //Error handling when data is falsy (empty, undefined or null)
//                if (!timeSeriesData) {
//                    $("#" + tab + "-chart-area-error").empty();
//                    var warning = $('<div></div>', { class: 'uk-alert uk-alert-warning' });
//                    warning.append($('<p></p>', { html: 'Something went wrong. Please try and reload the page. Error: metric timeseries data =' + timeSeriesData  }));
//                    $("#" + tab + "-chart-area-error").append(warning);
//                    $("#" + tab + "-chart-area-error").show();
//                    return
//                } else {
//                    $("#" + tab + "-chart-area-error").hide();
//                    $("#" + tab + "-display-chart-section").empty();
//                }
//                var placeholder = "#linechart-placeholder"
//                //renderTimeseriesArea(timeSeriesData, tab);
//                //drawMetricTimeSeries(timeSeriesData, anomalyData, tab, placeholder);
//
//                if (anomalyMetric) {
//                    $(".anomaly-metric-tip").hide();
//                    renderAnomalyThumbnails(anomalyData, tab);
//                }else{
//                    tipToUser(tab)
//                }
//
//                //anomalyFunctionId and hash.fnCompareWeeks are only present in hash when anomaly
//                // function run adhoc was requested on self service tab
//                //needs to be removed to be able to view other functions in later queries on the anomalies view
//                delete hash.anomalyFunctionId;
//                delete hash.fnCompareWeeks;
//            });
        };
    });
};

function renderTimeseriesArea(timeSeriesData, tab) {

    /* Handelbars template for time series legend */
    var result_metric_time_series_section = HandleBarsTemplates.template_metric_time_series_section(timeSeriesData);
    $("#" + tab + "-display-chart-section").append(result_metric_time_series_section);
}

function drawMetricTimeSeries(timeSeriesData, anomalyData, tab, placeholder) {

    var currentView = $("#" + tab + "-display-chart-section");

    //Unbind previous eventListeners
    currentView.off("click")
    currentView.off("change")

    var aggTimeGranularity = (window.datasetConfig.dataGranularity) ? window.datasetConfig.dataGranularity : "HOURS";
    var dateTimeFormat = "%I:%M %p";
    if (aggTimeGranularity == "DAYS" ) {
        dateTimeFormat = "%m-%d";
    }else if(timeSeriesData.summary.baselineEnd - timeSeriesData.summary.baselineStart > 86400000 ){
        dateTimeFormat = "%m-%d %I %p";
    }

    var lineChartPlaceholder = $(placeholder, currentView)[0];
    var barChartPlaceholder = $("#barchart-placeholder", currentView)[0];
    var metrics = timeSeriesData["metrics"];
    var lineChartData = {};
    var barChartData = {};
    var xTicksCurrent = [];
    var colors = {};
    var regions = [];

    for (var t = 0, len = timeSeriesData["timeBuckets"].length; t < len; t++) {
        var timeBucket = timeSeriesData["timeBuckets"][t]["currentStart"];
        var currentEnd = timeSeriesData["timeBuckets"][t]["currentEnd"];
        xTicksCurrent.push(timeBucket);
    }

    for (var i = 0; i < anomalyData.length; i++) {
        var anomaly = anomalyData[i];

        var anomalyStart = anomaly.startTime;
        var anomalyEnd = anomaly.endTime;
        var anomayID = "anomaly-id-" + anomaly.id;
        regions.push({'axis': 'x', 'start': anomalyStart, 'end': anomalyEnd, 'class': 'regionX ' + anomayID });
    }
    lineChartData["time"] = xTicksCurrent;
    barChartData["time"] = xTicksCurrent;

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
        var indexOfBaseline = timeSeriesData["data"][metrics[i]]["schema"]["columnsToIndexMapping"]["baselineValue"];
        var indexOfCurrent = timeSeriesData["data"][metrics[i]]["schema"]["columnsToIndexMapping"]["currentValue"];
        var indexOfRatio = timeSeriesData["data"][metrics[i]]["schema"]["columnsToIndexMapping"]["ratio"];

        for (var t = 0, tlen = timeSeriesData["timeBuckets"].length; t < tlen; t++) {

            var baselineValue = timeSeriesData["data"][metrics[i]]["responseData"][t][0];
            var currentValue = timeSeriesData["data"][metrics[i]]["responseData"][t][1];
            var deltaPercentage = parseInt(timeSeriesData["data"][metrics[i]]["responseData"][t][2]);
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

    metricLineChart = c3.generate({
        bindto: lineChartPlaceholder,
        padding: {
            top: 0,
            right: 100,
            bottom: 20,
            left: 100
        },
        data: {
            x: 'time',
            json: lineChartData,
            type: 'spline',
            colors: colors
        },
        zoom: {
            enabled: true,
            rescale: true
        },
        axis: {
            x: {
                type: 'timeseries',
                tick: {
                    count:11,
                    format: dateTimeFormat
                }
            },
            y: {
                tick: {
                    format: function (d) {
                        return getFormattedNumber(d);
                    }
                }
            }
        },
        //regions: regions,
        legend: {
            show: false
        },
        grid: {
            x: {
                show: false
            },
            y: {
                show: false
            }
        },
        point: {
            show: false
        }
    });


    var numAnomalies = anomalyData.length;
    var regionColors;

    if (anomalyData.length > 0 && anomalyData[0]["regionColor"]) {

        regionColors = [];
        regionColors.push( anomalyData[0]["regionColor"] )

    } else if(parseInt(numAnomalies) < 10) {
        regionColors = d3.scale.category10().range();

    } else if (numAnomalies < 20) {
        regionColors = d3.scale.category20().range();
    } else {
        regionColors = colorScale(numAnomalies);
    }

    //paint the anomaly regions based on anomaly id
    for (var i = 0; i < numAnomalies; i++) {
        var anomalyId = "anomaly-id-" + anomalyData[i]["id"];
        d3.select("." + anomalyId + " rect")
            .style("fill", regionColors[i])
    }

    var numAnomalies = anomalyData.length;
    var regionColors;

    if (anomalyData.length > 0 && anomalyData[0]["regionColor"]) {

        regionColors = [];
        regionColors.push( anomalyData[0]["regionColor"] )

    } else if(parseInt(numAnomalies) < 10) {
        regionColors = d3.scale.category10().range();

    } else if (numAnomalies < 20) {
        regionColors = d3.scale.category20().range();
    } else {
        regionColors = colorScale(numAnomalies);
    }


    //paint the anomaly regions based on anomaly id
    for (var i = 0; i < numAnomalies; i++) {
        var anomalyId = "anomaly-id-" + anomalyData[i]["id"];
        d3.select("." + anomalyId + " rect")
            .style("fill", regionColors[i])
    }

    metricChangeChart = c3.generate({

        bindto: barChartPlaceholder,
        padding: {
            top: 0,
            right: 100,
            bottom: 0,
            left: 100
        },
        data: {
            x: 'time',
            json: barChartData,
            type: 'area-spline',
            colors: colors
        },
        zoom: {
            enabled: false,
            rescale: true
        },
        axis: {
            x: {
                label: {
                    text: "Time"
                },
                type: 'timeseries',
                tick: {
                    count:11,
                    width:84,
                    multiline: true,
                    format: dateTimeFormat
                }
            },
            y: {
                label: {
                    text: "% change",
                    position: 'outer-middle'
                },
                tick: {
                    //count: 5,
                    format: function (d) {
                        return d.toFixed(2)
                    }
                }
            }
        },
        legend: {
            show: false
        },
        grid: {
            x: {

            },
            y: {
                lines: [
                    {
                        value: 0
                    }
                ]
            }
        },
        bar: {
            width: {
                ratio: .5
            }
        }
    });

    attach_MetricTimeSeries_EventListeners(currentView)

} //end of drawMetricTimeSeries

function calculateBucketSize(timeSeriesData) {
    if (timeSeriesData && timeSeriesData.summary) {
        var duration = timeSeriesData.summary.currentEnd - timeSeriesData.summary.currentStart;
        var bucketCount = timeSeriesData["currentValues"].length;
        return duration / bucketCount;
    } else {
        return 300000; // 5-MINUTES in millis
    }
}

function drawAnomalyTimeSeries(timeSeriesData, anomalyData, tab, placeholder, options) {
     var currentView = $("#anomalies");

     // Calculate the time format according to bucket size
     var bucketSize = calculateBucketSize(timeSeriesData);
     var dateTimeFormat = "%I:%M %p";
     if (bucketSize >= 86400000) {
         dateTimeFormat = "%m-%d";
     } else if (bucketSize >= 3600000) {
         dateTimeFormat = "%I %p";
     }
     // Append month and date if the time series is longer than a day; however, if a bucket is larger than a day,
     // then time format is already in month and date and hence we don't need to append anything.
     var needToAppendMonthDay = false;
     if (bucketSize < 86400000 && timeSeriesData.summary) {
         if (timeSeriesData.summary.currentEnd - timeSeriesData.summary.currentStart > 86400000) {
             needToAppendMonthDay = true;
         } else {
             var startDate = new Date(parseInt(timeSeriesData.summary.currentStart));
             var endDate = new Date(parseInt(timeSeriesData.summary.currentEnd));
             if (startDate.getDate() != endDate.getDate()) {
                 needToAppendMonthDay = true;
             }
         }
     }
     if (needToAppendMonthDay) {
         dateTimeFormat = "%m-%d " + dateTimeFormat;
     }

     var lineChartPlaceholder = $(placeholder, currentView)[0];
     var lineChartData = {};
     var xTicksCurrent = [];
     var colors = {};
     var regions = [];

     for (var t = 0, len = timeSeriesData["timeBuckets"].length; t < len; t++) {
         var timeBucket = timeSeriesData["timeBuckets"][t]["currentStart"];
         xTicksCurrent.push(timeBucket);
     }

     for (var i = 0; i < anomalyData.length; i++) {
         var anomaly = anomalyData[i];

         var anomalyStart = anomaly.startTime;
         var anomalyEnd = anomaly.endTime;
         var anomayID = "anomaly-id-" + anomaly.id;
         regions.push({'axis': 'x', 'start': anomalyStart, 'end': anomalyEnd, 'class': 'regionX ' + anomayID });
     }
     lineChartData["time"] = xTicksCurrent;

     var baselineData = [];
     var currentData = [];

     for (var t = 0, tlen = timeSeriesData["currentValues"].length; t < tlen; t++) {
         var baselineValue = timeSeriesData["baselineValues"][t];
         var currentValue = timeSeriesData["currentValues"][t];
         baselineData.push(baselineValue);
         currentData.push(currentValue);
     }
     lineChartData["baseline"] = baselineData;
     lineChartData["current"] = currentData;

     colors["baseline"] = '#1f77b4';
     colors["current"] = '#ff5f0e';

     var defaultSettings = {
         bindto: lineChartPlaceholder,
         padding: {
             top: 0,
             right: 100,
             bottom: 20,
             left: 100
         },
         data: {
             x: 'time',
             axes: {
                 baseline: 'y',
                 current: 'y'
             },
             json: lineChartData,
             type: 'spline',
             colors: colors

         },
         zoom: {
             enabled: false,
             rescale: true
         },
         axis: {
             min:{
                 y:0
             },

             x: {
                 type: 'timeseries',
                 tick: {
                     format: dateTimeFormat,
                     count:5
                 }
             },
             y: {
                 show: true,
                 tick: {
                     format: function (d) {
                         return getFormattedNumber(d);
                     }
                 },
                 min: 0
             }
         },
         regions: regions,
         legend: {
             show: false
         },
         grid: {
             x: {
                 show: false
             },
             y: {
                 show: true
             }
         },
         point: {
             show: false
         }
     };

    //Merge then recursively
    var settings = $.extend(true, defaultSettings, options);

    var anomalyThumbnailLineChart = c3.generate(settings);


    $(".c3-axis path.domain, .c3-axis-y line, .c3-axis-x line", lineChartPlaceholder).hide();
    $(".c3-axis-x path.domain", lineChartPlaceholder).hide();

     var numAnomalies = anomalyData.length;
     var regionColors = ['ff0000'];//'ff7f0e'

     //paint the anomaly regions based on anomaly id
     for (var i = 0; i < numAnomalies; i++) {
         var anomalyId = "anomaly-id-" + anomalyData[i]["id"];
         d3.select("." + anomalyId + " rect")
             .style("fill", regionColors[i])
     }

 } //end of drawAnomalyTimeSeries

function renderAnomalyThumbnails(data, extSourceLinkInfo, tab) {
    //Error handling when data is falsy (empty, undefined or null)
    if (!data) {
        $("#" + tab + "-chart-area-error").empty()
        var warning = $('<div></div>', { class: 'uk-alert uk-alert-warning' })
        warning.append($('<p></p>', { html: 'Something went wrong. Please try and reload the page. Error: anomalies data =' + data  }))
        $("#" + tab + "-chart-area-error").append(warning)
        $("#" + tab + "-chart-area-error").show()
        return
    }

    /* Handelbars template for table */
    var result_anomalies_template = HandleBarsTemplates.template_anomalies(data);
    $("#" + tab + "-display-chart-section").append(result_anomalies_template);

    attach_AnomalyTable_EventListeners();

    for (var i = 0, numAnomalies = data.length; i < numAnomalies; i++) {
        requestLineChart(i);
        applyExternalInfo(i);
    }

    function applyExternalInfo(i) {
        var startTime = data[i]["startTime"];
        var endTime = data[i]["endTime"];

        var links = "";
        for (var source in extSourceLinkInfo) {
            var template = extSourceLinkInfo[source];
            if (template) {
                template = template.replace("${startTime}", startTime);
                template = template.replace("${endTime}", endTime);
                source = source.charAt(0) + source.slice(1).toLowerCase();
                links = links + source.link(template) + " ";
            }
        }
        if (extSourceLinkInfo) {
            $("#external-props-" + i).html(links);
        }
    }

    function requestLineChart(i) {
        var anomalyStartTime = data[i]["startTime"];
        var anomalyEndTime = data[i]["endTime"];

        var aggTimeGranularity = calcAggregateGranularity(anomalyStartTime, anomalyEndTime);

        var extensionWindowMillis;
        switch (aggTimeGranularity) {
            case "DAYS":
                extensionWindowMillis = 86400000;
                break;
            case "HOURS":
            default:
                var viewWindowSize = anomalyEndTime - anomalyStartTime;
                var multiplier = Math.max(2, parseInt(viewWindowSize / (2 * 3600000)));
                extensionWindowMillis = 3600000 * multiplier;
        }


        var viewWindowStart = anomalyStartTime - extensionWindowMillis;
        var viewWindowEnd = anomalyEndTime + extensionWindowMillis;

        var anomalyId = data[i]["id"];
        var placeholder = "#d3charts-" + i;
        var timeSeriesUrl = "/dashboard/anomaly-merged-result/timeseries/" + anomalyId
            + "?aggTimeGranularity=" + aggTimeGranularity + "&start=" + viewWindowStart + "&end="
            + viewWindowEnd;
        var tab = hash.view;

        getDataCustomCallback(timeSeriesUrl, tab).done(function (timeSeriesData) {
            var maxBucketTime = 0;
            var anomalyRegionData = [];
            anomalyRegionData.push({
                startTime: parseInt(anomalyStartTime),
                endTime: parseInt(anomalyEndTime),
                id: anomalyId,
                regionColor: '#eedddd'
            });
            drawAnomalyTimeSeries(timeSeriesData, anomalyRegionData, tab, placeholder)

            // Caching it so that this can be reused on the details page
            timeSeriesDataForAllAnomalies[data[i]["id"]] = timeSeriesData;
        })
    }
}

function attach_MetricTimeSeries_EventListeners(currentView){

     //Unbind previously attached eventlisteners
     currentView.off("click");
     currentView.off("change");

     // Clicking the checkbox of the timeseries legend will redraw the timeseries
     // with the selected elements
     currentView.on("click", '.time-series-metric-checkbox', function () {
         anomalyTimeSeriesCheckbox(this);
     });

     //Select all / deselect all metrics option
     currentView.on("click", ".time-series-metric-select-all-checkbox", function () {
         anomalyTimeSelectAllCheckbox(this);
     });

 //Select all / deselect all metrics option
     currentView.on("click", "#self-service-link", function () {
        $(".header-tab[rel='self-service']").click();
     });

     function anomalyTimeSeriesCheckbox(target) {
         var checkbox = target;
         var checkboxObj = $(checkbox);
         metricName = checkboxObj.val();
         if (checkboxObj.is(':checked')) {
             //Show metric's lines on timeseries
             metricLineChart.show(metricName + "-current");
             metricLineChart.show(metricName + "-baseline");
             metricChangeChart.show(metricName + "-delta");

         } else {
             //Hide metric's lines on timeseries
             metricLineChart.hide(metricName + "-current");
             metricLineChart.hide(metricName + "-baseline");
             metricChangeChart.hide(metricName + "-delta");
         }
     }

     function anomalyTimeSelectAllCheckbox(target) {
         //if select all is checked
         if ($(target).is(':checked')) {
             //trigger click on each unchecked checkbox
             $(".time-series-metric-checkbox", currentView).each(function (index, checkbox) {
                 if (!$(checkbox).is(':checked')) {
                     $(checkbox).click();
                 }
             })
         } else {
             //trigger click on each checked checkbox
             $(".time-series-metric-checkbox", currentView).each(function (index, checkbox) {
                 if ($(checkbox).is(':checked')) {
                     $(checkbox).click();
                 }
             })
         }
     }

     //Show the first line on the timeseries
     var firstLegendLabel = $($(".time-series-metric-checkbox")[0])
     if( !firstLegendLabel.is(':checked')) {
         firstLegendLabel.click();
     }
 }

function attach_AnomalyTable_EventListeners(){

     //Unbind previously attached eventlisteners
     $("#anomalies-table").off("click");
     $("#anomalies-table").off("hide.uk.dropdown");
     $("#anomaly-result-thumbnails").off("click");
     $("#anomaly-result-thumbnails").off("hide.uk.dropdown");

    //Clicking a checkbox in the table takes user to related heatmap chart
    $("#anomalies-section").on("click", ".anomaly-details-link", function () {
        var mergedAnomalyId = $(this).attr("data-id")
        showAnomalyDetails(mergedAnomalyId)
    });

    //Clicking a checkbox in the table takes user to related heatmap chart
    $("#anomaly-result-thumbnails").on("click", ".heatmap-link", function () {
        showHeatMapOfAnomaly(this);
    });

    //Clicking the feedback option will trigger the ajax - post
    $("#anomaly-result-thumbnails").on("click", ".feedback-list", function () {
        $(this).next("textarea").show();
    });


    $('.feedback-dropdown[data-uk-dropdown]').on('hide.uk.dropdown', function(){
        submitAnomalyFeedback(this);
    });

     /** Switches the view to compare view/heat-map **/
     function showHeatMapOfAnomaly(target) {
         var $target = $(target);
         hash.view = "compare";
         hash.aggTimeGranularity = "aggregateAll";

         var currentStartUTC = $target.attr("data-start-utc-millis");
         var currentEndUTC = $target.attr("data-end-utc-millis");

         //Using WoW for anomaly baseline
         var baselineStartUTC = moment(parseInt(currentStartUTC)).add(-7, 'days').valueOf();
         var baselineEndUTC = moment(parseInt(currentEndUTC)).add(-7, 'days').valueOf();

         hash.baselineStart = baselineStartUTC;
         hash.baselineEnd = baselineEndUTC;
         hash.currentStart = currentStartUTC;
         hash.currentEnd = currentEndUTC;
         delete hash.dashboard;
         metrics = [];
         var metricName = $target.attr("data-metric");
         metrics.push(metricName);
         hash.metrics = metrics.toString();

         //update hash will trigger window.onhashchange event:
         // update the form area and trigger the ajax call
         window.location.hash = encodeHashParameters(hash);
     }

     function submitAnomalyFeedback(target) {
         var $target = $(target);
         var selector = $(".selected-feedback", $target);
         var feedbackType = getFeedbackTypeFromString(selector.attr("value"));
         var anomalyId = selector.attr("data-anomaly-id");
         var comment = $(".feedback-comment", $target).val();

         //Remove control characters
         comment = comment.replace(/[\x00-\x1F\x7F-\x9F]/g, "")
         if(feedbackType){

             var data = '{ "feedbackType": "' + feedbackType + '","comment": "'+ comment +'"}';
             var url = "/dashboard/anomaly-merged-result/feedback/" + anomalyId;

             //post anomaly feedback
             submitData(url, data).done(function () {
                 $(selector).addClass("green-background");
             }).fail(function(){
                 $(selector).addClass("red-background");
             })
         }
     }

    //Set initial state of view
    $(".box-inner .change").each( function(){
        var change = $(this).text();
        change = parseFloat(change.trim())
        if( change < 0){
            $(this).addClass("negative-icon");

        }else if( change > 0){
            $(this).addClass("positive-icon");
        }
    });

 }

function tipToUser(tab) {
    var tipToUser = document.createElement("div");
    tipToUser.id = "anomaly-metric-tip";
    tipToUser.className = "tip-to-user uk-alert uk-form-row";
    tipToUser.setAttribute("data-tip-source", "not-anomaly-metric");
    var closeIcon = document.createElement("i");
    closeIcon.className = "close-parent uk-icon-close";
    var msg = document.createElement("p");
    msg.innerText = "There is no anomaly monitoring function set up for this metric. You can create a function on the"
    var link = document.createElement("a");
    link.id = "self-service-link";
    link.innerText = "Self Service tab.";

    msg.appendChild(link);
    tipToUser.appendChild(closeIcon);
    tipToUser.appendChild(msg);
    $("#" + tab + "-display-chart-section").append(tipToUser)
}

//format integers with comma-grouping for thousands and round to 2 decimal numbers for floats
function getFormattedNumber(number) {
    return getFormattedNumber(number, 2);
}

//format integers with comma-grouping for thousands and round to "digits" decimal numbers for floats
function getFormattedNumber(number, digits) {
    if (number % 1 == 0) {
        return d3.format(",")(number);
    } else {
        var formatString = ",." + digits + "f";
        return d3.format(formatString)(number);
    }
}

