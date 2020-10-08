function getTabular(tab) {

    var url = "/dashboard/data/tabular?" + window.location.hash.substring(1);
    getData(url, tab).done(function (data) {
        renderTabular(data, tab);
    });
};

function renderTabular(data, tab) {
    //Error handling when data is falsy (empty, undefined or null)
    if (!data) {
        $("#" + tab + "-chart-area-error").empty()
        var warning = $('<div></div>', { class: 'uk-alert uk-alert-warning' })
        warning.append($('<p></p>', { html: 'Something went wrong. Please try and reload the page. Error: data =' + data  }))
        $("#" + tab + "-chart-area-error").append(warning)
        $("#" + tab + "-chart-area-error").show()
        return
    } else {
        $("#" + tab + "-chart-area-error").hide()
    }

    if (data.metrics.length == 0) {
        $("#" + tab + "-chart-area-error").empty()
        var warning = $('<div></div>', { class: 'uk-alert uk-alert-warning' })
        warning.append($('<p></p>', { html: 'No metric data is present. Error: data.metrics.length = 0'  }))
        $("#" + tab + "-chart-area-error").append(warning)
        $("#" + tab + "-chart-area-error").show()
        return
    }

    /* Handelbars template for time series legend */
    var result_metric_time_series_section =
        HandleBarsTemplates.template_metric_time_series_section(data);
    $("#" + tab + "-display-chart-section").append(result_metric_time_series_section);

    drawTimeSeries(data, tab)


    /* Handelbars template for funnel table */
    var result_funnels_template = HandleBarsTemplates.template_funnels_table(data);
    $("#" + tab + "-display-chart-section").append(result_funnels_template);
    calcHeatMapBG();
    //formatMillisToTZ();

    /* Build the dimension dropdown*/
    var dimensionListHtml = "";
    var dimensionList = window.datasetConfig.datasetDimensions;

    for (var index = 0, numDimensions = dimensionList.length; index < numDimensions; index++) {
        dimensionListHtml += "<li class='funnels-dimension-option' rel='dimensions' value='" + dimensionList[index] + "'><a href='#' class='uk-dropdown-close'>" + dimensionList[index] + "</a></li>";

    }

    $(".funnels-table-dimension-list").append(dimensionListHtml);

    $(".funnels-dimension-selector-btn").click(function (event) {

        var metric = $(this).attr("data-metric")

        var topPosition = event.pageY - 65;
        var leftPosition = 15;

        var dimensionDropdown = $(".funnels-table-dimension-box:last-of-type");
        dimensionDropdown.attr("data-metric", metric);
        dimensionDropdown.css("position", "absolute");
        dimensionDropdown.css("top", topPosition + "px");
        dimensionDropdown.css("left", leftPosition + "%");
        dimensionDropdown.css("z-index", "3");
        dimensionDropdown.show()

    })

    // Click metric name in the table
    $("#main-view").on("click", ".funnels-dimension-option", function () {
        switchToContributors(this)
    });
}
var lineChart;
function drawTimeSeries(timeSeriesData, tab) {

    var currentView = $("#" + tab + "-display-chart-section")
    var lineChartPlaceholder = $("#linechart-placeholder", currentView)[0];

    var barChartPlaceholder = $("#barchart-placeholder", currentView)[0];
    var dateTimeFormat = "%I:%M %p";
    if (hash.hasOwnProperty("aggTimeGranularity") && hash.aggTimeGranularity == "DAYS") {
        dateTimeFormat = "%m-%d"
    }else if(timeSeriesData.summary.baselineEnd - timeSeriesData.summary.baselineStart > 86400000){
        dateTimeFormat = "%m-%d %I %p"
    }


    // Metric(s)
    var metrics = timeSeriesData["metrics"]
    var lineChartData = {};
    var barChartData = {};
    var xTicksBaseline = [];
    var xTicksCurrent = [];
    var colors = {};
    var chartTypes = {};
    var axes = {};
    for (var t = 0, len = timeSeriesData["timeBuckets"].length; t < len; t++) {
        var timeBucket = timeSeriesData["timeBuckets"][t]["currentStart"]
        xTicksBaseline.push(timeBucket)
        xTicksCurrent.push(timeBucket)
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
            colorArray = colorScale(metrics.length)
        }
    }

    for (var i = 0, mlen = metrics.length; i < mlen; i++) {
        var metricBaselineData = [];
        var metricCurrentData = [];
        var deltaPercentageData = [];
        for (var t = 0, len = timeSeriesData["timeBuckets"].length; t < len; t++) {
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

    lineChart = c3.generate({
        bindto: lineChartPlaceholder,
        padding: {
            top: 0,
            right: 100,
            bottom: 0,
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
            rescale:true
        },
        axis: {
            x: {
                type: 'timeseries',
                tick: {
                    format: dateTimeFormat
                }
            },
            y: {
                tick: {
                    //format integers with comma-grouping for thousands
                    format: d3.format(",.1 ")
                }
            }
        },
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
        }
    });
    var barChart = c3.generate({

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
//Todo: the area should be red or blue based on the delta value being positive or negative, the border line color should be the color of the color scale element
//            color: function (color, d) {
//                var color = d.value > 0 ? "#00f" :  "#f00"
//                return color;
//            },
            //type : 'spline',
            colors: colors
        },
        zoom: {
            enabled: true,
            rescale:true
        },
        axis: {
            x: {
                label: {
                    text: "Time"
                },
                type: 'timeseries',
                tick: {
                    format: dateTimeFormat
                }
            },
            y: {
                label: {
                    text: "% change",
                    position: 'outer-middle'
                },
                tick: {
                    count: 5,
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

    ids = [];
    for (var i = 0; i < metrics.length; i++) {
        ids.push(i);
    }


    lineChart.hide();
    barChart.hide();

    //EventListeners
    var currentView = $("#" + tab + "-display-chart-section")

    // Clicking the checkbox of the timeseries legend will redraw the timeseries
    // with the selected elements
    $("#metric-time-series-legend", currentView).on("click", '.time-series-metric-checkbox', function () {
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

    //Select all / deselect all metrics option
    currentView.on("click", ".time-series-metric-select-all-checkbox", function () {

        //if select all is checked
        if ($(this).is(':checked')) {
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
    });

    //Preselect first metric
    $($(".time-series-metric-checkbox", currentView)[0]).click();
}


function switchToContributors(target) {
    // Change the view to contributors

    var dimensionContainer = $(target).closest(".funnels-table-dimension-box");
    var metric = $(dimensionContainer).attr("data-metric");
    var dimension = $(target).attr("value");

    //either dashboard or metrics param is present in hash
    delete hash.dashboard;

    //switch to time tab
    hash.view = "compare";

    //set start and end date to the starte and end date of the table
    var timeBuckets = $("#timebuckets>span")
    var numTimeBuckets = timeBuckets.length;

    var firstTimeBucketInRow = $("#timebuckets>span")[0];
    var lastTimeBucketInRow = $("#timebuckets>span")[numTimeBuckets - 1];

    var currentStartUTC = $($("span", firstTimeBucketInRow)[0]).text().trim();
    var baselineStartUTC = $($("span", firstTimeBucketInRow)[2]).text().trim();

    var currentEndUTC = $($("span", lastTimeBucketInRow)[1]).text().trim();
    var baselineEndUTC = $($("span", lastTimeBucketInRow)[3]).text().trim();

    hash.baselineStart = baselineStartUTC;
    hash.baselineEnd = baselineEndUTC;
    hash.currentStart = currentStartUTC;
    hash.currentEnd = currentEndUTC;

    //check the current granularity of the data on the table
    var endOfFirstTimeBucket = $($("span", firstTimeBucketInRow)[1]).text().trim();
    var diff = parseInt(endOfFirstTimeBucket) - parseInt(currentStartUTC)
    var diffProperties = describeMillis(diff)
    var aggTimeGranularity = diffProperties ? diffProperties.unit : "HOURS"

    hash.aggTimeGranularity = aggTimeGranularity

    //set the metrics
    metrics = [];
    metrics.push(metric)
    hash.metrics = metrics.toString();

    //select only the first dimension to retrieve less data
    hash.dimensions = dimension;

    //update hash will trigger window.onhashchange event:
    // update the form area and trigger the ajax call
    window.location.hash = encodeHashParameters(hash);
}
