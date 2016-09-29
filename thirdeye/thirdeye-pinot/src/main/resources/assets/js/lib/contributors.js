//Contributors section
function getContributors(tab) {

    var url = "/dashboard/data/contributor?" + window.location.hash.substring(1)

    getData(url, tab).done(function (data) {

        //Error handling when data is empty, undefined or null
        if (!data) {
            $("#" + tab + "-chart-area-error").empty();
            var warning = $('<div></div>', { class: 'uk-alert uk-alert-warning' });
            var closeBtn = $('<i></i>', { class: 'close-parent uk-icon-close' });
            warning.append($('<p></p>', { html: 'Something went wrong. Please try and reload the page. Error: data =' + data  }));
            $("#" + tab + "-chart-area-error").append(closeBtn);
            $("#" + tab + "-chart-area-error").append(warning);
            $("#" + tab + "-chart-area-error").show();
            return
        } else {
            $("#" + tab + "-chart-area-error").hide();
        }

        if (data.metrics.length == 0) {
            $("#" + tab + "-chart-area-error").empty();
            var warning = $('<div></div>', { class: 'uk-alert uk-alert-warning' });
            var closeBtn = $('<i></i>', { class: 'close-parent uk-icon-close' });
            warning.append($('<p></p>', { html: 'No metric data is present. Error: data.metrics.length = 0'  }));
            $("#" + tab + "-chart-area-error").append(closeBtn);
            $("#" + tab + "-chart-area-error").append(warning);
            $("#" + tab + "-chart-area-error").show();
            return
        }

        if (data.responseData.responseData.length == 0) {
            $("#" + tab + "-chart-area-error").empty();
            var warning = $('<div></div>', { class: 'uk-alert uk-alert-warning' });
            var closeBtn = $('<i></i>', { class: 'close-parent uk-icon-close' });
            warning.append($('<p></p>', { html: 'The result of the query is empty. Error: data.responseData.length = 0'  }));
            $("#" + tab + "-chart-area-error").append(closeBtn);
            $("#" + tab + "-chart-area-error").append(warning);
            $("#" + tab + "-chart-area-error").show();
            return
        }

        // Handelbars contributors table template
        var result_contributors_template = HandleBarsTemplates.template_contributors_table(data);
        $("#" + tab + "-display-chart-section").append(result_contributors_template);

        // Create timeseries
        renderContributionTimeSeries(data);

        // Calculate heatmap-cells-bg color
        calcHeatMapBG();

        // Translate UTC date into user selected or local timezone
        transformUTCToTZ();
        displayFiltersInTitle()

    })
}

function renderContributionTimeSeries(ajaxData) {

    var dateTimeFormat = "%I:%M %p";
    if (hash.hasOwnProperty("aggTimeGranularity") && hash.aggTimeGranularity == "DAYS") {
        dateTimeFormat = "%m-%d"
    }else if( ( ajaxData["timeBuckets"][ ajaxData.timeBuckets.length -1]["baselineEnd"] - ajaxData["timeBuckets"][0]["baselineStart"])  > 86400000 ){
        dateTimeFormat = "%m-%d %I %p";
    }

    var metrics = ajaxData['metrics'];
    var lineChartData = {};
    var lineChartMap = {};
    var barChartData = {};
    var barChartMap = {};
    var xTicksBaseline = [];
    var xTicksCurrent = [];
    for (var t = 0, len = ajaxData["timeBuckets"].length; t < len; t++) {
        var timeBucket = ajaxData["timeBuckets"][t]["currentStart"]
        xTicksBaseline.push(timeBucket);
        xTicksCurrent.push(timeBucket);
    }
    var schema = ajaxData["responseData"]["schema"]["columnsToIndexMapping"]
    for (var metricIndex = 0, metricsLen = metrics.length; metricIndex < metricsLen; metricIndex++) {
        var metricName = metrics[metricIndex]
        var dimensions = ajaxData['dimensions'];
        lineChartData[metricName] = {};
        lineChartMap[metricName] = {};
        barChartData[metricName] = {};
        barChartMap[metricName] = {};
        for (var dimensionIndex = 0; dimensionIndex < dimensions.length; dimensionIndex++) {
            var dimensionName = dimensions[dimensionIndex];
            lineChartData[metricName][dimensionName] = {};
            barChartData[metricName][dimensionName] = {};
            var dimensionValueArray = ajaxData["dimensionValuesMap"][dimensionName];
            var colorArray;
            if (dimensionValueArray.length < 10) {
                colorArray = d3.scale.category10().range();
            } else if (dimensionValueArray.length < 20) {
                colorArray = d3.scale.category20().range();
            } else {
                colorArray = colorScale(dimensionValueArray.length)
            }

            var colors = {};
            for (var dimensionValIndex = 0; dimensionValIndex < dimensionValueArray.length; dimensionValIndex++) {
                dimensionValue = dimensionValueArray[dimensionValIndex]
                key = metricName + "|" + dimensionName + "|" + dimensionValue;
                var rowIds = ajaxData["responseData"]["keyToRowIdMapping"][key];
                baselineValues = [];
                currentValues = [];
                percentageChanges = [];
                for (var rowId = 0; rowId < rowIds.length; rowId++) {
                    var rowData = ajaxData["responseData"]["responseData"][rowIds[rowId]];
                    baselineValues.push(rowData[schema["baselineValue"]]);
                    currentValues.push(rowData[schema["currentValue"]]);
                    percentageChanges.push(rowData[schema["percentageChange"]]);
                }
                lineChartData[metricName][dimensionName][dimensionValue + "-baseline"] = baselineValues;
                lineChartData[metricName][dimensionName][dimensionValue + "-current"] = currentValues;
                barChartData[metricName][dimensionName][dimensionValue + "-percentChange"] = percentageChanges;
                colors[dimensionValue + "-baseline"] = colorArray[dimensionValIndex];
                colors[dimensionValue + "-current"] = colorArray[dimensionValIndex];
                colors[dimensionValue + "-percentChange"] = colorArray[dimensionValIndex];
            }


            // line chart
            lineChartData[metricName][dimensionName]["time"] = xTicksCurrent;
            var lineChartBindTo = "#contributor-timeseries-" + metricName + "-" + dimensionName;
            lineChartMap[metricName][dimensionName] = c3.generate({
                bindto: lineChartBindTo,
                padding: {
                    top: 0,
                    right: 10,
                    bottom: 0,
                    left: 100
                },
                data: {
                    x: 'time',
                    json: lineChartData[metricName][dimensionName],
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
            // percent change map
            barChartData[metricName][dimensionName]["time"] = xTicksCurrent;
            var percentChangeChartBindTo = "#contributor-percentChange-" + metricName + "-" + dimensionName;
            barChartMap[metricName][dimensionName] = c3.generate({
                bindto: percentChangeChartBindTo,
                padding: {
                    top: 0,
                    right: 10,
                    bottom: 0,
                    left: 100,
                },
                data: {
                    x: 'time',
                    json: barChartData[metricName][dimensionName],
                    type: 'area-spline',
                    colors: colors
                },
                zoom: {
                    enabled: true,
                    rescale:true
                },
                axis: {
                    x: {
                        type: 'timeseries'
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
                        show: false
                    },
                    y: {
                        lines: [
                            {
                                value: 0
                            }
                        ]
                    }
                }
            });
            var lineChart = lineChartMap[metricName][dimensionName];
            var barChart = barChartMap[metricName][dimensionName];
            lineChart.hide();
            barChart.hide();

        }

    }

    // Clicking the checkbox of the timeseries legend will redraw the
    // timeseries with the selected elements
    $(".time-series-dimension-checkbox").on("click", function () {
        var checkboxObj = $(this);
        var metricName = $(this).attr("metric");
        var dimensionName = $(this).attr("dimension");
        var legendId = "#contributor-timeseries-legend-" + metricName + "-" + dimensionName;
        var dimValue = checkboxObj.val();

        var currentLineChart = lineChartMap[metricName][dimensionName];
        var currentBarChart = barChartMap[metricName][dimensionName];

        if (checkboxObj.is(':checked')) {
            currentLineChart.show(dimValue + "-current");
            currentLineChart.show(dimValue + "-baseline");
            currentBarChart.show(dimValue + "-percentChange");
        } else {
            currentLineChart.hide(dimValue + "-current");
            currentLineChart.hide(dimValue + "-baseline");
            currentBarChart.hide(dimValue + "-percentChange");
        }
    });

    $(".time-series-dimension-select-all-checkbox").on("click", "", function () {

        //if select all is checked
        if ($(this).is(':checked')) {
            var valueList = $(this).parent().next("div");
            //trigger click on each unchecked checkbox
            $(".time-series-dimension-checkbox", valueList).each(function (index, checkbox) {
                if (!$(checkbox).is(':checked')) {
                    $(checkbox).click();
                }
            })
        } else {
            //trigger click on each checked checkbox
            $(".time-series-dimension-checkbox").each(function (index, checkbox) {
                if ($(checkbox).is(':checked')) {
                    $(checkbox).click();
                }
            })
        }
    });

    //Initially select the first element of each dimension value list
    $($(".dimension-time-series-legend label:first-child .time-series-dimension-checkbox")).click();

}

function displayFiltersInTitle() {
    var hashParams = parseHashParameters(window.location.hash);
    if (hashParams.filters && hashParams.filters != "{}") {
        var filters = JSON.parse(hashParams.filters);
        var html = "";

        for (k in filters) {
            var values = decodeURIComponent(filters[k]).trim().split(",")

            //Todo: remove the following value adjustment when the "" values are part of OTHER in the response data
            for (i = 0, len = values.length; i < len; i++) {
                if (values[i] == "") {
                    values[i] = "UNKNOWN"
                }
            }

            html += " in <b>" + k + "</b>: <b>" + values + "</b>";
        }

        $(".filters-title").append(html);
    } else {
        $(".filters-title").empty()
    }
}

