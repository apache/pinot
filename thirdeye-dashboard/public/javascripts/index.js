// Globals
DIMENSION_NAMES = [];
MIN_TIME = null;
MAX_TIME = null;
AGGREGATION_SIZE = null;
AGGREGATION_UNIT = null;
FIXED_DIMENSIONS = {};
SHADE_COLOR = "#F6E0A0";

// Add time spinner to prototype
$.widget("ui.timespinner", $.ui.spinner, {
    options: {
        step: 60 * 1000,
        page: 60
    },

    _parse: function(value) {
        if (typeof value === 'string') {
            if (Number(value) == value) {
                return Number(value);
            }
            return +Globalize.parseDate(value);
        }
        return value;
    },

    _format: function(value) {
        return Globalize.format(new Date(value), "t");
    }
});

/**
 * Uses date input and baseline selection to update the baseline display date
 */
function loadBaseline() {
    date = $("#date-picker").val();
    baseline = $("#baseline").val();
    baselineMillis = baseline * 7 * 24 * 3600 * 1000;
    dateMillis = Date.parse(date);
    baselineDate = new Date(dateMillis - baselineMillis);
    $("#baseline-display").html(baselineDate.toDateString());
}

/**
 * Gets metric / dimension names from server
 */
function loadConfig() {
    collection = $("#collections").val();
    $.get('/data/collections/' + collection, function(config) {
        $("#metrics").empty();
        $.each(config['metricNames'], function(i, e) {
            $("#metrics").append('<option value="' + e + '">' + e + '</option>');
        });
        DIMENSION_NAMES = config['dimensionNames'];
        MIN_TIME = config['minTime'];
        MAX_TIME = config['maxTime'];
        AGGREGATION_SIZE = config['timeColumnAggregationSize'];
        AGGREGATION_UNIT = config['timeColumnAggregationUnit'];
        $("#time-window-units").html('(' + AGGREGATION_SIZE + ' ' + AGGREGATION_UNIT + ')');
    });
}

/**
 * Performs query given form inputs and fixed dimensions for each non-fixed dimension
 */
function doQuery() {
    // Clear heat map area
    $("#image-placeholder").empty();
    $("#heat-maps").empty();
    $("#heat-map-navigation").empty();
    $("#time-series").empty();
    $("#heat-map-navigation").append('<dd data-magellan-arrival="top-bar"><a href="#top-bar">&#8679;</a></dd>');

    // Get collection / metric
    collection = $("#collections").val();
    metric = $("#metrics").val();

    // Compute which "loose" dimensions we should query on
    dimensionsToQuery = []
    $.each(DIMENSION_NAMES, function(i,e) {
        if (FIXED_DIMENSIONS[e] == null) {
            dimensionsToQuery.push(e);
        }
    });

    // Add placeholders for these in alphabetical order
    dimensionsToQuery.sort();
    $.each(dimensionsToQuery, function(i, e) {
        // Heat map for this dimension
        $("#heat-maps").append($("<div id='" + e + "'></div>"));

        // Navigation for this dimension
        var navigator = $("<dd data-magellan-arrival='" + e + "'></dd>")
        navigator.append("<a href='#" + e + "'>" + e + "</a>");
        $("#heat-map-navigation").append(navigator);
    });

    // Get time range
    currentDate = getCurrentDate();
    baselineDate = getBaselineDate(currentDate, $("#baseline").val());

    // Check time range
    if (currentDate.getTime() > new Date().getTime() ||
        millisToCollectionTime(baselineDate.getTime()) < MIN_TIME ||
        millisToCollectionTime(currentDate.getTime()) > MAX_TIME) {
        alert("Time range not present in data set: " + baselineDate + " to " + currentDate);
        return;
    }

    // Get look back
    timeWindow = millisToCollectionTime(getTimeWindowMillis());

    // Generate time series plot
    baseTimeSeriesUrl = '/data/timeSeries/'
        + collection + '/'
        + metric + '/'
        + millisToCollectionTime(baselineDate.getTime()) + '/'
        + millisToCollectionTime(currentDate.getTime()) + '/'
        + timeWindow;
    timeSeriesUrl = addFixedDimensions(baseTimeSeriesUrl);
    $.get(timeSeriesUrl, generateTimeSeriesChart);

    // Query and generate heat maps
    $.each(dimensionsToQuery, function(i, dimensionName) {
        baseUrl = '/data/heatMap/'
            + collection + '/'
            + metric + '/'
            + dimensionName + '/'
            + millisToCollectionTime(baselineDate.getTime()) + '/'
            + millisToCollectionTime(currentDate.getTime()) + '/'
            + timeWindow;

        url = addFixedDimensions(baseUrl);

        $.get(url, function(data) {
            addLogValue(data, "baseline");
            normalize(data, "logBaseline", getStats(data, "logBaseline"));
            heatMap = generateHeatMap(dimensionName, data, 5, fixDimension);
            $("#" + dimensionName).append(heatMap);
        });
    });
}

function generateTimeSeriesChart(data) {
    // Add plot area
    var placeholder = $('<div id="time-series-plot"></div>')
        .css('width', $(window).width() * 0.80 + 'px')
        .css('height', '300px');

    // Find ranges to highlight
    currentDate = getCurrentDate();
    baselineDate = getBaselineDate(currentDate, $("#baseline").val());
    timeWindow = millisToCollectionTime(getTimeWindowMillis());
    currentCollectionTime = millisToCollectionTime(currentDate.getTime());
    baselineCollectionTime = millisToCollectionTime(baselineDate.getTime());

    // Get time series
    timeSeries = []
    baselineSum = 0;
    currentSum = 0;
    $.each(data, function(i, e1) {
        timeSeries.push(e1["timeSeries"]);
        if (e1["metricName"] === metric) {
            $.each(e1["timeSeries"], function(j, e2) {
                if (e2[0] <= baselineCollectionTime) {
                    baselineSum += e2[1];
                } else if (e2[0] >= currentCollectionTime - timeWindow) {
                    currentSum += e2[1];
                }
            });
        }
    });


    // Build title
    baseline = $("#baseline").val();
    metric = $("#metrics").val();
    ratio = (currentSum - baselineSum) / (1.0 * baselineSum);

    // Add elements
    $("#time-series").append("<h1>" + metric + " (" + (ratio * 100).toFixed(2) + "% w/" + baseline + "w)" + "</h1>");
    $("#time-series").append(placeholder);

    // Plot data
    $.plot(placeholder, timeSeries, {
        xaxis: {
            tickFormatter: function(collectionTime) {
                var date = new Date(collectionTimeToMillis(collectionTime));
                return date.toUTCString();
            }
        },
        grid: {
            markings: [
                { xaxis: { from: baselineCollectionTime - timeWindow, to: baselineCollectionTime }, color: SHADE_COLOR },
                { xaxis: { from: currentCollectionTime - timeWindow, to: currentCollectionTime }, color: SHADE_COLOR }
            ]
        }
    });
}

function addFixedDimensions(baseUrl) {
    var url = baseUrl;
    if (Object.keys(FIXED_DIMENSIONS).length > 0) {
        var first = true;
        for (d in FIXED_DIMENSIONS) {
            url += first ? "?" : "&";
            first = false;
            url += encodeURIComponent(d) + "=" + encodeURIComponent(FIXED_DIMENSIONS[d]);
        }
    }
    return url;
}

function fixDimension() {
    FIXED_DIMENSIONS[this.getAttribute("dimension")] = this.text;
    refreshBreadcrumbs();
    doQuery();
}

function relaxDimension() {
    var tokens = this.innerHTML.split(":");
    delete FIXED_DIMENSIONS[tokens[0]];
    refreshBreadcrumbs();
    doQuery();
}

function refreshBreadcrumbs() {
    $("#fixed-dimensions").empty();
    if (Object.keys(FIXED_DIMENSIONS).length > 0) {
        var breadcrumbs = $('<ul class="breadcrumbs"></ul>');
        var sortedKeys = Object.keys(FIXED_DIMENSIONS).sort();
        $.each(sortedKeys, function(i, e) {
            var breadcrumb = $('<li>' + e + ':' + FIXED_DIMENSIONS[e] + '</li>');
            breadcrumb.click(relaxDimension);
            breadcrumbs.append(breadcrumb);
        });
        $("#fixed-dimensions").append(breadcrumbs);
    }
}

/**
 * Extracts the timeWindow from slider
 */
function getTimeWindowMillis() {
    return collectionTimeToMillis($("#time-window").val());
}

/**
 * Extracts the date / time inputs and creates a Date object
 */
function getCurrentDate() {
    date = $("#date-picker").val();
    time = $("#spinner").val();
    millis = Date.parse(date + " " + time);
    return new Date(millis)
}

/**
 * Computes the baseline date given current date and delta
 */
function getBaselineDate(currentDate, deltaWeeks) {
    date = new Date(currentDate.getTime());
    date.setDate(date.getDate() - deltaWeeks * 7);
    return date;
}

/**
 * Converts milliseconds to the collection time
 */
function millisToCollectionTime(millis) {
    return Math.floor(millis / getFactor() / AGGREGATION_SIZE);
}

function collectionTimeToMillis(collectionTime) {
    return collectionTime * getFactor() * AGGREGATION_SIZE;
}

function getFactor() {
    if (AGGREGATION_UNIT == 'SECONDS') {
        return 1000;
    } else if (AGGREGATION_UNIT == 'MINUTES') {
        return 60 * 1000;
    } else if (AGGREGATION_UNIT == 'HOURS') {
        return 60 * 60 * 1000;
    } else if (AGGREGATION_UNIT == 'DAYS') {
        return 24 * 60 * 60 * 1000;
    }
    return 1;
}

function generateHeatMap(dimension, tuples, numColumns, selectCallback) {

    // Compute sum of baseline
    var baselineSum = 0;
    $.each(tuples, function(i, e) {
        baselineSum += tuples[i]['baseline'];
    });

    // Generate cells
    var volumeRatioSum = 0;
    var cells = []
    for (var i = 0; i < tuples.length; i++) {
        if (tuples[i]["baseline"] > 0) {
            var volumeRatio = (tuples[i]["current"] - tuples[i]["baseline"]) / (1.0 * baselineSum);
            var selfRatio = (tuples[i]["current"] - tuples[i]["baseline"]) / (1.0 * tuples[i]["baseline"]);

            var link = $('<a href="#" dimension="' + dimension + '">' + tuples[i]['value'] + '</a>');
            link.attr("title", "(current=" + tuples[i]["current"] + ", baseline=" + tuples[i]["baseline"] + ", change=" + (selfRatio * 100).toFixed(2) + "%)");
            $(link).click(selectCallback);

            var cell = $('<td></td>');
            cell.addClass('cell');
            cell.append(link);
            cell.append('</br>' + (volumeRatio * 100).toFixed(2) + '%');
            cell.css('background-color', 'rgba(136,138,252,' + tuples[i]["prob"] + ')');
            cells.push(cell);

            volumeRatioSum += volumeRatio;
        }
    }

    // Create link to generate modal time series
    var modalTimeSeriesLink = $('<a href="#" class="modal-time-series-link" data-reveal-id="modal-time-series" dimension="' + dimension + '">&#10138;</a>');
    modalTimeSeriesLink.attr('title', 'Show time-series');
    modalTimeSeriesLink.click(generateModalTimeSeries);

    // Create table
    var table = $("<table class='heatmap'></table>");
    var caption = $("<caption>" + dimension + "</caption>");
    caption.append(modalTimeSeriesLink);
    table.append(caption);

    // Create cells
    var batch = [];
    for (var i = 0; i < cells.length; i++) {
        batch.push(cells[i]);
        if (batch.length == numColumns) {
            var row = $("<tr></tr>");
            for (var j = 0; j < batch.length; j++) {
                row.append(batch[j]);
            }
            table.append(row);
            batch = [];
        }
    }
    if (batch.length > 0) { // any stragglers
        var row = $("<tr></tr>");
        for (var i = 0; i < batch.length; i++) {
            row.append(batch[i]);
        }
        table.append(row);
    }

    return table;
}

function addLogValue(tuples, keyName) {
    var logKeyName = "log" + keyName.charAt(0).toUpperCase() + keyName.slice(1);
    for (var i = 0; i < tuples.length; i++) {
        tuples[i][logKeyName] = Math.log(tuples[i][keyName])
    }
}

function getStats(tuples, keyName) {
    sum = 0
    sumSquares = 0
    for (var i = 0; i < tuples.length; i++) {
        if (tuples[i][keyName] !== Number.NEGATIVE_INFINITY) {
            sum += tuples[i][keyName]
            sumSquares += tuples[i][keyName] * tuples[i][keyName]
        }
    }
    return [sum / tuples.length, (sumSquares - (sum * sum) / tuples.length) / (tuples.length - 1)]
}

function normalize(tuples, keyName, stats) {
    var mean = stats[0]
    var stdev = Math.sqrt(stats[1])
    for (var i = 0; i < tuples.length; i++) {
        var score = (tuples[i][keyName] - mean) / stdev
        tuples[i]['score'] = score
        tuples[i]['prob'] = normalcdf(score)
    }
}

// from: http://www.math.ucla.edu/~tom/distributions/normal.html
function normalcdf(X){   //HASTINGS.  MAX ERROR = .000001
    var T=1/(1+.2316419*Math.abs(X));
    var D=.3989423*Math.exp(-X*X/2);
    var Prob=D*T*(.3193815+T*(-.3565638+T*(1.781478+T*(-1.821256+T*1.330274))));
    if (X>0) {
        Prob=1-Prob
    }
    return Prob
}

function generateModalTimeSeries() {
    $("#modal-time-series").empty();

    dimensionName = this.getAttribute("dimension");

    // Get all displayed cells for this dimension
    cells = $("#" + dimensionName + " td > a");

    // Get top 5 cells
    dimensionValues = [];
    for (i = 0; i < 5; i++) {
        dimensionValues.push(cells[i].text);
    }

    // Params
    collection = $("#collections").val();
    metric = $("#metrics").val();
    currentDate = getCurrentDate();
    baselineDate = getBaselineDate(currentDate, $("#baseline").val());
    lookBack = getTimeWindow();
    baselineCollectionTime = millisToCollectionTime(baselineDate.getTime());
    currentCollectionTime = millisToCollectionTime(currentDate.getTime());

    // Base query
    url = '/data/timeSeries/'
        + collection + '/'
        + metric + '/'
        + baselineCollectionTime + '/'
        + currentCollectionTime + '/'
        + lookBack;

    // Add all fixed dimensions
    url = addFixedDimensions(url);

    // Add these explicit time series
    $.each(dimensionValues, function(i, e) {
        if (i == 0 && Object.keys(FIXED_DIMENSIONS).length == 0) {
            url += '?';
        }
        else {
            url += '&';
        }
        url += encodeURIComponent(dimensionName) + '=' + encodeURIComponent(e);
    });

    // Render time series for top 5 cells
    $.get(url, function(data) {
        var container = $("#modal-time-series");

        var placeholder = $('<div id="time-series-plot"></div>')
            .css('width', container.width() * 0.75 + 'px')
            .css('height', '500px')
            .css('float', 'left');

        var legend = $('<div id="time-series-legend"></div>')
            .css('width', container.width() * 0.20 + 'px')
            .css('margin-right', '25px')
            .css('margin-top', '10px')
            .css('float', 'right');

        // Sort w.r.t. appearance order in heat map
        data.sort(function(a, b) {
            aVal = a["dimensionValues"][dimensionName];
            bVal = b["dimensionValues"][dimensionName];
            return dimensionValues.indexOf(aVal) - dimensionValues.indexOf(bVal);
        });

        timeSeries = [];
        $.each(data, function(i, e) {
            timeSeries.push({ label: e["dimensionValues"][dimensionName], data: e["timeSeries"] });
        });

        container.append(placeholder);
        container.append(legend);
        container.append('<a class="close-reveal-modal">&#215;</a>');

        $.plot(placeholder, timeSeries, {
            xaxis: {
                tickFormatter: function(hoursSinceEpoch) {
                    var date = new Date(hoursSinceEpoch * 3600 * 1000);
                    return date.toUTCString();
                }
            },
            grid: {
                markings: [
                    { xaxis: { from: baselineCollectionTime - lookBack, to: baselineCollectionTime }, color: SHADE_COLOR },
                    { xaxis: { from: currentCollectionTime - lookBack, to: currentCollectionTime }, color: SHADE_COLOR }
                ]
            },
            legend: {
                container: legend
            }
        });
    });
}

$(function() {
    // Date picker
    $("#date-picker")
        .datepicker()
        .datepicker("setDate", new Date());

    // Time spinner
    $("#spinner").timespinner();
    $(".ui-spinner").removeClass("ui-corner-all");

    // Load metrics on collection change
    $("#collections").change(loadConfig)

    // Update baseline
    $("#baseline").change(loadBaseline);
    $("#date-picker").change(loadBaseline);
    loadBaseline();

    // Query system
    $("#query").click(doQuery)

    // Get collections
    $.get("/data/collections", function(collections) {
        if (collections.length == 0) {
            alert("No collections registered!");
            return;
        }

        $.each(collections, function(i, e) {
            $("#collections").append('<option value="' + e + '">' + e + '</option>');
        });

        loadConfig();
    });
});
