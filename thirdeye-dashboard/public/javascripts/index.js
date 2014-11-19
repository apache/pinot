// Globals
DIMENSION_NAMES = [];
MIN_TIME = null;
MAX_TIME = null;
FIXED_DIMENSIONS = {};

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
    $.get('/data/' + collection + '/config', function(config) {
        $.each(config['metricNames'], function(i, e) {
            $("#metrics").append('<option value="' + e + '">' + e + '</option>');
        });
        DIMENSION_NAMES = config['dimensionNames'];
        MIN_TIME = config['minTime'];
        MAX_TIME = config['maxTime'];
    });
}

/**
 * Performs query given form inputs and fixed dimensions for each non-fixed dimension
 */
function doQuery() {
    // Clear heat map area
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
        $("#heat-maps").append($("<div id='" + e + "'></div>"));

        var navigator = $("<dd data-magellan-arrival='" + e + "'></dd>")
        navigator.append("<a href='#" + e + "'>" + e + "</a>");
        $("#heat-map-navigation").append(navigator);
    });

    // Get time range
    currentDate = getCurrentDate();
    baselineDate = getBaselineDate(currentDate, $("#baseline").val());

    // Check time range
    if (currentDate.getTime() > new Date().getTime() ||
        millisToHoursSinceEpoch(baselineDate.getTime()) < MIN_TIME ||
        millisToHoursSinceEpoch(currentDate.getTime()) > MAX_TIME) {
        alert("Time range not present in data set: " + baselineDate + " to " + currentDate);
        return;
    }

    // Get look back
    lookBack = getLookBack();

    // Generate time series plot
    baseTimeSeriesUrl = '/data/timeSeries/'
        + collection + '/'
        + metric + '/'
        + millisToHoursSinceEpoch(baselineDate.getTime()) + '/'
        + millisToHoursSinceEpoch(currentDate.getTime()) + '/'
        + lookBack;
    timeSeriesUrl = addFixedDimensions(baseTimeSeriesUrl);
    $.get(timeSeriesUrl, generateTimeSeriesChart);

    // Query and generate heat maps
    $.each(dimensionsToQuery, function(i, dimensionName) {
        baseUrl = '/data/heatmap/'
            + collection + '/'
            + metric + '/'
            + dimensionName + '/'
            + millisToHoursSinceEpoch(baselineDate.getTime()) + '/'
            + millisToHoursSinceEpoch(currentDate.getTime()) + '/'
            + lookBack;

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
    $("#time-series").append(placeholder);

    // Find ranges to highlight
    currentDate = getCurrentDate();
    baselineDate = getBaselineDate(currentDate, $("#baseline").val());
    lookBack = getLookBack();
    currentHoursSinceEpoch = millisToHoursSinceEpoch(currentDate.getTime());
    baselineHoursSinceEpoch = millisToHoursSinceEpoch(baselineDate.getTime());

    // Get time series
    timeSeries = []
    $.each(data, function(i, e) {
        timeSeries.push(e["timeSeries"]);
    });

    // Plot data
    shadeColor = "#F6E0A0";
    $.plot(placeholder, timeSeries, {
        xaxis: {
            tickFormatter: function(hoursSinceEpoch) {
                var date = new Date(hoursSinceEpoch * 3600 * 1000);
                return date.toUTCString();
            }
        },
        grid: {
            markings: [
                { xaxis: { from: baselineHoursSinceEpoch - lookBack, to: baselineHoursSinceEpoch }, color: shadeColor },
                { xaxis: { from: currentHoursSinceEpoch - lookBack, to: currentHoursSinceEpoch }, color: shadeColor }
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
 * Extracts the lookBack from slider
 */
function getLookBack() {
    return parseInt($("#look-back").val());
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
 * Converts milliseconds to equivalent hours
 */
function millisToHoursSinceEpoch(millis) {
    return Math.floor(millis / 3600 / 1000);
}

function generateHeatMap(dimension, tuples, numColumns, selectCallback) {

    // Generate cells
    var cells = []
    for (var i = 0; i < tuples.length; i++) {
        if (tuples[i]["baseline"] > 0) {
            var ratio = (tuples[i]["current"] - tuples[i]["baseline"]) / (1.0 * tuples[i]["baseline"]);
            var alpha = tuples[i]["prob"];
            var link = $('<a href="#" dimension="' + dimension + '">' + tuples[i]['value'] + '</a>');
            var cell = $('<td></td>');
            link.attr("title", "(current=" + tuples[i]["current"] + ",baseline=" + tuples[i]["baseline"] + ")");
            cell.addClass('cell');
            cell.append(link);
            cell.append('</br>' + (ratio.toPrecision(3) * 100).toFixed(2) + '%');
            cell.css('background-color', 'rgba(136,138,252,' + alpha + ')');
            cells.push(cell);
            $(link).click(selectCallback);
        }
        // TODO: Make an option to show these cells, but for now, just ignore them
//        else {
//            // Grayed out cell
//            var cell = $('<td>' + tuples[i]['value'] + '<br/>N/A</td>');
//            cell.addClass('disabled-cell');
//            cells.push(cell);
//        }
    }


    // Create table
    var table = $("<table class='heatmap'></table>");
    table.append($("<caption>" + dimension + "</caption>"))
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
