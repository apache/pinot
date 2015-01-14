// Globals
DIMENSION_NAMES = [];
AGGREGATION_SIZE = null;
AGGREGATION_UNIT = null;
FIXED_DIMENSIONS = {};
TIME_SERIES_MARGIN = 1000 * 60 * 60 * 24; // 1 day
EMPTY_STRING = '-';
EXCLUDED_SHADE_COLOR = '#EDEDEF';
BOUNDARY_SHADE_COLOR = '#000000';

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
        $("#metrics-options").empty();
        $("#fixed-dimensions").empty();
        $("#time-series").empty();
        $("#heat-maps").empty();
        $("#heat-map-navigation-area").empty();
        $("#image-placeholder").css('display', 'block');

        $.each(config['metrics'], function(i, e) {
            var metricName = e['name'];
            $("#metrics").append('<option value="' + metricName + '">' + metricName + '</option>');

            $('<input />', {type: 'checkbox', id: 'checkbox-' + metricName, class: 'metrics-checkbox', value: metricName}).appendTo($("#metrics-options"));
            $('<label />', {'for': 'checkbox-' + metricName, text: metricName}).appendTo($("#metrics-options"));
            $('<br />').appendTo($("#metrics-options"));
        });
        initMetricSelection();

        // Globals
        FIXED_DIMENSIONS = {};
        DIMENSION_NAMES = [];
        AGGREGATION_SIZE = config['time']['bucket']['size'];
        AGGREGATION_UNIT = config['time']['bucket']['unit'];

        $.each(config['dimensions'], function(i, e) {
            DIMENSION_NAMES.push(e['name']);
        });

        $("#time-window-units").html('(' + AGGREGATION_SIZE + ' ' + AGGREGATION_UNIT + ')');
    });
}

/**
 * Sets the selected metrics
 */
function initMetricSelection() {
    var metric = $("#metrics").val();
    $(".metrics-checkbox").each(function(i, e) {
        if (e.getAttribute('value') == metric) {
            e.checked = true;
        } else {
            e.checked = false;
        }
    });
}

/**
 * Performs query given form inputs and fixed dimensions for each non-fixed dimension
 */
function doQuery() {
    // Clear heat map area
    $("#image-placeholder").css('display', 'none');
    $("#heat-maps").empty();
    $("#heat-map-navigation").empty();
    $("#time-series").empty();
    $("#heat-map-navigation").append('<dd data-magellan-arrival="top-bar"><a href="#top-bar">&#8679;</a></dd>');

    // Get collection / metric
    collection = $("#collections").val();
    metric = $("#metrics").val();

    // Get metrics
    timeSeriesMetrics = [];
    $(".metrics-checkbox").each(function(i, e) {
        if (e.checked) {
            timeSeriesMetrics.push(e.getAttribute('value'));
        }
    })

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

    // Get time window
    timeWindow = millisToCollectionTime(getTimeWindowMillis());

    // Check time window
    if (timeWindow < 1) {
        alert("Time window must be >= 1");
        return;
    }

    // Check if should normalize
    var normalized = $("#normalized")[0].checked;

    // Generate time series plot (include margin before baseline and after current)
    baseTimeSeriesUrl = '/data/timeSeries/'
        + collection + '/'
        + timeSeriesMetrics.join(',') + '/'
        + millisToCollectionTime(baselineDate.getTime() - TIME_SERIES_MARGIN) + '/'
        + millisToCollectionTime(currentDate.getTime() + TIME_SERIES_MARGIN) + '/'
        + timeWindow + '/'
        + normalized;
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

    metric = $("#metrics").val();
    borderPoints = {
        data: [],
        points: {
            show: true
        },
        label: null,
        color: 'red'
    };
    $.each(data, function(i, series) {
        if (series['label'] == metric) {
            $.each(series['data'], function(j, point) {
                if (point[0] == baselineCollectionTime || point[0] == currentCollectionTime) {
                    borderPoints['data'].push(point);
                }
            });
        }
    });
    data.push(borderPoints);

    // Add elements
    $("#time-series").append(placeholder);
    var legendContainer = $("<div id='legend-container'></div>")
    $("#time-series").append(legendContainer);

    // Plot data
    var plot = $.plot(placeholder, data, {
        xaxis: {
            tickFormatter: tickFormatter
        },
        legend: {
            show: true,
            container: legendContainer,
            labelFormatter: function(label, series) {
                return '<span id="' + label + '-current-baseline"></span> ' + label;
            }
        },
        series: {
        },
        grid: {
            clickable: true,
            hoverable: true,
            markings: [
                { xaxis: { from: baselineCollectionTime, to: baselineCollectionTime }, color: BOUNDARY_SHADE_COLOR, lineWidth: 1 },
                { xaxis: { from: currentCollectionTime, to: currentCollectionTime}, color: BOUNDARY_SHADE_COLOR, lineWidth: 1 }
            ]
        }
    });

    // Add labels on border points
    $.each(borderPoints['data'], function(i, point) {
        var offset = plot.pointOffset({ x: point[0], y: point[1] });
        var label = point[1];
        placeholder.append("<div style='position:absolute;left:" + (offset.left + 4) + "px;top:" + offset.top + "px;color:#666;font-size:smaller'>" + label + "</div>");
    });

    // Get overall ratios for plotted series
    $(".metrics-checkbox").each(function(i, e) {
        if (e.checked) {
            metric = e.getAttribute('value');
            url = '/data/metricRatio/'
                + collection + '/'
                + metric + '/'
                + baselineCollectionTime + '/'
                + currentCollectionTime + '/'
                + timeWindow;
            url = addFixedDimensions(url);
            $.get(url, function(data) {
                var name = data['name'];
                var ratio = (data['ratio'] * 100).toFixed(2);
                $("#" + name + "-current-baseline").html('(' + ratio + '% w/' + baseline + 'w)');
            });
        }
    })

    // Tooltip
    $('<div id="tooltip"></div>').css({
        position: 'absolute',
        display: 'none',
        border: '1px solid #fdd',
        padding: '2px',
        'background-color': '#fee',
        opacity: 0.80
    }).appendTo('body');

    // Hover handler
    placeholder.bind('plothover', function(event, pos, item) {
        if (item) {
            time = item.datapoint[0].toFixed(2);
            value = item.datapoint[1].toFixed(2);

            $("#tooltip").html(value + ' @ (' + tickFormatter(time) + ")")
                         .css({ top: item.pageY + 5, left: item.pageX + 5 })
                         .fadeIn(200);
        } else {
            $('#tooltip').hide();
        }
    });

    // Click handler
    placeholder.bind('plotclick', function(event, pos, item) {
        if (item) {
            date = new Date(collectionTimeToMillis(item.datapoint[0]));
            $("#date-picker").val(formatDatePickerInput(date));
            $("#spinner").val(formatSpinnerInput(date));
        }
    });
}

function formatDatePickerInput(date) {
    month = date.getUTCMonth() + 1;
    day = date.getUTCDate();
    year = date.getUTCFullYear();
    return month + '/' + day + '/' + year;
}

function formatSpinnerInput(date) {
    hours = date.getUTCHours();
    minutes = date.getUTCMinutes();
    ampm = hours >= 12 ? 'pm' : 'am';
    hours %= 12;
    hours = hours ? hours : 12;
    minutes = minutes < 10 ? '0' + minutes : minutes;
    return hours + ':' + minutes + ' ' + ampm;
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

function tickFormatter(collectionTime) {
    var date = new Date(collectionTimeToMillis(collectionTime));
    return date.toUTCString();
}

function fixDimension() {
    FIXED_DIMENSIONS[this.getAttribute("dimension")] = this.text == EMPTY_STRING ? '' : this.text;
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
            var value = FIXED_DIMENSIONS[e] ? FIXED_DIMENSIONS[e] : EMPTY_STRING;
            var breadcrumb = $('<li>' + e + ':' + value + '</li>');
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
    millis = Date.parse(date + " " + time + " GMT");
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
    var currentSum = 0;
    $.each(tuples, function(i, e) {
        baselineSum += tuples[i]['baseline'];
        currentSum += tuples[i]['current'];
    });

    // Generate cells
    var volumeRatioSum = 0;
    var cells = []
    for (var i = 0; i < tuples.length; i++) {
        if (tuples[i]["baseline"] > 0) {
            var volumeRatio = (tuples[i]["current"] - tuples[i]["baseline"]) / (1.0 * baselineSum);
            var selfRatio = (tuples[i]["current"] - tuples[i]["baseline"]) / (1.0 * tuples[i]["baseline"]);
            var delta = tuples[i]['current'] / (1.0 * currentSum) - tuples[i]['baseline'] / (1.0 * baselineSum);
            var value = tuples[i]['value'] ? tuples[i]['value'] : EMPTY_STRING;

            var link = $('<a href="#" dimension="' + dimension + '">' + value + '</a>');
            link.attr("title", "(current=" + tuples[i]["current"] + ", baseline=" + tuples[i]["baseline"] + ", change=" + (selfRatio * 100).toFixed(2) + "%)");
            $(link).click(selectCallback);

            var cell = $('<td></td>');
            cell.addClass('cell');
            cell.append(link);
            cell.append('</br>' + (volumeRatio * 100).toFixed(2) + '%');
            cell.append(' <span class="cell-delta">(&Delta;' + (delta * 100).toFixed(2) + ')</span>')
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
    numTimeSeries = Math.min(cells.length, 5);

    // Get top 5 cells
    dimensionValues = [];
    for (i = 0; i < numTimeSeries; i++) {
        dimensionValues.push(cells[i].text);
    }

    // Params
    collection = $("#collections").val();
    metric = $("#metrics").val();
    currentDate = getCurrentDate();
    baselineDate = getBaselineDate(currentDate, $("#baseline").val());
    timeWindow = millisToCollectionTime(getTimeWindowMillis());
    baselineCollectionTime = millisToCollectionTime(baselineDate.getTime() - TIME_SERIES_MARGIN);
    currentCollectionTime = millisToCollectionTime(currentDate.getTime() + TIME_SERIES_MARGIN);
    normalized = $("#normalized")[0].checked;

    // Base query
    url = '/data/timeSeries/'
        + collection + '/'
        + metric + '/'
        + baselineCollectionTime + '/'
        + currentCollectionTime + '/'
        + timeWindow + '/'
        + normalized;

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
        var currentDate = getCurrentDate();
        var baselineDate = getBaselineDate(currentDate, $("#baseline").val());
        var currentCollectionTime = millisToCollectionTime(currentDate.getTime());
        var baselineCollectionTime = millisToCollectionTime(baselineDate.getTime());

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
            timeSeries.push({ label: e["dimensionValues"][dimensionName], data: e["data"] });
        });

        container.append(placeholder);
        container.append(legend);
        container.append('<a class="close-reveal-modal">&#215;</a>');

        $.plot(placeholder, timeSeries, {
            xaxis: {
                tickFormatter: tickFormatter
            },
            legend: {
                container: legend
            },
            grid: {
                clickable: true,
                hoverable: true,
                markings: [
                    { xaxis: { from: baselineCollectionTime - millisToCollectionTime(TIME_SERIES_MARGIN), to: baselineCollectionTime }, color: EXCLUDED_SHADE_COLOR },
                    { xaxis: { from: currentCollectionTime, to: currentCollectionTime + millisToCollectionTime(TIME_SERIES_MARGIN) }, color: EXCLUDED_SHADE_COLOR }
                ]
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

    // Reset selected metrics on metrics change
    $("#metrics").change(initMetricSelection);

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
