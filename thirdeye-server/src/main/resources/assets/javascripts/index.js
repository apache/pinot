/**
 * Renders UI components, metric time series, and heat maps.
 */

TIME_SERIES_MARGIN = 1000 * 60 * 60 * 24 // 1 day
EMPTY_STRING = '-'

/**
 * Initialize jQuery UI components
 */
function loadInputComponents() {
    $("#spinner").timespinner()
    $(".ui-spinner").removeClass("ui-corner-all")
    $("#date-picker").datepicker().datepicker("setDate", new Date())
    $("#baseline").change(loadBaseline)
    $("#date-picker").change(loadBaseline)
    $("#collections").change(loadConfig)
    $("#metrics").change(loadMetricSelection)
    $("#query").click(doQuery)

    // Allow tabs in function
    $("#user-function").keydown(function(e) {
        if (e.keyCode == 9) { // tab
            e.preventDefault()
            var start = $(this).get(0).selectionStart;
            var end = $(this).get(0).selectionEnd;
            $(this).val($(this).val().substring(0, start)
                        + "\t"
                        + $(this).val().substring(end));
            $(this).get(0).selectionStart =
            $(this).get(0).selectionEnd = start + 1;
        }
    })
}

/**
 * Sets the selected metrics
 */
function loadMetricSelection() {
    $(".metrics-checkbox").each(function(i, metric) {
        metric.checked = $("#metrics").val() === metric.getAttribute("value")
    })
}

/**
 * Loads the configuration for a collection
 */
function loadConfig() {
    $.get('/collections/' + encodeURIComponent($("#collections").val()), function(config) {

        // Reset
        $("#metrics").empty()
        $("#metrics-options").empty()
        $("#dimensions").empty()
        $("#image-placeholder").css('display', 'block')

        // Metrics
        $.each(config["metrics"], function(i, metricSpec) {

            // Metrics drop-down input
            $("#metrics").append($("<option></option>", {
                value: metricSpec["name"],
                text: metricSpec["name"]
            }))

            // Modal metrics selection
            $("#metrics-options").append($("<input />", {
                type: "checkbox",
                id: "checkbox-" + metricSpec["name"],
                class: "metrics-checkbox",
                value: metricSpec["name"]
            }))
            .append($("<label />", {
                'for': 'checkbox-' + metricSpec["name"],
                text: metricSpec["name"]
            }))
            .append("<br />")
        })

        // Time
        $("#time-window-size").html(config["time"]["bucket"]["size"])
        $("#time-window-unit").html(config["time"]["bucket"]["unit"])

        // Dimensions
        $.each(config["dimensions"], function(i, dimensionSpec) {
            $("#dimensions").append($("<li></li>", {
                'name': dimensionSpec["name"],
                'value': '*'
            }))
        })

        // Collection stats
        $.get('/collections/' + encodeURIComponent($("#collections").val()) + '/stats', function(stats) {
            $("#config > #min-time").html(stats["minTime"])
            $("#config > #max-time").html(stats["maxTime"])
        })

        loadMetricSelection()
    })
}

function addFixedDimensions(baseUrl, dimensionValues) {
    var url = baseUrl
    var first = true
    $.each(dimensionValues, function(name, value) {
        if (value !== "*") {
            url += first ? "?" : "&"
            url += encodeURIComponent(name) + "=" + encodeURIComponent(value)
            first = false
        }
    })
    return url
}

/**
 * Gets metric time series / heat maps from server
 */
function doQuery() {

    // Reset
    $("#image-placeholder").css('display', 'none')
    $("#heat-maps").empty()
    $("#time-series").empty()

    // Get selected metrics
    var selectedMetrics = []
    $(".metrics-checkbox").each(function(i, metric) {
        if (metric.checked) {
            selectedMetrics.push(metric.getAttribute("value"))
        }
    })

    // Get remaining dimensions on which to query and their values
    var dimensionsToQuery = []
    var dimensionValues = {}
    $("#dimensions > li").each(function(i, dimension) {
        var name = dimension.getAttribute("name")
        var value = dimension.getAttribute("value")
        if (value === "*") {
            dimensionsToQuery.push(name)
        }
        dimensionValues[name] = value
    })

    // Add heat map placeholders for dimensions to query
    $.each(dimensionsToQuery, function(i, dimensionName) {
        // Heat map
        $("#heat-maps").append($("<div></div>", {
            id: dimensionName
        }))
    })

    // Get time
    var currentDate = getCurrentDate()
    var baselineDate = getBaselineDate(currentDate, $("#baseline").val())
    var timeWindow = millisToCollectionTime(getTimeWindowMillis())
    var baselineCollectionTime = millisToCollectionTime(baselineDate.getTime())
    var currentCollectionTime = millisToCollectionTime(currentDate.getTime())
    var marginCollectionTime = millisToCollectionTime(TIME_SERIES_MARGIN)

    // Check time
    checkTime(baselineCollectionTime)
    checkTime(currentCollectionTime)
    if (timeWindow < 1) {
        alert("Time window must be >= 1")
        return
    }


    // Time series plot (include margin before baseline and after current)
    var type = $("#normalized")[0].checked ? "normalized" : "raw"

    var url = '/timeSeries/' + type + '/'
        + encodeURIComponent($("#collections").val()) + '/'
        + encodeURIComponent(selectedMetrics.join(',')) + '/'
        + (baselineCollectionTime - marginCollectionTime) + '/'
        + (currentCollectionTime + marginCollectionTime) + '/'
        + timeWindow

    url = addFixedDimensions(url, dimensionValues)

    $.get(url, generateTimeSeriesChart)

    // Heat maps
    $.each(dimensionsToQuery, function(i, dimensionName) {
        var url = "/heatMap/"
            + encodeURIComponent($("#heat-map-type").attr("value")) + "/"
            + encodeURIComponent($("#collections").val()) + "/"
            + encodeURIComponent($("#metrics").val()) + "/"
            + encodeURIComponent(dimensionName) + "/"
            + baselineCollectionTime + "/"
            + (baselineCollectionTime + timeWindow) + "/"
            + currentCollectionTime + "/"
            + (currentCollectionTime + timeWindow)

        url = addFixedDimensions(url, dimensionValues)

        $.get(url, function(data) {
            $("#" + dimensionName).append(generateHeatMap(dimensionName, data))
        })
    })
}

function hexToRgb(hex) {
    var result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex)
    return result ? {
        r: parseInt(result[1], 16),
        g: parseInt(result[2], 16),
        b: parseInt(result[3], 16)
    } : null
}

function generateHeatMap(dimensionName, data) {

    // Convert to table cells
    var cells = []
    $.each(data, function(i, datum) {
        var color = hexToRgb(datum['color'])

        var cell = $("<td></td>", {
            class: 'cell'
        })
        .css('background-color', 'rgba(' + color.r + ',' + color.g + ',' + color.b + ',' + datum['alpha'] + ')')

        var link = $("<a></a>", {
            'href': '#',
            'dimension': dimensionName,
            'text': datum["dimensionValue"] ? datum["dimensionValue"] : EMPTY_STRING
        })
        .click(fixDimension)

        var ratio = "<br/>" + (datum["ratio"] * 100).toFixed(2) + "%"

        cells.push(cell.append(link).append(ratio))
    })

    // Create heat map
    if (cells.length > 0) {
        var table = $("<table></table>", {
            class: 'heatmap'
        }).append($("<caption></caption>", {
            text: dimensionName
        }))

        var batch = []

        $.each(cells, function(i, cell) {
            batch.push(cell)

            // Flush at 5 columns per row
            if (batch.length == 5) {
                var row = $("<tr></tr>")
                $.each(batch, function(j, batchCell) {
                    row.append(batchCell)
                })
                table.append(row)
                batch = []
            }
        })

        if (batch.length > 0) {
            var row = $("<tr></tr>")
            $.each(batch, function(j, batchCell) {
                row.append(batchCell)
            })
            table.append(row)
        }

        return table
    }

    return null
}

function generateTimeSeriesChart(data) {
    // Add plot area
    var placeholder = $('<div id="time-series-plot"></div>')
        .css('width', $(window).width() * 0.80 + 'px')
        .css('height', '300px')

    var currentDate = getCurrentDate()
    var baselineDate = getBaselineDate(currentDate, $("#baseline").val())
    var timeWindow = millisToCollectionTime(getTimeWindowMillis())
    var currentCollectionTime = millisToCollectionTime(currentDate.getTime())
    var baselineCollectionTime = millisToCollectionTime(baselineDate.getTime())
    var metric = $("#metrics").val()

    var dimensionValues = {}
    $("#dimensions > li").each(function(i, dimension) {
        var name = dimension.getAttribute("name")
        var value = dimension.getAttribute("value")
        dimensionValues[name] = value
    })

    // Add elements
    $("#time-series").append(placeholder)
    var legendContainer = $("<div id='legend-container'></div>")
    $("#time-series").append(legendContainer)

    // Evaluate any UDF
    var userFunction = $("#user-function").val()
    if (userFunction) {
        var grouped = {}
        $.each(data, function(i, series) {
            grouped[series["label"]] = series["data"]
        })

        try {
            grouped = eval('(function(series) {' + userFunction + '})(grouped)')
        } catch (ex) {
            alert("Error evaluating user function")
            throw ex
        }

        data = []
        $.each(grouped, function(label, points) {
            data.push({
                "label": label,
                "data": points
            })
        })
    }

    // Plot data
    var plot = $.plot(placeholder, data, {
        xaxis: {
            tickFormatter: tickFormatter
        },
        legend: {
            show: true,
            container: legendContainer,
            labelFormatter: function(label, series) {
                return '<span id="' + label + '-current-baseline"></span> ' + label
            }
        },
        series: {
        },
        grid: {
            clickable: true,
            hoverable: true,
            markings: [
                { xaxis: { from: baselineCollectionTime, to: baselineCollectionTime }, color: "#000", lineWidth: 1 },
                { xaxis: { from: currentCollectionTime, to: currentCollectionTime}, color: "#000", lineWidth: 1 }
            ]
        }
    })

    // Get overall ratios for plotted series
    $(".metrics-checkbox").each(function(i, metric) {
        if (metric.checked) {
            var metricName = metric.getAttribute("value")

            var baselineUrl = addFixedDimensions('/metrics/'
                + encodeURIComponent($("#collections").val()) + '/'
                + baselineCollectionTime + '/'
                + (baselineCollectionTime + timeWindow), dimensionValues)

            var currentUrl = addFixedDimensions('/metrics/'
                + encodeURIComponent($("#collections").val()) + '/'
                + currentCollectionTime + '/'
                + (currentCollectionTime + timeWindow), dimensionValues)

            $.get(baselineUrl, function(baselineData) {
                $.get(currentUrl, function(currentData) {
                    var baseline = baselineData[0]["metricValues"][metricName]
                    var current = currentData[0]["metricValues"][metricName]
                    var ratio = baseline > 0 ? (current - baseline) / baseline : -1
                    $("#" + metricName + "-current-baseline").html('(' + (ratio * 100).toFixed(2) + '% w/' + $("#baseline").val() + 'w)')
                })
            })
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
    }).appendTo('body')

    // Hover handler
    placeholder.bind('plothover', function(event, pos, item) {
        if (item) {
            time = item.datapoint[0].toFixed(2)
            value = item.datapoint[1].toFixed(2)

            $("#tooltip").html(value + ' @ (' + tickFormatter(time) + ")")
                         .css({ top: item.pageY + 5, left: item.pageX + 5 })
                         .fadeIn(200)
        } else {
            $('#tooltip').hide()
        }
    })

    // Click handler
    placeholder.bind('plotclick', function(event, pos, item) {
        if (item) {
            date = new Date(collectionTimeToMillis(item.datapoint[0]))
            $("#date-picker").val(formatDatePickerInput(date))
            $("#spinner").val(formatSpinnerInput(date))
        }
    })
}

function fixDimension() {
    var name = this.getAttribute("dimension")
    var value = this.text == EMPTY_STRING ? '' : this.text

    $("#dimensions > li").each(function(i, dimension) {
        if (dimension.getAttribute("name") === name) {
            dimension.setAttribute("value", value)
        }
    })

    refreshBreadCrumbs()

    doQuery()
}

function relaxDimension() {
    var tokens = this.innerHTML.split(":")
    var name = tokens[0]

    $("#dimensions > li").each(function(i, dimension) {
        if (dimension.getAttribute("name") === name) {
            dimension.setAttribute("value", "*")
        }
    })

    refreshBreadCrumbs()

    doQuery()
}

function refreshBreadCrumbs() {
    $("#fixed-dimensions").empty()

    var breadCrumbs = $("<ul></ul>", {
        class: 'breadcrumbs'
    })

    var someFixed = false

    $("#dimensions > li").each(function(i, dimension) {
        var name = dimension.getAttribute("name")
        var value = dimension.getAttribute("value")
        if (value !== "*") {
            breadCrumbs.append($("<li></li>", {
                text: name + ":" + (value ? value : EMPTY_STRING)
            }).click(relaxDimension))
            someFixed = true
        }
    })

    if (someFixed) {
        $("#fixed-dimensions").append(breadCrumbs)
    }
}

/**
 * Main
 */
$(document).ready(function() {
    loadInputComponents()
    loadBaseline()
    loadConfig()
})