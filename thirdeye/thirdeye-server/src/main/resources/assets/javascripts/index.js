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

    // Refresh heat map when input changes
    $("input[name=heat-map-option]:radio").change(function() {
        doQuery()
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
        $("#spinner").timespinner({
            step: collectionTimeToMillis(1) // the unit collection time
        })


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

function getDimensionsToQuery() {
    var dimensionsToQuery = []
    $("#dimensions > li").each(function(i, dimension) {
        var name = dimension.getAttribute("name")
        var value = dimension.getAttribute("value")
        if (value === "*") {
            dimensionsToQuery.push(name)
        }
    })
    return dimensionsToQuery
}

function getDimensionValues() {
    var dimensionValues = {}
    $("#dimensions > li").each(function(i, dimension) {
        var name = dimension.getAttribute("name")
        var value = dimension.getAttribute("value")
        dimensionValues[name] = value
    })
    return dimensionValues
}

function getTime() {
    var currentDate = getCurrentDate()
    var baselineDate = getBaselineDate(currentDate, $("#baseline").val())
    return {
        timeWindow: millisToCollectionTime(getTimeWindowMillis()),
        baselineCollectionTime: millisToCollectionTime(baselineDate.getTime()),
        currentCollectionTime: millisToCollectionTime(currentDate.getTime()),
        marginCollectionTime: millisToCollectionTime(TIME_SERIES_MARGIN)
    }
}

function generateBaseTimeSeriesUrl(time) {
    // Get selected metrics
    var selectedMetrics = []
    $(".metrics-checkbox").each(function(i, metric) {
        if (metric.checked) {
            selectedMetrics.push(metric.getAttribute("value"))
        }
    })

    var smoothingOption = $("input[name=smoothing-option]:checked", "#modal-metrics > form").val()

    var url = '/timeSeries/'
        + encodeURIComponent($("#collections").val()) + '/'
        + encodeURIComponent(selectedMetrics.join(',')) + '/'
        + (time.baselineCollectionTime - time.marginCollectionTime) + '/'
        + (time.currentCollectionTime + time.marginCollectionTime)

    if (smoothingOption === "moving-average") {
        url += '/movingAverage/' + $("#moving-average-window").val()
    } else {
        url += '/aggregate/' + time.timeWindow
    }

    if ($("#normalized")[0].checked) {
        url += '/normalized'
        var normalizationOption = $("input[name=normalization-type]:checked", "#modal-metrics > form").val()
        if (normalizationOption === 'funnel') {
            url += '/' + encodeURIComponent($("#metrics").val())
        }
    }

    return url
}

function generateBaseHeatMapUrl(time, dimensionName) {
    var smoothingOption = $("input[name=smoothing-option]:checked", "#modal-metrics > form").val()
    var heatMapOption = $("input[name=heat-map-option]:checked", "#modal-heat-map > form").val()

    var url = "/heatMap/"
        + encodeURIComponent(heatMapOption) + "/"
        + encodeURIComponent($("#collections").val()) + "/"
        + encodeURIComponent($("#metrics").val()) + "/"
        + encodeURIComponent(dimensionName)

    if (smoothingOption === "moving-average") {
        url += '/' + time.baselineCollectionTime +
              '/' + time.baselineCollectionTime +
              '/' + time.currentCollectionTime +
              '/' + time.currentCollectionTime +
              '/' + $("#moving-average-window").val()
    } else { // aggregation
        url += '/' + time.baselineCollectionTime +
              '/' + (time.baselineCollectionTime + time.timeWindow - 1) +
              '/' + time.currentCollectionTime +
              '/' + (time.currentCollectionTime + time.timeWindow - 1)
    }
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

    // Time
    var time = getTime()
    try {
      checkTime(time.baselineCollectionTime)
      checkTime(time.currentCollectionTime)
      if (time.timeWindow < 1) {
          alert("Time window must be >= 1")
          return
      }
    } catch (error) {
      alert(error)
      $("#image-placeholder").css('display', 'block')
      return
    }

    // Time series
    var url = addFixedDimensions(generateBaseTimeSeriesUrl(time), getDimensionValues())
    $.get(url, generateTimeSeriesChart)

    // Heat maps
    $.each(getDimensionsToQuery(), function(i, dimensionName) {
        var url = addFixedDimensions(generateBaseHeatMapUrl(time, dimensionName), getDimensionValues())

        $("#heat-maps").append($("<div></div>", {
            id: dimensionName
        }))

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
            'text': datum["dimensionValue"] ? datum["dimensionValue"] : EMPTY_STRING,
            'title': '(baseline=' + datum['baseline'] + ', current=' + datum['current'] + ')'
        })
        .click(fixDimension)

        var ratio = "<br/>" + (datum["ratio"] * 100).toFixed(2) + "%"

        cells.push(cell.append(link).append(ratio))
    })

    // Create heat map
    if (cells.length > 0) {
        var multipleTimeSeriesLink = $("<a></a>", {
            "data-reveal-id": "modal-multi-time-series",
            href: "#",
            'dimensionName': dimensionName
        }).append($("<img></img>", {
            src: '/assets/images/line_chart-32-inverted.png',
            class: 'heat-map-time-series-link'
        }))

        multipleTimeSeriesLink.click(function() {
            var dimensionName = $(this).attr('dimensionName')
            var dimensionValues = getDimensionValues()
            dimensionValues[dimensionName] = '!'
            var url = addFixedDimensions(generateBaseTimeSeriesUrl(getTime()), dimensionValues)
            $.get(url, function(data) {
                $("#multi-time-series").empty()

                var placeholder = $('<div id="multi-time-series-plot"></div>')
                    .css('width', $("#modal-multi-time-series").width() + 'px')
                    .css('height', '300px')
                    .css('margin-top', '20px')
                    .css('margin-bottom', '50px')

                var time = getTime()

                var legendContainer = $("<div id='multi-legend-container'></div>")
                $("#multi-time-series").append(placeholder)
                $("#multi-time-series").append(legendContainer)

                // Evaluate UDF on data
                data = evaluateUdf(data)

                // Alias to dimension value
                var dataByValue = {}
                $.each(data, function(i, datum) {
                    var value = datum['dimensionValues'][dimensionName]
                    value = value == '' ? EMPTY_STRING : value
                    datum['label'] = value
                    dataByValue[value] = datum
                })

                // Filter top k
                var cells = $("#" + dimensionName + " td a")
                var firstCell = $("#multi-series-first-cell").val()
                var lastCell = Math.min($("#multi-series-last-cell").val(), cells.length)
                var filteredData = []
                for (var i = firstCell - 1; i < lastCell; i++) {
                    var value = $(cells[i]).text()
                    filteredData.push(dataByValue[value])
                }

                var plotConfig = getDefaultPlotConfig(time)
                plotConfig['legend']['container'] = legendContainer

                var plot = $.plot(placeholder, filteredData, plotConfig)
            })
        })

        var table = $("<table></table>", {
            class: 'heatmap'
        }).append($("<caption></caption>", {
            text: dimensionName
        }).append(multipleTimeSeriesLink))

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

function evaluateUdf(data) {
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
    return data
}

function getDefaultPlotConfig(time) {
    var plotConfig = {
        xaxis: {
            tickFormatter: tickFormatter
        },
        legend: {
            show: true,
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
                { xaxis: { from: time.baselineCollectionTime, to: time.baselineCollectionTime }, color: "#000", lineWidth: 1 },
                { xaxis: { from: time.currentCollectionTime, to: time.currentCollectionTime}, color: "#000", lineWidth: 1 }
            ]
        }
    }

    // Y Axis scale
    if ($("#y-axis-scale")[0].checked) {
        var minValueY = $("#y-axis-scale-min").val()
        var maxValueY = $("#y-axis-scale-max").val()

        if (!minValueY || !maxValueY) {
            alert("Must specify min / max if scaling y-axis")
        } else {
            plotConfig['yaxis'] = {
                min: $("#y-axis-scale-min").val(),
                max: $("#y-axis-scale-max").val()
            }
        }
    }

    return plotConfig
}

function updateMetricRatios() {
    $(".metrics-checkbox").each(function(i, metric) {
        if (metric.checked) {
            var time = getTime()

            var metricName = metric.getAttribute("value")

            var baselineUrl = '/metrics/'
                + encodeURIComponent($('#collections').val()) + '/'
                + time.baselineCollectionTime

            var currentUrl = '/metrics/'
                + encodeURIComponent($('#collections').val()) + '/'
                + time.currentCollectionTime

            var smoothingOption = $("input[name=smoothing-option]:checked", "#modal-metrics > form").val()

            if (smoothingOption === 'moving-average') {
                baselineUrl += '/' + time.baselineCollectionTime + '/' + $("#moving-average-window").val()
                currentUrl += '/' + time.currentCollectionTime + '/' + $("#moving-average-window").val()
            } else { // aggregation
                baselineUrl += '/' + (time.baselineCollectionTime + time.timeWindow - 1)
                currentUrl += '/' + (time.currentCollectionTime + time.timeWindow - 1)
            }

            var normalizationOption = $("input[name=normalization-type]:checked", "#modal-metrics > form").val()
            if (normalizationOption === 'funnel') {
                baselineUrl += '/normalized/' + encodeURIComponent($("#metrics").val())
                currentUrl += '/normalized/' + encodeURIComponent($("#metrics").val())
            }

            var dimensionValues = getDimensionValues()

            baselineUrl = addFixedDimensions(baselineUrl, dimensionValues)
            currentUrl = addFixedDimensions(currentUrl, dimensionValues)

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
}

function generateTimeSeriesChart(data) {
    var placeholder = $('<div id="time-series-plot"></div>')
        .css('width', $(window).width() * 0.80 + 'px')
        .css('height', '300px')

    var time = getTime()

    var legendContainer = $("<div id='legend-container'></div>")
    $("#time-series").append(placeholder)
    $("#time-series").append(legendContainer)

    data = evaluateUdf(data)

    var plotConfig = getDefaultPlotConfig(time)
    plotConfig['legend']['container'] = legendContainer

    var plot = $.plot(placeholder, data, plotConfig)

    updateMetricRatios()

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
