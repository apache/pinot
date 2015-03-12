<div class="time-series-area"></div>

<div class="time-series-choices"></div>

<pre class="flot-json-data" style="display: none">
${flotJsonData}
</pre>

<script>
$(document).ready(function() {
    moment().utc().format()
})

function evaluateUdf(data) {
    var userFunction = $("#user-function").val()
    if (userFunction) {
        var grouped = {}
        $.each(data, function(i, series) {
            grouped[series["metricName"]] = series
        })

        try {
            grouped = eval('(function(series) {' + userFunction + '})(grouped)')
        } catch (ex) {
            alert("Error evaluating user function")
            throw ex
        }

        data = []
        $.each(grouped, function(metricName, series) {
            data.push(series)
        })
    }
    return data
}

function plotTimeSeries(parentName, minSeries, maxSeries, comparator) {
    var timeSeriesArea = $("#" + parentName + " .time-series-area")

    var placeholder = $('<div id="' + parentName + '-time-series-plot"></div>')
        .css('width', timeSeriesArea.width() + 'px')
        .css('height', '400px')

    timeSeriesArea.append(placeholder)

    // Config
    var plotConfig = {
        xaxis: {
            tickSize: 86400000, // 1 day
            tickFormatter: function(millis) {
                return moment.utc(millis).format("MM/DD")
            }
        },
        legend: {
            show: false
        },
        grid: {
            clickable: true,
            hoverable: true
        }
    }

    // Data
    var data = JSON.parse($("#" + parentName + " .flot-json-data").html())

    // Sort
    if (comparator) {
        data = data.sort(comparator)
    }

    // Filter
    if (minSeries != null && maxSeries != null) {
        var filteredData = []
        for (var i = minSeries; i < maxSeries; i++) {
            if (i < data.length) {
                filteredData.push(data[i])
            }
        }
        data = filteredData
    }

    // Fix colors
    var i = 0;
    $.each(data, function(i, elt) {
        elt.color = i;
        ++i;
    });

    // insert checkboxes
    var choiceContainer = $("#" + parentName + " .time-series-choices");
    $.each(data, function(i, elt) {
        choiceContainer.append("<br/><input type='checkbox' name='" + elt.label +
            "' id='id" + elt.label + "'></input>" +
            "<label for='id" + elt.label + "' id='label-id" + elt.label + "'>"
            + elt.label + "</label>");
    });

    var hashRoute = {}
    if (window.location.hash) {
        var hashKeyValuePairs = window.location.hash.substring(1).split("&")
        for (var i = 0; i < hashKeyValuePairs.length; i++) {
            var pair = hashKeyValuePairs[i].split("=")
            hashRoute[decodeURIComponent(pair[0])] = decodeURIComponent(pair[1])
        }
    }

    var selectedMetrics = hashRoute["selectedMetrics"]
        ? $.map(hashRoute["selectedMetrics"].split(","), function (elt) { return parseInt(elt) })
        : null
    if (hashRoute["selectedMetrics"] && hashRoute["selectedMetrics"].length > 0) {
        choiceContainer.find("input").each(function(i, elt) {
            if (!selectedMetrics || $.inArray(i, selectedMetrics) > -1) {
                $(elt).attr('checked', 'checked')
            }
        })
    }

    choiceContainer.find("input").each(function(i, ele) {
        if ($(ele).attr('name').indexOf($("#input-primary-metric").val()) != -1) {
            $(ele).attr('checked', 'checked')
        }
    })

    choiceContainer.find("input").click(plotAccordingToChoices);

    function plotAccordingToChoices() {

        var plotData = []

        var checkedSeries = {}
        choiceContainer.find("input:checked").each(function() {
            checkedSeries[$(this).attr("name")] = true
        })

        var selectedMetrics = []
        choiceContainer.find("input").each(function(i, elt) {
            if (elt.checked) {
                selectedMetrics.push(i)
            }
        })

        // Set hash route
        var hashRoute = {}
        if (window.location.hash) {
            var hashKeyValuePairs = window.location.hash.substring(1).split("&")
            for (var i = 0; i < hashKeyValuePairs.length; i++) {
                var pair = hashKeyValuePairs[i].split("=")
                hashRoute[decodeURIComponent(pair[0])] = decodeURIComponent(pair[1])
            }
        }
        hashRoute["selectedMetrics"] = selectedMetrics.join(",")
        delete hashRoute[""]
        window.location.hash = $.map(hashRoute, function(val, key) {
            return encodeURIComponent(key) + "=" + encodeURIComponent(val)
        }).join("&")

        $.each(data, function(i, elt) {
            if (checkedSeries[elt["label"]]) {
                plotData.push(elt)
            }
        })

        plotData = evaluateUdf(plotData)

        // Add end points
        var points = []
        $.each(plotData, function(i, series) {
            var start = series["data"][0]
            var end = series["data"][series["data"].length - 1]
            points.push({
                lines: { show: false },
                points: { show: true, radius: 3 },
                data: [start, end],
                color: series.color,
                metricName: series.metricName
            })
        })

        $.each(points, function(i, elt) {
            plotData.push(elt)
        })

        if (plotData.length > 0) {
            var plot = $.plot(placeholder, plotData, plotConfig)
            var series = plot.getData()
            for (var i = 0; i < series.length; i++) {
                $(document.getElementById("label-id" + series[i].label)).css('color', series[i].color)
            }
        }
    }

    $("#user-function-evaluate").click(plotAccordingToChoices)

    plotAccordingToChoices();

    // Tooltip
    $('<div id="' + parentName + '-tooltip"></div>').css({
        position: 'absolute',
        display: 'none',
        border: '1px solid #fdd',
        padding: '2px',
        'background-color': '#fee',
        opacity: 0.80
    }).appendTo(timeSeriesArea)

    // Hover handler
    placeholder.bind('plothover', function(event, pos, item) {
        if (item) {
            time = item.datapoint[0]
            value = item.datapoint[1]

            var dateString = moment.utc(time).format()

            $("#" + parentName + "-tooltip").html(item.series.metricName + "=" + value.toFixed(2) + ' @ (' + dateString + ")")
                         .css({ top: item.pageY + 5, left: item.pageX + 5 })
                         .fadeIn(200)
        } else {
            $('#' + parentName + '-tooltip').hide()
        }
    })

    // Click handler
    placeholder.bind('plotclick', function(event, pos, item) {
        if (item) {

            var dateTime = moment.utc(item.datapoint[0])
            var dateString = dateTime.format("MM/DD/YYYY")
            var timeString = dateTime.format("HH:mm")

            $("#input-date").val(dateString)
            $("#input-time").val(timeString)
        }
    })
}
</script>
