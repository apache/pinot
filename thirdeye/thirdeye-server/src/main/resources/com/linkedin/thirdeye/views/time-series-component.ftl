<div class="time-series-area"></div>

<div class="time-series-choices"></div>

<pre class="flot-json-data" style="display: none">
${flotJsonData}
</pre>

<form style="display: none">
    <input type="hidden" id="start" name="start" value="${start?string["0"]}"/>
    <input type="hidden" id="end" name="end" value="${end?string["0"]}"/>
</form>

<script>
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

    // Time
    var start = $("#start").val()
    var end = $("#end").val()

    // Config
    var plotConfig = {
        xaxis: {
            mode: "time",
            minTickSize: [1, "day"],
            timeformat: "%m/%d/%y"
        },
        legend: {
            show: false
        },
        grid: {
            clickable: true,
            hoverable: true,
            markings: [
                { xaxis: { from: start, to: start }, color: "#000", lineWidth: 1 },
                { xaxis: { from: end, to: end }, color: "#000", lineWidth: 1 }
            ]
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
            "' checked='checked' id='id" + elt.label + "'></input>" +
            "<label for='id" + elt.label + "' id='label-id" + elt.label + "'>"
            + elt.label + "</label>");
    });

    choiceContainer.find("input").click(plotAccordingToChoices);

    function plotAccordingToChoices() {

        var plotData = []

        var checkedSeries = {}
        choiceContainer.find("input:checked").each(function() {
            checkedSeries[$(this).attr("name")] = true
        })

        $.each(data, function(i, elt) {
            if (checkedSeries[elt["label"]]) {
                plotData.push(elt)
            }
        })

        plotData = evaluateUdf(plotData)

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
            time = item.datapoint[0].toFixed(2)
            value = item.datapoint[1].toFixed(2)

            $("#" + parentName + "-tooltip").html(value + ' @ (' + new Date(parseInt(time)) + ")")
                         .css({ top: item.pageY + 5, left: item.pageX + 5 })
                         .fadeIn(200)
        } else {
            $('#' + parentName + '-tooltip').hide()
        }
    })

    // Click handler
    placeholder.bind('plotclick', function(event, pos, item) {
        if (item) {
            var dateTime = new Date(item.datapoint[0])
            var dateString = (dateTime.getMonth() + 1) + "/" + dateTime.getDate() + "/" + dateTime.getFullYear()
            var timeString = (dateTime.getHours() < 10 ? "0" + dateTime.getHours() : dateTime.getHours())
                + ":" + (dateTime.getMinutes() < 30 ? "00" : "30")

            $("#input-date").val(dateString)
            $("#input-time").val(timeString)
        }
    })
}
</script>