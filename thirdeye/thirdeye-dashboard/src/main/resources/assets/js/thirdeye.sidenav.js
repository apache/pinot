$(document).ready(function() {
    var path = parsePath(window.location.pathname)

    // Load current time zone
    $("#sidenav-timezone").html(" (" + getLocalTimeZone() + ")")

    // Load existing metrics selection / function
    if (path.metricFunction) {
      var metricFunctionObj = parseMetricFunction(path.metricFunction)
      $.each(metricFunctionObj.functions, function(i, func) {
        if (func.name == 'AGGREGATE') {
          $("#sidenav-aggregate-size").val(func.size)
          $("#sidenav-aggregate-unit").val(func.unit)
        }
        // TODO moving average, more...
      })

      $(".sidenav-metric").each(function(i, checkbox) {
        var checkboxObj = $(checkbox)
        if ($.inArray(checkboxObj.val(), metricFunctionObj.metrics) >= 0) {
          checkboxObj.attr("checked", "checked")
        }
      })
    }

    // Load existing date / time
    if (path.currentMillis) {
        var aggregateMillis = toMillis($("#sidenav-aggregate-size").val(), $("#sidenav-aggregate-unit").val())
        var currentDateTime = moment(parseInt(path.currentMillis) - aggregateMillis)
        var dateString = currentDateTime.format("YYYY-MM-DD")
        var timeString = currentDateTime.format("HH:mm")
        $("#sidenav-date").val(dateString)
        $("#sidenav-time").val(timeString)

        var baselineDateTime = moment(parseInt(path.baselineMillis))
        var diffMillis = currentDateTime.valueOf() - baselineDateTime.valueOf()
        var diffDescriptor = describeMillis(diffMillis)
        $("#sidenav-baseline-size").val(diffDescriptor.size)
        $("#sidenav-baseline-unit").val(diffDescriptor.sizeMillis)
    }

    $("#sidenav-submit").click(function(event) {
        event.preventDefault()

        // Clear any existing alert
        var errorAlert = $("#sidenav-error")
        var errorMessage = $("#sidenav-error p")
        errorMessage.empty()

        // Metric(s)
        var metrics = []
        $(".sidenav-metric").each(function(i, checkbox) {
            var checkboxObj = $(checkbox)
            if (checkboxObj.is(':checked')) {
                metrics.push(checkboxObj.val())
            }
        })

        if (metrics.length == 0) {
            errorMessage.html("Must provide at least one metric")
            errorAlert.fadeIn(100)
            return
        }
        
        // Date
        var date = $("#sidenav-date").val()
        if (!date) {
            errorMessage.html("Must provide date")
            errorAlert.fadeIn(100)
            return
        }

        // Time
        var time = $("#sidenav-time").val()
        if (!time) {
            errorMessage.html("Must provide time")
            errorAlert.fadeIn(100)
            return
        }

        // Baseline
        var baselineSize = parseInt($("#sidenav-baseline-size").val())
        var baselineUnit = parseInt($("#sidenav-baseline-unit").val())

        // DateTimes
        var current = moment(date + " " + time)
        var baseline = moment(current.valueOf() - (baselineSize * baselineUnit))
        var currentMillisUTC = current.utc().valueOf()
        var baselineMillisUTC = baseline.utc().valueOf()

        // Aggregate
        var aggregateSize = parseInt($("#sidenav-aggregate-size").val())
        var aggregateUnit = $("#sidenav-aggregate-unit").val()
        var aggregateMillis = toMillis(aggregateSize, aggregateUnit)

        if ((currentMillisUTC - baselineMillisUTC) % aggregateMillis != 0) {
            errorMessage.html("Time range is not divisible by aggregation unit")
            errorAlert.fadeIn(100)
            return
        }

        // Include an aggregation window past the current
        currentMillisUTC += aggregateMillis

        // TODO: More functions (e.g. moving average)

        // Metric function
        var metricFunction = metrics.join(",")
        metricFunction = "AGGREGATE_" + aggregateSize + "_" + aggregateUnit + "(" + metricFunction + ")"

        // Path
        var path = parsePath(window.location.pathname)
        path.metricFunction = metricFunction
        path.metricViewType = path.metricViewType == null ? "INTRA_DAY" : path.metricViewType
        path.dimensionViewType = path.dimensionViewType == null ? "HEAT_MAP" : path.dimensionViewType
        path.baselineMillis = baselineMillisUTC
        path.currentMillis = currentMillisUTC
        var dashboardPath = getDashboardPath(path)
        errorAlert.hide()
        window.location.pathname = dashboardPath
    })
})
