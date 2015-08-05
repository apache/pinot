$(document).ready(function() {
    var path = parsePath(window.location.pathname)

    // Derived metrics
    $("#sidenav-derived-metrics-add").click(function(event) {
        event.preventDefault()

        var derivedMetrics = [
            { type: "RATIO", numArgs: 2 }
            // TODO: More
        ]

        var controls = $("<div></div>", {
            class: "uk-form-row uk-form-controls uk-panel uk-panel-box"
        })
        $("#sidenav-derived-metrics-list").append(controls)

        // Close button
        var exit = $("<a></a>", { href: "#" }).append($("<i></i>", { class: "uk-icon-close" }))
        var badge = $("<div></div>", { class: "uk-panel-badge" }).append(exit)
        exit.click(function(event) {
            event.preventDefault()
            controls.remove()
        })
        controls.append(badge)
        
        // Function type select
        var select = $("<select></select>", { class: "derived-metric-type" })
        controls.append(select)
        $.each(derivedMetrics, function(i, derivedMetric) {
            var option = $("<option></option>", {
                value: derivedMetric.type,
                html: derivedMetric.type
            })
            option.attr('idx', i)
            if (i == 0) {
                option.attr('selected', 'selected')
            }
            select.append(option)
        })

        var inputs = $("<div></div>")
        controls.append(inputs)
        select.change(function() {
            inputs.empty()
            var selected = select.find(':selected')
            var idx = $(selected).attr('idx')
            var derivedMetric = derivedMetrics[idx]

            for (var i = 0; i < derivedMetric.numArgs; i++) {
                var metricSelect = $("<select></select>", {
                    id: derivedMetric.type + '-arg' + i,
                    class: 'derived-metric-arg'
                })

                $(".sidenav-metric").each(function(j, metric) {
                    var metricName = $(metric).val()
                    var option = $("<option></option>", {
                        value: metricName,
                        html: metricName
                    })
                    if (i == j) {
                        option.attr('selected', 'selected')
                    }
                    metricSelect.append(option)
                })

                inputs.append(metricSelect)
            }
        })

        select.trigger('change')
    })

    // Moving average toggle
    $("#sidenav-moving-average").change(function() {
        var thisObj = $(this)
        if (thisObj.is(':checked')) {
            $("#sidenav-moving-average-controls").fadeIn(100)
        } else {
            $("#sidenav-moving-average-controls").hide()
        }
    })


    // Generate Timezone drop down
    var timezones = moment.tz.names()
    var currentTimeZone = getTimeZone()
    $.each(timezones, function(idx, tz){
        $("#sidenav-timezone").append('<option value="' + tz + '">' + tz + '</option>')
        if(tz === currentTimeZone) {
            $("#sidenav-timezone option:last-child").prop('selected', 1)
        }
    })

    //Display min-,max-data-time in the selected time zone
    //using moment.js methods, documentation: http://momentjs.com/timezone/docs/
    var selectedTimeZone = $("#sidenav-timezone").val()
    var maxDateTimeInUTC = moment.tz(parseInt($("#sidenav-max-time").attr('millis')), "UTC" )
    var maxDateTimeInSelectedTz = maxDateTimeInUTC.tz(selectedTimeZone).format("YYYY-MM-DD HH:mm z")
    $("#sidenav-max-time").html(maxDateTimeInSelectedTz)

    var minDateTimeInUTC = moment.tz(parseInt($("#sidenav-min-time").attr('millis')), "UTC" )
    var minDateTimeInSelectedTz = minDateTimeInUTC.tz(selectedTimeZone).format("YYYY-MM-DD HH:mm z")
    $("#sidenav-min-time").html(minDateTimeInSelectedTz)

    // Load existing metrics selection / function
    if (path.metricFunction) {
      var metricFunctionObj = parseMetricFunction(decodeURIComponent(path.metricFunction))

      // Always start at AGGREGATE
      var tokens = metricFunctionObj.name.split("_")
      $("#sidenav-aggregate-size").val(tokens[tokens.length - 2])
      $("#sidenav-aggregate-unit").val(tokens[tokens.length - 1])

      // May have applied moving average as well
      var firstArg = metricFunctionObj.args[0]
      if (typeof(firstArg) === 'object') {
        if (firstArg.name && firstArg.name.indexOf("MOVING_AVERAGE") >= 0) {
          metricFunctionObj = firstArg
          var tokens = metricFunctionObj.name.split("_")
          $("#sidenav-moving-average").attr('checked', 'checked')
          $("#sidenav-moving-average-controls").fadeIn(100)
          $("#sidenav-moving-average-size").val(tokens[tokens.length - 2])
          $("#sidenav-moving-average-unit").val(tokens[tokens.length - 1])
        }
      }

      // Rest are metrics (primitive and derived)
      var primitiveMetrics = []
      var derivedMetrics = []
      $.each(metricFunctionObj.args, function(i, arg) {
        if (typeof(arg) === 'string') {
            primitiveMetrics.push(arg)
        } else {
            derivedMetrics.push(arg)
        }
      })

      // Rest can be assumed to be plain args or derived metrics
      $(".sidenav-metric").each(function(i, checkbox) {
        var checkboxObj = $(checkbox)
        if ($.inArray(checkboxObj.val(), primitiveMetrics) >= 0) {
          checkboxObj.attr("checked", "checked")
        }
      })

      // Add derived metrics
      $.each(derivedMetrics, function(i, derivedMetric) {
        $("#sidenav-derived-metrics-add").trigger('click')
        var metricElements = $("#sidenav-derived-metrics-list .uk-form-row")
        var metricElement = $(metricElements[metricElements.length - 1])
        metricElement.find(".derived-metric-type").val(derivedMetric.name)
        $.each(derivedMetric.args, function(j, arg) {
            var input = $("#" + derivedMetric.name + "-arg" + j)
            input.val(arg)
        })
      })
    }

    // Load existing date / time
    if (path.currentMillis) {
        var currentDateTime = moment(parseInt(path.currentMillis))
        var dateString = currentDateTime.format("YYYY-MM-DD")
        var timeString = currentDateTime.format("HH:mm")
        $("#sidenav-date").val(dateString)
        $("#sidenav-time").val(timeString)

        var baselineDateTime = moment(parseInt(path.baselineMillis))
        var diffMillis = currentDateTime.valueOf() - baselineDateTime.valueOf()
        var diffDescriptor = describeMillis(diffMillis)
        $("#sidenav-baseline-size").val(diffDescriptor.size)
        $("#sidenav-baseline-unit").val(diffDescriptor.sizeMillis)
    } else {
        // Start at latest loaded time
        var aggregateMillis = toMillis($("#sidenav-aggregate-size").val(), $("#sidenav-aggregate-unit").val())
        var latestDateTime = moment(parseInt($("#sidenav-max-time").attr('millis'))-aggregateMillis);
        var dateString = latestDateTime.format("YYYY-MM-DD")
        var timeString = latestDateTime.format("HH:mm");
        $("#sidenav-date").val(dateString)
        $("#sidenav-time").val(timeString)
    }

    // Select the first metric if none selected
    var oneChecked = false
    $(".sidenav-metric").each(function(i, elt) {
        oneChecked = oneChecked || $(elt).attr('checked') == 'checked'
    })
    if (!oneChecked) {
      $($(".sidenav-metric")[0]).attr('checked', 'checked')
    }

    $(".custom-dashboard-link").click(function(event) {
        event.preventDefault()
        var dateTime = $("#sidenav-date").val().split('-').join('/')
        var href = $(this).attr('href')
        window.location.pathname = href + '/' + dateTime
    })

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

        // Derived metric(s)
        $("#sidenav-derived-metrics-list").find(".uk-form-row").each(function(i, row) {
            var type = $(row).find(".derived-metric-type").find(":selected").val()
            var args = []
            $(row).find(".derived-metric-arg").each(function(j, arg) {
                args.push($(arg).find(":selected").val())
            })
            metrics.push(type + '(' + args.join(',') + ')')
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

        // Timezone
        var timezone = $("#sidenav-timezone").val()

        // Baseline
        var baselineSize = parseInt($("#sidenav-baseline-size").val())
        var baselineUnit = parseInt($("#sidenav-baseline-unit").val())

        // DateTimes
        var current = moment.tz(date + " " + time, timezone)
        var baseline = moment(current.valueOf() - (baselineSize * baselineUnit))
        var currentMillisUTC = current.utc().valueOf()
        var baselineMillisUTC = baseline.utc().valueOf()

        // Aggregate
        var aggregateSize = parseInt($("#sidenav-aggregate-size").val())
        var aggregateUnit = $("#sidenav-aggregate-unit").val()
        var aggregateMillis = toMillis(aggregateSize, aggregateUnit)

        // Metric function
        var metricFunction = metrics.join(",")

        // Moving average
        if ($("#sidenav-moving-average").is(":checked")) {
            var movingAverageSize = $("#sidenav-moving-average-size").val()
            var movingAverageUnit = $("#sidenav-moving-average-unit").val()
            metricFunction = "MOVING_AVERAGE_" + movingAverageSize + "_" + movingAverageUnit + "(" + metricFunction + ")"
        }

        // Aggregate
        metricFunction = "AGGREGATE_" + aggregateSize + "_" + aggregateUnit + "(" + metricFunction + ")"


        // Path
        var path = parsePath(window.location.pathname)
        path.metricFunction = metricFunction
        path.metricViewType = path.metricViewType == null ? "INTRA_DAY" : path.metricViewType
        path.dimensionViewType = path.dimensionViewType == null ? "HEAT_MAP" : path.dimensionViewType
        path.baselineMillis = baselineMillisUTC
        path.currentMillis = currentMillisUTC
        var dashboardPath = getDashboardPath(path)
        var params = {}
        if(timezone !== getLocalTimeZone().split(' ')[1]) {
            params.timezone = timezone.split('/').join('-')
        }
        errorAlert.hide()
        window.location = dashboardPath + encodeHashParameters(params)
    })
})
