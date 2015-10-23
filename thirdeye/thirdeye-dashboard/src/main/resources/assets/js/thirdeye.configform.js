$(document).ready(function() {
    //Highlight active tab on navbar
    var view = parsePath(window.location.pathname).dimensionViewType == null ? "TABULAR" : parsePath(window.location.pathname).dimensionViewType
    $("#dashboard-output-nav a[view='" + view + "' ]").closest("li").addClass("uk-active")

    //Allow user to switch dimension view on the dropdown
    $(".section-selector").on("change", function(){
        $(".section-wrapper").hide()
        $(".section-wrapper[rel = '" +  $(".section-selector").val() + "' ]").show()
    })

    //Set default dimension view on Timeseries and the default metric view on Heatmap
    window.onload = load
    function load() {
        //Using settimeout since the table has minor css issues when created on a hidden parent
        // and flot.js has no callback function for drawing the data
        window.setTimeout(function(){$(".section-selector").trigger("change")}, 2000)
    }

    var path = parsePath(window.location.pathname)
    var queryParams = parseDimensionValuesAry(window.location.search)
    $(".baseline-aggregate").click(function(){
        var aggregateUnit = $(".baseline-aggregate.uk-active").attr("unit")

    })

    $(".time-input-form-submit").click(function(event) {

        event.preventDefault()

        // Clear any existing alert
        var errorAlert = $("#time-input-form-error")
        var errorMessage = $("#time-input-form-error > p")
        errorMessage.empty()

        // Metric(s)
        var metrics = []
        $(".panel-metric").each(function(i, checkbox) {
            var checkboxObj = $(checkbox)
            if (checkboxObj.is(':checked')) {
                metrics.push("'" + checkboxObj.val() + "'")
            }
        });

        // Metric function
        var metricFunction = metrics.join(",")
        // Date input field validation
        var date = $("#time-input-form-current-date").val()
        if (!date) {
            errorMessage.html("Must provide current date")
            errorAlert.fadeIn(100)
            return
        }

        // Date input field validation
        if($("#time-input-form-baseline-date").length > 0) {
            var baselineDate = $("#time-input-form-baseline-date").val()
            if (!baselineDate) {
                errorMessage.html("Must provide baseline date")
                errorAlert.fadeIn(100)
                return
            }
        }

        //Baseline aggregate checkbox validation
        if ($(".baseline-aggregate.uk-active").length == 0 ) {
            errorMessage.html("Please select a baseline: hours or days")
            errorAlert.fadeIn(100)
            return
        }

        // Timezone
        var timezone = getTimeZone()

        // Aggregate
        var aggregateSize = 1
        var aggregateUnit = $(".baseline-aggregate.uk-active").attr("unit")

        // Date
        var current = moment.tz(date, timezone)
        //Baseline is the user selected baseline or 1 week prior to current
        var baseline = baselineDate ? moment.tz(baselineDate, timezone) :  moment(current.valueOf() - (604800000))
        var currentMillisUTC = current.utc().valueOf()
        var baselineMillisUTC = baseline.utc().valueOf()

        // Derived metric(s) todo: take the derived metrics from the URI instead of the sidenav
        $("#sidenav-derived-metrics-list").find(".uk-form-row").each(function(i, row) {
            var type = $(row).find(".derived-metric-type").find(":selected").val()
            var args = []
            $(row).find(".derived-metric-arg").each(function(j, arg) {
                args.push("'" + $(arg).find(":selected").val() + "'")
            })
            metrics.push(type + '(' + args.join(',') + ')')
        });

        // Moving average
        if ($("#time-input-moving-average-size").length > 0 && $("#time-input-moving-average-size").val() != "") {
            var movingAverageSize = $("#time-input-moving-average-size").val()
            var movingAverageUnit = "DAYS"
            metricFunction = "MOVING_AVERAGE_" + movingAverageSize + "_" + movingAverageUnit + "(" + metricFunction + ")"
        }

        // Aggregate
        metricFunction = "AGGREGATE_" + aggregateSize + "_" + aggregateUnit + "(" + metricFunction + ")"

        // Path
        var path = parsePath(window.location.pathname)
        path.metricFunction = metricFunction
        path.metricViewType = path.metricViewType == null ? "INTRA_DAY" : path.metricViewType
        path.dimensionViewType = path.dimensionViewType == null ? "TABULAR" : path.dimensionViewType
        path.baselineMillis = baselineMillisUTC
        path.currentMillis = currentMillisUTC

        var dashboardPath = getDashboardPath(path)

        //Dimension Parameters
        var queryDimensionParams = []
        $(".panel-dimension-option").each(function(i, checkbox) {
            var checkboxObj = $(checkbox)
            if (checkboxObj.is(':checked')) {
                var dimension = checkboxObj.attr("dimension-name")
                var value = (checkboxObj.attr("dimension-value") == "-") ? "" : checkboxObj.attr("dimension-value")
                queryDimensionParams.push(dimension + "=" + value)
            }

        });
        var queryParams = parseDimensionValuesAry(window.location.search)

        if(queryParams.length > 0){
            var firstArg = queryParams[0]
            if(firstArg.indexOf("funnels") == 0){
               queryDimensionParams.unshift(queryParams[0])
            }
        }
        queryParams = queryDimensionParams

        //Hash parameters
        var params = parseHashParameters(window.location.hash)
        if(timezone !== getLocalTimeZone().split(' ')[1]) {
            params.timezone = timezone.split('/').join('-')
        }

        errorAlert.hide()
        window.location = dashboardPath + encodeDimensionValuesAry(queryParams) + encodeHashParameters(params)
    });


    //Load existing date selection
    var currentDateTime = moment(parseInt(path.currentMillis))
    var currentDateString = currentDateTime.format("YYYY-MM-DD")
    $("#time-input-form-current-date").val(currentDateString)

    //Load existing baseline date selection
    var baselineDateTime = moment(parseInt(path.baselineMillis))
    var baselineDateString = baselineDateTime.format("YYYY-MM-DD")
    $("#time-input-form-baseline-date").val(baselineDateString)

    // Load existing metrics selection / function
    if (path.metricFunction) {
        var metricFunctionObj = parseMetricFunction(decodeURIComponent(path.metricFunction))

        // Always start at AGGREGATE
        var tokens = metricFunctionObj.name.split("_")

        if($(".baseline-aggregate[unit='" + tokens[tokens.length - 1] +"'").length > 0){

            $(".baseline-aggregate[unit='" + tokens[tokens.length - 1] +"'").trigger("click")
        }

        // May have applied moving average as well
        var firstArg = metricFunctionObj.args[0]

        if (typeof(firstArg) === 'object') {
            if (firstArg.name && firstArg.name.indexOf("MOVING_AVERAGE") >= 0) {
                metricFunctionObj = firstArg
                var tokens = metricFunctionObj.name.split("_")

                if($("#time-input-moving-average-size option[value='" + tokens[tokens.length - 2] + "']").length > 0){
                    $("#time-input-moving-average-size").val(tokens[tokens.length - 2])
                    $("#time-input-form-moving-average span").html($("#time-input-moving-average-size option[value='" + tokens[tokens.length - 2] + "']").html())
                }
            }
        }

        // Rest are metrics (primitive and derived)
        var primitiveMetrics = []
        var derivedMetrics = []
        $.each(metricFunctionObj.args, function(i, arg) {
            if (typeof(arg) === 'string') {
                primitiveMetrics.push(arg.replace(/'/g, ""))
            } else {
                derivedMetrics.push(arg)
            }
        })

        // Rest can be assumed to be plain args or derived metrics
        for(var i= 0, len = primitiveMetrics.length; i< len; i++){
            $(".panel-metric[ value = " + primitiveMetrics[i] + "]").attr("checked", "checked")
        }

        /*$(".panel-metric ").each(function(i, checkbox) {
            var checkboxObj = $(checkbox)
            if ($.inArray(checkboxObj.val(), primitiveMetrics) >= 0) {
                checkboxObj.attr("checked", "checked")
            }
        })*/

        /*//On funnel heatmap preselect WoW
        var path = parsePath(window.location.pathname)
        if(path.dimensionViewType == "TABULAR"){
            $("#time-input-moving-average-size").html('<option class="uk-button" unit="WoW" value="7">WoW</option>')
            $("#time-input-moving-average-size").val("7")
            $("#time-input-form-moving-average span").html($("#time-input-moving-average-size option[value='7']").html())
        }*/
    }

    //Set selected dimesnion query selectors
    var queryParams = parseDimensionValuesAry(window.location.search)
    var firstArg = queryParams[0]

    if(firstArg.indexOf("funnels") == 0){
        queryParams.shift(queryParams[0])
    }

    for(var i = 0, len = queryParams.length; i < len; i++){
        var keyValue = queryParams[i].split("=")
        var dimensionName = keyValue[0]
        var dimensionValue = keyValue[1]
        $("[dimension-name = '" + dimensionName +"'][dimension-value = '" + dimensionValue +"']").attr("checked", "checked")
    }

    //Display current query value to none when no fixed element in the query
    if($("ul.filters-applied li").length == 0){
        $("ul.filters-applied").append("<li style='list-style-type: none;'>None</li>")
    }

    //Selecting the metric or dimension to display
    $(".section-selector").on("change", function(){
        $(".section-wrapper").hide();
        $(".section-wrapper[rel = '" +  $(".section-selector").val() + "' ]").show();
    })

    $("#time-input-metrics").click(function(){
        $("#time-input-metrics-panel").toggleClass("hidden")
    })
})