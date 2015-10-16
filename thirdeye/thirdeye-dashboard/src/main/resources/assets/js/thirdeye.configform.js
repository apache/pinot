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
    var queryParams = getQueryParamValue(window.location.search);

    $(".baseline-aggregate").click(function(){
        var aggregateUnit = $(".baseline-aggregate.uk-active").attr("unit")

    })

    $(".time-input-form-submit").click(function(event) {

        event.preventDefault()

        // Clear any existing alert
        var errorAlert = $("#time-input-form-error")
        var errorMessage = $("#time-input-form-error > p")
        errorMessage.empty()

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

        // Aggregate  todo:
        var aggregateSize = 1
        var aggregateUnit = $(".baseline-aggregate.uk-active").attr("unit")

        // Date
        var current = moment.tz(date, timezone)
        //Baseline is the user selected baseline or 1 week prior to current
        var baseline = baselineDate ? moment.tz(baselineDate, timezone) :  moment(current.valueOf() - (604800000))
        var currentMillisUTC = current.utc().valueOf()
        var baselineMillisUTC = baseline.utc().valueOf()

        // Metric(s)  todo: take the metrics from the URI instead of the sidenav
        var metrics = []
        $(".sidenav-metric").each(function(i, checkbox) {
            var checkboxObj = $(checkbox)
            if (checkboxObj.is(':checked')) {
                metrics.push("'" + checkboxObj.val() + "'")
            }
        });

        // Derived metric(s) todo: take the metrics from the URI instead of the sidenav
        $("#sidenav-derived-metrics-list").find(".uk-form-row").each(function(i, row) {
            var type = $(row).find(".derived-metric-type").find(":selected").val()
            var args = []
            $(row).find(".derived-metric-arg").each(function(j, arg) {
                args.push("'" + $(arg).find(":selected").val() + "'")
            })
            metrics.push(type + '(' + args.join(',') + ')')
        });

        // Metric function
        var metricFunction = metrics.join(",")

        // Moving average
        if ($("#moving-average-size").val() != "") {
            var movingAverageSize = $("#moving-average-size").val()
            var movingAverageUnit = "DAYS"
            metricFunction = "MOVING_AVERAGE_" + movingAverageSize + "_" + movingAverageUnit + "(" + metricFunction + ")"
        }

        // Aggregate
        metricFunction = "AGGREGATE_" + aggregateSize + "_" + aggregateUnit + "(" + metricFunction + ")"

        //Query Parameters
        if(queryParams.hasOwnProperty("")){
            delete queryParams[""]
        }

        // Path
        var path = parsePath(window.location.pathname)
        path.metricFunction = metricFunction
        path.metricViewType = path.metricViewType == null ? "INTRA_DAY" : path.metricViewType
        path.dimensionViewType = path.dimensionViewType == null ? "TABULAR" : path.dimensionViewType
        path.baselineMillis = baselineMillisUTC
        path.currentMillis = currentMillisUTC

        var dashboardPath = getDashboardPath(path)

        var params = parseHashParameters(window.location.hash)
        if(timezone !== getLocalTimeZone().split(' ')[1]) {
            params.timezone = timezone.split('/').join('-')
        }

        if (queryParams.funnels) {
            var funnels = decodeURIComponent(queryParams.funnels).split( ",")

            if (funnels.length > 0) {
                queryParams["funnels"] = funnels.join();
            }
        }

        errorAlert.hide()

        window.location = dashboardPath + encodeDimensionValues(queryParams) + encodeHashParameters(params)
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

                if($("#moving-average-size option[value='" + tokens[tokens.length - 2] + "']").length > 0){
                    $("#moving-average-size").val(tokens[tokens.length - 2])
                    $("#time-input-form-moving-average span").html($("#moving-average-size option[value='" + tokens[tokens.length - 2] + "']").html())
                }
            }
        }

        //On funnel heatmap preselect WoW
        var path = parsePath(window.location.pathname)
        if(path.dimensionViewType == "TABULAR"){
            $("#moving-average-size").html('<option class="uk-button" unit="WoW" value="7">WoW</option>')
            $("#moving-average-size").val("7")
            $("#time-input-form-moving-average span").html($("#moving-average-size option[value='7']").html())
        }
    }

    //Display current query value to none when no fixed element in the query
    if($("ul.dimension-combination li").length == 0){
        $("ul.dimension-combination").append("<li style='list-style-type: none;'>None</li>")
    }

    //Selecting the metric or dimension to display
    $(".section-selector").on("change", function(){
        $(".section-wrapper").hide();
        $(".section-wrapper[rel = '" +  $(".section-selector").val() + "' ]").show();
    })
})