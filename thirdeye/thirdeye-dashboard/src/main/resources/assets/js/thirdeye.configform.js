$(document).ready(function() {
    //Highlight active tab on navbar
    var view = parsePath(window.location.pathname).dimensionViewType == null ? "TABULAR" : parsePath(window.location.pathname).dimensionViewType
    $("#dashboard-output-nav a[view='" + view + "' ]").closest("li").addClass("uk-active")

    //Allow user to switch dimension displayed on the dropdown
    $("#view-dimension-selector .section-selector").on("change", function(){
        $(".section-wrapper").hide()
        $(".section-wrapper[rel = '" +  $(".section-selector").val() + "' ]").show()
    })

    //Allow user to switch metric displayed on the dropdown
    $("#view-metric-selector .metric-section-selector").on("change", function(){
        $(".metric-section-wrapper").hide()
        $(".metric-section-wrapper[rel = '" +  $(".metric-section-selector").val() + "' ]").show()
    })

    //Cumulative checkbox
    $("#funnel-cumulative").click(function() {
        $(".hourly-values").toggleClass("hidden")
        $(".cumulative-values").toggleClass("hidden")
    })



    //If filters applied link is clicked remove it's value/s from the URI
    // so it won't be fixed in the query WHERE clause, reload the page with the new query
    $(".dimension-link").each(function(i, link) {
        var linkObj = $(link)
        var dimension = linkObj.attr('dimension')
        var value =  linkObj.attr('dimension-value')

        linkObj.click(function() {
            var values = []
            if(value.indexOf(" OR ") >= 0){
                var values = value.split(" OR ")
            }else{
                values.push(value)
            }

            var dimensionValues = parseDimensionValuesAry(window.location.search)
            for(var i = 0, len = values.length; i < len; i++) {

                var dimensionValue = dimension + "=" + values[i]
                if ( dimensionValues.indexOf(dimensionValue) > -1) {
                    dimensionValues.splice(dimensionValues.indexOf(dimensionValue), 1);
                }
            }
            var updatedQuery = encodeDimensionValuesAry(dimensionValues)
            window.location.search = updatedQuery
        })
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
        var baseline =  moment(current.valueOf() - $("#time-input-comparison-size").val() * 86400000)
        var currentMillisUTC = current.utc().valueOf()
        var baselineMillisUTC = baseline.utc().valueOf()

        // Moving average
        /*if ($("#time-input-moving-average-size").length > 0 && $("#time-input-moving-average-size").val() != "") {
            var movingAverageSize = $("#time-input-moving-average-size").val()
            var movingAverageUnit = "DAYS"
            metricFunction = "MOVING_AVERAGE_" + movingAverageSize + "_" + movingAverageUnit + "(" + metricFunction + ")"
        */

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
    var path = parsePath(window.location.pathname)
    var currentDateTime = moment(parseInt(path.currentMillis))
    var currentDateString = currentDateTime.format("YYYY-MM-DD")
    $("#time-input-form-current-date").val(currentDateString)

    // Load existing metrics selection / function
    if (path.metricFunction) {
        var metricFunctionObj = parseMetricFunction(decodeURIComponent(path.metricFunction))

        // Always start at AGGREGATE
        var tokens = metricFunctionObj.name.split("_")

        if($(".baseline-aggregate[unit='" + tokens[tokens.length - 1] +"'").length > 0){

            $(".baseline-aggregate[unit='" + tokens[tokens.length - 1] +"'").trigger("click")
        }

        // May have applied moving average as well
        /*var firstArg = metricFunctionObj.args[0]
        if (typeof(firstArg) === 'object') {
            if (firstArg.name && firstArg.name.indexOf("MOVING_AVERAGE") >= 0) {
                metricFunctionObj = firstArg
                var tokens = metricFunctionObj.name.split("_")

                if($("#time-input-moving-average-size option[value='" + tokens[tokens.length - 2] + "']").length > 0){
                    $("#time-input-moving-average-size").val(tokens[tokens.length - 2])
                    $("#time-input-form-moving-average span").html($("#time-input-moving-average-size option[value='" + tokens[tokens.length - 2] + "']").html())
                }
            }
        }*/

        // Rest are metrics
        var metrics = []

        $.each(metricFunctionObj.args, function(i, arg) {
             metrics.push(arg.replace(/'/g, ""))
        })
        for(var i= 0, len = metrics.length; i< len; i++){

            $(".panel-metric[ value = " + metrics[i] + "]").attr("checked", "checked")
        }

        //Set the comparison size to WoW / Wo2W / Wo4W based on the URI currentmillis - baselinemillis
        if($("#time-input-comparison-size").length > 0 ){
            var path = parsePath(window.location.pathname)
            var baselineDiff = path.currentMillis - path.baselineMillis
            var hasWoWProfile = [7,14,21,28].indexOf(baselineDiff / 86400000 ) > -1

            if(hasWoWProfile){
                var baselineDiffDays = (baselineDiff / 86400000 )
                $("#time-input-comparison-size").val(baselineDiffDays)
                $("span", $("#time-input-comparison-size").parent()).html($("#time-input-comparison-size option[value='" + baselineDiffDays + "']").html())
            }
        }
    }

    //Set selected dimesnion query selectors
    var queryParams = parseDimensionValuesAry(window.location.search)
  
    if(queryParams.length > 0) {
        var firstArg = queryParams[0]
        if (firstArg.indexOf("funnels") == 0) {
            queryParams.shift(queryParams[0])
        }
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

    //Selecting the metric to display
    $(".metric-section-selector").on("change", function(){
        $(".metric-section-wrapper").hide();
        $(".metric-section-wrapper[rel = '" +  $(".metric-section-selector").val() + "' ]").show();
    })

    //Selecting the dimension to display
    $(".section-selector").on("change", function(){
        $(".section-wrapper").hide();
        $(".section-wrapper[rel = '" +  $(".section-selector").val() + "' ]").show();
    })

    $("#time-input-metrics").click(function(){
        $("#time-input-metrics-panel").toggleClass("hidden")
    })


    //Set default dimension view on Timeseries and the default metric view on Heatmap
    window.onload = load
    function load() {
        //Using settimeout since the table has minor css issues when created on a hidden parent
        // and flot.js has no callback function for drawing the data
        window.setTimeout(function(){$(".metric-section-selector").trigger("change")}, 2000)
        window.setTimeout(function(){$(".section-selector").trigger("change")}, 2000)
    }
})