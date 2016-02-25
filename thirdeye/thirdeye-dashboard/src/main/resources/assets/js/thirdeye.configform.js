$(document).ready(function() {

    /* --- Headnav related code --- */
    var collectionsUrl =  "/dashboardConfig/collections"

    //Handlebars compile template
    var source_collections_template = $("#collections-template").html();
    var collections_template = Handlebars.compile(source_collections_template);

    $.ajax({
        url: collectionsUrl,
        method: 'GET',
        statusCode: {
            404: function() {
                $("#time-input-form-error").empty()
                var warning = $('<div></div>', { class: 'uk-alert uk-alert-warning' })
                warning.append($('<p></p>', { html: 'No data available dashboardConfig/collections' }))
                $("#time-input-form-error").append(warning)
            },
            500: function() {
                $("#time-input-form-error").empty()
                var error = $('<div></div>', { class: 'uk-alert uk-alert-danger' })
                error.append($('<p></p>', { html: 'Internal server error' }))
                $("#time-input-form-error").append(error)
            }
        }
    }).done(function(data){
        /* Handelbars template for collections dropdown */
        var result_collections_template = collections_template(data);
        $("#landing-collection").html(result_collections_template);

        //Display the selected collection name 
        var path = parsePath(window.location.pathname)
        $("#collection-name-display").html(path.collection)
        $(".collection-option[value ='" + path.collection + "']").attr('selected', 'selected')

        //When user changed collectionname enable Go (#landing-submit) button
        $("#landing-collection").change(function(){
            document.getElementById('landing-submit').disabled = false
        })


    })

    /* --- Headnav related event listeners --- */


    //Highlight active tab on navbar
    var view = parsePath(window.location.pathname).dimensionViewType == null ? "TABULAR" : parsePath(window.location.pathname).dimensionViewType
    $("#dashboard-output-nav a[view='" + view + "' ]").closest("li").addClass("uk-active")

    //On submit load the selected collection
    $("#landing-submit").click(function(event) {
        event.preventDefault()
        var collection = $("#landing-collection").val()
        window.location  = "/dashboard/" + collection;
    })

    /* --- Configform related event listeners  --- */

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

    //Allow user to switch dimension displayed on the dropdown
    $("#view-dimension-selector .dimension-section-selector").on("change", function(){
        $(".section-wrapper").hide()
        for(var i= 0, len=$(".dimension-section-selector").length; i < len; i++){
            var checkboxObj = $($(".dimension-section-selector")[i])
            if (checkboxObj.is(':checked')){
                $(".section-wrapper[rel = '" +  checkboxObj.attr("value") + "' ]").show()
            }
        }
    })

    //Allow user to switch metric displayed on the dropdown
    $("#view-metric-selector.metric-section-selector").on("change", function(){
        $(".metric-section-wrapper").hide()
        $(".metric-section-wrapper[rel = '" +  $(".metric-section-selector").val() + "' ]").show()
    })

    //Cumulative checkbox
    $("#funnel-cumulative").click(function() {
        $(".hourly-values").toggleClass("hidden")
        $(".cumulative-values").toggleClass("hidden")
        var path = parsePath(window.location.pathname)
        if(path.dimensionViewType  == "MULTI_TIME_SERIES"){
            var tableBodies = $("#dimension-contributor-area tbody")
            for(var i = 0, len = tableBodies.length; i < len; i++){
                sumColumn(tableBodies[i])
            }
        }
    })

    //Only display time selection option on Heat map till the backend will support time selection on funnel_heatmap.ftl and breakdown.ftl
    var path = parsePath(window.location.pathname)
    if(path.dimensionViewType  == "HEAT_MAP") {
        //Toggle Time inputfield based on hourly / daily granularity
        $("#time-input-form-gran-hours").click(function () {
            $("label, input, div", '#config-form-time-picker-box').animate({
                'width': '184px'}, 200, function () {
                $("label, input, i, div", '#config-form-time-picker-box').show()
            })
        })

        $("#time-input-form-gran-days").click(function () {
            $("label, input, div", '#config-form-time-picker-box').animate({
                'display': 'none'}, 200, function () {
                $("label, input, i, div", '#config-form-time-picker-box').hide()
            })
        })
    }
    //When any form field has changed enable Go (submit) button
    $('.panel-dimension-option , #time-input-comparison-size , #time-input-form-current-date, #time-input-form-current-time' ).change(enableFormSubmit)
    function enableFormSubmit(){
        document.getElementById('time-input-form-submit').disabled = false
    }






    //Form Submit
    $("#time-input-form-submit").click(function(event) {

        event.preventDefault()

        // Clear any existing alert
        var errorAlert = $("#time-input-form-error")
        var errorMessage = $("#time-input-form-error > p")
        errorMessage.empty()

        // Metric(s)
        var metrics = []
        var path = parsePath(window.location.pathname)
        //The metriclist in the metric function starts and ends with "'" except when ratio is present in the metric list, that ends with ")"
        var firstindex = path.metricFunction.indexOf("'");

        // Metric function
        var metricFunction = path.metricFunction.substr(firstindex)
        var lastindex = (path.metricFunction.indexOf("RATIO") >= 0) ? metricFunction.indexOf(")") : metricFunction.lastIndexOf("'") ;
        metricFunction = metricFunction.substr(0, lastindex + 1)

        // Date input field validation
        var date = $("#time-input-form-current-date").val()
        if (!date) {
            errorMessage.html("Must provide end date")
            errorAlert.fadeIn(100)
            return
        }

        // Time - if time inputfield present take it's value or send error message if no time selected  otherwise
         var time = ($("#time-input-form-current-time").css('display') == 'none') ?  "00:00" : $("#time-input-form-current-time").val()

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
        var current = moment.tz(date + " " + time, timezone)

        //Baseline is the user selected baseline or 1 week prior to current
        var baseline =  moment(current.valueOf() - $("#time-input-comparison-size").val() * 86400000)

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


    /* --- Load values from the URI --- */


    // Load existing date / time
    var path = parsePath(window.location.pathname)
    if (path.currentMillis) {
        var currentDateTime = moment(parseInt(path.currentMillis))
        var dateString = currentDateTime.format("YYYY-MM-DD")
        var timeString = currentDateTime.format("HH:mm")
        $("#time-input-form-current-date").val(dateString)
        $("#time-input-form-current-time").val(timeString)
    }
   /* //Load existing date selection
    var path = parsePath(window.location.pathname)
    var currentDateTime = moment(parseInt(path.currentMillis))
    var currentDateString = currentDateTime.format("YYYY-MM-DD")
    $("#time-input-form-current-date").val(currentDateString)*/

    // Load existing metrics selection / function
    var metricFunctionObj = parseMetricFunction(decodeURIComponent(path.metricFunction))

    // Always start at AGGREGATE
    var tokens = metricFunctionObj.name.split("_")
    if($(".baseline-aggregate[unit='" + tokens[tokens.length - 1] +"']").length > 0){
        $(".baseline-aggregate[unit='" + tokens[tokens.length - 1] +"']").trigger("click");
    }
    //When baseline-aggregate after the initial change on load enable Go (submit) button
    $('.baseline-aggregate:not(.uk-active)').click(enableFormSubmit);

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

    //If metric selector dropdown is present on the configform in the current view add it's options
    if($("#view-metric-selector").length > 0){

        // Metrics (primitive and derived)
        var metrics = []

        $.each(metricFunctionObj.args, function(i, arg) {
            if (typeof(arg) === 'string') {
               metrics.push(arg.replace(/'/g, ""))
           }else{
               metrics.push(arg.name + "(" + arg.args[0].replace(/'/g, "") + "," + arg.args[1].replace(/'/g, "") + ")")
           }
        })

        // plain args or derived metrics
        var optionsHTML = []
        for(var i= 0, len = metrics.length; i< len; i++){
            optionsHTML.push("<option value='" + metrics[i] + "'>" + metrics[i] + "</option>")
        }

        document.getElementById("view-metric-selector").innerHTML = optionsHTML.join('')


        //Set the comparison size to WoW / Wo2W / Wo4W based on the URI currentmillis - baselinemillis
        if($("#time-input-comparison-size").length > 0 ){
            var path = parsePath(window.location.pathname)
            var baselineDiff = path.currentMillis - path.baselineMillis
            var hasWoWProfile = [7,14,21,28].indexOf(baselineDiff / 86400000 -1)

            if(hasWoWProfile){
                var baselineDiffDays = (baselineDiff / 86400000 )

                $("#time-input-comparison-size").val(baselineDiffDays)
                $("span", $("#time-input-comparison-size").parent()).html($("#time-input-comparison-size option[value='" + baselineDiffDays + "']").html())
               
            }


        }
    }

    //Set selected dimension query selectors
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

    $($(".dimension-section-selector")[0]).trigger("click");
    //Set default dimension view on Timeseries and the default metric view on Heatmap
    window.onload = load
    function load() {
        //Using settimeout since the table has minor css issues when created on a hidden parent
        // and flot.js has no callback function for drawing the data
        window.setTimeout(function(){$(".metric-section-selector").trigger("change")}, 2000)
         }

})