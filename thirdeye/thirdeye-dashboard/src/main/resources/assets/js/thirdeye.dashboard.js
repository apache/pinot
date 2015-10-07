$(document).ready(function() {
    $(".view-links a").each(function(i, link) {
        var linkObj = $(link)
        var linkType = linkObj.attr('type')
        linkObj.click(function() {
            var dashboardPath = parsePath(window.location.pathname)
            if (linkType === 'METRIC') {
                dashboardPath.metricViewType = linkObj.attr('view')
            } else if (linkType === 'DIMENSION') {
                dashboardPath.dimensionViewType = linkObj.attr('view')
            } else {
                throw 'Invalid link type ' + linkType
            }
            window.location.pathname = getDashboardPath(dashboardPath)
        })
    })

    $(".dimension-link").each(function(i, link) {
        var linkObj = $(link)
        var dimension = linkObj.attr('dimension')
        linkObj.click(function() {
            var dimensionValues = parseDimensionValues(window.location.search)
            delete dimensionValues[dimension]
            var updatedQuery = encodeDimensionValues(dimensionValues)
            window.location.search = updatedQuery
        })
    })

   /* Currently not using the left-, right buttons
   $("#time-nav-left").click(function() {
        var path = parsePath(window.location.pathname)
        var baselineMillis = parseInt(path.baselineMillis)
        var currentMillis = parseInt(path.currentMillis)
        var period = currentMillis - baselineMillis
        path.baselineMillis = baselineMillis - (period / 2)
        path.currentMillis = currentMillis - (period / 2)
        window.location.href = window.location.href.replace(window.location.pathname, getDashboardPath(path))
    })

    $("#time-nav-right").click(function() {
        var path = parsePath(window.location.pathname)
        var baselineMillis = parseInt(path.baselineMillis)
        var currentMillis = parseInt(path.currentMillis)
        var period = currentMillis - baselineMillis
        path.baselineMillis = baselineMillis + (period / 2)
        path.currentMillis = currentMillis + (period / 2)
        window.location.href = window.location.href.replace(window.location.pathname, getDashboardPath(path))
    })*/

    $(".collapser").click(function() {
        var $header = $(this);

        //getting the next element
        var $content = $header.next();

        //handle h2, h3, h3 headers
        if($("h2", $header).length > 0) {
            var parseTitle = $("h2", $header).html().split(" ")
            var title = parseTitle.splice(1, parseTitle.length).join(" ")
            var element = "h2"
        }else if($("h3", $header).length > 0){
            var parseTitle = $("h3", $header).html().split(" ")
            var title = parseTitle.splice(1, parseTitle.length).join(" ")
            var element = "h3"
        }else{
            var parseTitle = $("h4", $header).html().split(" ")
            var title = parseTitle.splice(1, parseTitle.length).join(" ")
            var element = "h4"
        }

        //open up the content needed - toggle the slide- if visible, slide up, if not slidedown.
        $content.slideToggle(800, function () {
            $header.html(function () {
                //change text based on condition
                return $content.is(":visible") ? '<' + element + ' style="color:#069;cursor:pointer">(-) ' + title + '</' + element + '>' : '<' + element + ' style="color:#069;cursor:pointer">(+) ' + title + '</' + element + '>';
            });
        });

    });

    var view = parsePath(window.location.pathname).dimensionViewType == null ? "TABULAR" : parsePath(window.location.pathname).dimensionViewType
    $("#dashboard-output-nav a[view='" + view + "' ]").closest("li").addClass("uk-active")

    //Allow user to switch dimension view on the dropdown
    $(".section-selector").on("change", function(){
        $(".section-wrapper").hide();
        $(".section-wrapper[rel = '" +  $(".section-selector").val() + "' ]").show();
    })

    //Set default dimension view on Timeseries and the default metric view on Heatmap
    window.onload = load
    function load() {
        window.setTimeout(function(){$(".section-selector").trigger("change")}, 2000);
    }

    var path = parsePath(window.location.pathname)
    var queryParams = getQueryParamValue(window.location.search);

    $(".time-input-form-submit").click(function(event) {

        event.preventDefault()

        // Clear any existing alert
        var errorAlert = $("#time-input-form-error")
        var errorMessage = $("#time-input-form-error > p")
        errorMessage.empty()

        // Date input field validation
        var date = $("#time-input-form-current-date").val()
        if (!date) {
            errorMessage.html("Must provide date")
            errorAlert.fadeIn(100)
            return
        }

        //Baseline checkbox validation
        if ($(".baseline-aggregate.uk-active").length == 0 ) {
            errorMessage.html("Please select a baseline: hours or days")
            errorAlert.fadeIn(100)
            return
        }

        if($(".moving-average-size.uk-active").length == 0 ){
            errorMessage.html("Please select a moving average size: WoW, Wo2W, Wo4W")
            errorAlert.fadeIn(100)
            return
        }

        // Timezone
        var timezone = getTimeZone()

        // Aggregate  todo:
        var aggregateSize = parseInt($(".baseline-aggregate.uk-active").val())
        var aggregateUnit = $("baseline-aggregate.uk-active").attr("unit")

        // Baseline
        var baselineSize = 1
        var baselineUnit = parseInt($(".baseline-aggregate.uk-active").val())

        // Date
        var current = moment.tz(date, timezone)
        var baseline = moment(current.valueOf() - (baselineSize * baselineUnit))
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
        if ($(".moving-average-size.uk-active").length > 0) {
            var movingAverageSize = $(".moving-average-size.uk-active").val()
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

    //Load existing date selection
      var baselineDateTime = moment(parseInt(path.baselineMillis))
      var baselineDateString = baselineDateTime.format("YYYY-MM-DD")
      $("#time-input-form-baseline-date").val(baselineDateString)

     // Load existing metrics selection metrics function if the value is present in the options of the funnel form
     var metricFunctionObj = parseMetricFunction(decodeURIComponent(path.metricFunction))

     // May have applied moving average as well
     var firstArg = metricFunctionObj.args[0]

     if (typeof(firstArg) === 'object') {
         if (firstArg.name && firstArg.name.indexOf("MOVING_AVERAGE") >= 0) {
         metricFunctionObj = firstArg
         var tokens = metricFunctionObj.name.split("_")

         if($(".moving-average-size[value='" + tokens[tokens.length - 2] + "']").length > 0){
            $(".moving-average-size[value='" + tokens[tokens.length - 2] + "']").trigger("click")
         }
     }
 }

     var baselineDateTime = moment(parseInt(path.baselineMillis))
     var diffMillis = currentDateTime.valueOf() - baselineDateTime.valueOf()
     var diffDescriptor = describeMillis(diffMillis)
     if($(".baseline-aggregate[value='" + diffDescriptor.sizeMillis +"'").length > 0){
         $(".baseline-aggregate[value='" + diffDescriptor.sizeMillis +"'").trigger("click")
     }
})
