$(document).ready(function() {

    //Click on a funnel thumbnail will display it's content in the main funnel display section
    $("#funnel-thumbnails .funnel").click(function(){

        //Draw the selected thumbnail table and title in the main section
        $("#custom-funnel-section").html($(this).html())
        $("#custom-funnel-section h3").css("display", "inline")

        //Highlight currently selected thumbnail
        $("#funnel-thumbnails .funnel").removeClass("uk-panel-box")
        $(this).addClass("uk-panel-box")
    })


    //Assign background color value to each cells
    $(".heat-map-cell").each(function(i, cell) {
        var cellObj = $(cell)
        var value = parseFloat(cellObj.attr('value'))
        var absValue = Math.abs(value)

        if (value < 0) {
            cellObj.css('background-color', 'rgba(255,0,0,' + absValue + ')') // red
        } else {
            cellObj.css('background-color', 'rgba(0,0,255,' + absValue + ')') // blue
        }
    });


    $(".funnel-table-time").each(function(i, cell){
        var cellObj = $(cell)
        var currentTime = moment(cellObj.attr('currentUTC'))
        cellObj.html(currentTime.tz(tz).format('YYYY-MM-DD HH:mm z'))
    })

    //Preselect the 1st funnel
    $("#funnel-thumbnails .funnel:first-of-type").trigger("click")

        //Toggle Sunmmary and Details tabs,
    $(".funnel-tabs li").on("click", function(){
        if(!$(this).hasClass("uk-active")) {

            $(".details-cell").toggleClass("hidden")
            $('#custom-funnel-section .metric-label,#funnel-thumbnails  .metric-label').attr('colspan', function(index, attr){
                return attr == 3 ? null : 3;
            });
            $("#custom-funnel-section .funnel-table-time").css("width","110px")
        }
    })


    //Create funnel table header tooltip displaying the base metrics of the derived metric
    var path = parsePath(window.location.pathname)

    //Clicking the time cell will set current time in the URI to the selected time
    //$(".funnel-table-time").each(renderTimeCell)

    //Clicking heat-map-cell should fix the related metrics in the URI and set the current time to the related hour
    $("#custom-funnel-section").on("click", " .heat-map-cell", function(){
        var  columnIndex = $(this).parent().children().index($(this)) /3 + 1 ;
        var currentUTC = $("td:first-child", $(this).closest("tr")).attr("currentUTC")

        var funnelName = $("#custom-funnel-section h3:first-child").html().trim()
        var baseMetrics = $("tr:first-child th:nth-child(" + columnIndex + ")", $(this).closest("table")).attr("title")
        var metrics = []
        if(baseMetrics.indexOf("RATIO") >= 0){
            var metricNames = (baseMetrics.substring(6, baseMetrics.length - 1).split(","))
           for(var i = 0, len = metricNames.length; i < len;i++){
               var metric = metricNames[i]
               metrics.push("'" + metric + "'")
           }
        }else{
            metrics.push("'" + baseMetrics + "'")
        }



        // Metric function
        var metricFunction = metrics.join(",")
        var path = parsePath(window.location.pathname)
        var firstindex = path.metricFunction.indexOf("'");
        var lastindex = path.metricFunction.lastIndexOf("'");
        var previousMetricFunction = path.metricFunction
        var newMetricFunction = previousMetricFunction.substr(0, firstindex ) + metrics +  previousMetricFunction.substr( lastindex + 1, previousMetricFunction.length )

        path.metricFunction = newMetricFunction
        path.dimensionViewType = "HEAT_MAP"

        var baselineDiff = path.currentMillis - path.baselineMillis
        var currentMillis = moment.utc(currentUTC)
        path.currentMillis = currentMillis
        path.baselineMillis = currentMillis - baselineDiff
        var dashboardPath = getDashboardPath(path)

        //Funnels
        var queryParams = getQueryParamValue(window.location.search);
        if (queryParams.funnels) {
            var funnels = decodeURIComponent(queryParams.funnels).split( ",")
            if (funnels.length > 0) {
                queryParams["funnels"] = funnels.join();
            }
        }

        // Timezone
        var timezone = $("#sidenav-timezone").val()
        var params = {}
        if(timezone !== getLocalTimeZone().split(' ')[1]) {
            params.timezone = timezone.split('/').join('-')
        }

       window.location = dashboardPath + encodeDimensionValues(queryParams) + encodeHashParameters(params)
    })

    //Clicking a cell in the header should fix the related metrics in the query
    $("#custom-funnel-section .metric-label").click(function(){
        //Find the related metrics
        var funnelName = $("#custom-funnel-section h3:first-child").html().trim()
        var baseMetrics = $(this).attr("title")
        var metrics = []
        if(baseMetrics.indexOf("RATIO") >= 0){
            var metricNames = (baseMetrics.substring(6, baseMetrics.length - 1).split(","))
            for(var i = 0, len = metricNames.length; i < len;i++){
                var metric = metricNames[i]
                metrics.push("'" + metric + "'")
            }
        }else{
            metrics.push("'" + baseMetrics + "'")
        }

        //Create metric function
        var path = parsePath(window.location.pathname)

        var firstindex = path.metricFunction.indexOf("'");
        var lastindex = path.metricFunction.lastIndexOf("'");
        var previousMetricFunction = path.metricFunction
        var newMetricFunction = previousMetricFunction.substr(0, firstindex ) + metrics +  previousMetricFunction.substr( lastindex + 1, previousMetricFunction.length )
        path.metricFunction = newMetricFunction
        path.dimensionViewType = "MULTI_TIME_SERIES"

        var dashboardPath = getDashboardPath(path)

        //Funnels
        var queryParams = getQueryParamValue(window.location.search);
        if (queryParams.funnels) {
            var funnels = decodeURIComponent(queryParams.funnels).split( ",")

            if (funnels.length > 0) {
                queryParams["funnels"] = funnels.join();
            }
        }

        // Timezone
        var timezone = $("#sidenav-timezone").val()
        var params = {}
        if(timezone !== getLocalTimeZone().split(' ')[1]) {
            params.timezone = timezone.split('/').join('-')
        }
        window.location = dashboardPath + encodeDimensionValues(queryParams) + encodeHashParameters(params)

    })



    //Cumulative checkbox
    $("#funnel-cumulative").click(function() {
            $(".hourly-values").toggleClass("hidden")
            $(".cumulative-values").toggleClass("hidden")
    })


});