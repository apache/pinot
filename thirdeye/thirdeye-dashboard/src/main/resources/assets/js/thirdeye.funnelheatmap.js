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

    $(".heat-map-cell").each(function(i, cell){
        calcHeatMapCellBackground(cell)
    })



    /* Transform UTC time into user selected or browser's timezone */
    $(".funnel-table-time").each(function(i, cell){
        var dateTimeFormat = 'MM-DD HH:mm'
        transformUTCToTZTime(cell, dateTimeFormat )
    })

    //Translate the UTC time of the title to local / user selected timezone
    $(".funnel .metric-list[currentUTC]").each(function(i, label){
        var labelObj = $(label)
        var currentTime = moment(labelObj.attr('currentUTC'))
        var baselineTime = moment(labelObj.attr('baselineUTC'))
        labelObj.html("current = " + currentTime.tz(tz).format('YYYY-MM-DD') + " & baseline = " + baselineTime.tz(tz).format('YYYY-MM-DD'))
    })

    //Preselect the 1st funnel
    $("#funnel-thumbnails .funnel:first-of-type").trigger("click")

        //Toggle Sunmmary and Details tabs,
    $(".funnel-tabs li").on("click", function(){

            if(!$(this).hasClass("uk-active")) {

                $(".details-cell").toggleClass("hidden")
                $(".subheader").toggleClass("hidden")

                $('.metric-label').attr('colspan', function(index, attr){
                    return attr == 3 ? null : 3;
                });


            $("#custom-funnel-section .funnel-table-time").css("width","110px")
        }
    })


    //Clicking the time cell will set current time in the URI to the selected time
    //$(".funnel-table-time").each(renderTimeCell)

    //Clicking heat-map-cell should fix the related metrics and the derived metric of them in the URI and set the current time to the related hour
    $("#custom-funnel-section").on("click", " .heat-map-cell", function(){
        var  columnIndex = $(this).parent().children().index($(this)) /3 + 1 ;
        var currentUTC = $("td:first-child", $(this).closest("tr")).attr("currentUTC")
        var funnelName = $("#custom-funnel-section h3:first-child").html().trim()
        var baseMetrics = $("tr:first-child th:nth-child(" + columnIndex + ")", $(this).closest("table")).attr("title")
        var metrics = []
        //If the metric is a derived metric ie. ratio of 2 primitive metrics the metricfunction should contain the 2 metrics and the ratio
        if(baseMetrics.indexOf("RATIO") >= 0){
           var ratioToURI = []
           var metricNames = (baseMetrics.substring(6, baseMetrics.length - 1).split(","))
           for(var i = 0, len = metricNames.length; i < len;i++){
               var metric = metricNames[i]
               metrics.push("'" + metric + "'")
               ratioToURI.push("'" + metric + "'")
           }
            ratioToURI = "RATIO(" + ratioToURI + ")"
            metrics.push( ratioToURI )
        }else{
            metrics.push("'" + baseMetrics + "'")
        }


        // Metric function
        var metricFunction = metrics.join(",")
        var path = parsePath(window.location.pathname)
        //The metriclist in the metric function starts and ends with "'" ec=xcept when ratio is present in the metric list, that ends with ")"
        var firstindex = path.metricFunction.indexOf("'");
        //If the previous metric function contained RATIO we assume, that is in the last place in the metric list and will cut out the closing ")" of the RATIO() from the path
        var lastindex = (path.metricFunction.indexOf("RATIO") >= 0) ? path.metricFunction.indexOf(")") : path.metricFunction.lastIndexOf("'")  ;
        //Change the metric names in the metric function
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
    $("#custom-funnel-section").on("click", " .metric-label", function(){
        //Find the related metrics
        var funnelName = $("#custom-funnel-section h3:first-child").html().trim()
        var baseMetrics = $(this).attr("title")
        var metrics = []
        //If the metric is a derived metric ie. ratio of 2 primitive metrics the metricfunction should contain the 2 metrics and the ratio
        if(baseMetrics.indexOf("RATIO") >= 0){
            var ratioToURI = []
            var metricNames = (baseMetrics.substring(6, baseMetrics.length - 1).split(","))
            for(var i = 0, len = metricNames.length; i < len;i++){
                var metric = metricNames[i]
                metrics.push("'" + metric + "'")
                ratioToURI.push("'" + metric + "'")
            }
            ratioToURI = "RATIO(" + ratioToURI + ")"
            metrics.push( ratioToURI )
        }else{
            metrics.push("'" + baseMetrics + "'")
        }

        //Create metric function
        var path = parsePath(window.location.pathname)

        var firstindex = path.metricFunction.indexOf("'");
        //If the previous metric fnction contained RATIO we assume, that was the last element and will cut out the closing ")" of that from the path
        var lastindex = (path.metricFunction.indexOf("RATIO") >= 0) ? path.metricFunction.indexOf(")") : path.metricFunction.lastIndexOf("'");
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

});