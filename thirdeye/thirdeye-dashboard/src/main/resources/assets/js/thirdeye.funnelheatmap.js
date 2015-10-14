$(document).ready(function() {

    //When a funnel thumbnail is clicked display it's content in the main funnel display section
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


    //Preselect the 1st funnel
    $("#funnel-thumbnails .funnel:first-of-type").trigger("click")

    //Toggle funnel and Metric Intraday tabs
    $(".funnel-tabs li").on("click", function(){
        if(!$(this).hasClass("uk-active")) {
            $("#custom-funnel-section").toggleClass("hidden")
            $("#funnel-thumbnails").toggleClass("hidden")
            $("#metric-table-area").toggleClass("hidden")
            $("#intra-day-buttons").toggleClass("hidden")
            $("#intra-day-table").toggleClass("hidden")
        }
    })


    //Create funnel table header tooltip displaying the base metrics of the derived metric
    var path = parsePath(window.location.pathname)
    var url = "/dashboard/" + path.collection + "/configs"
    //Add tooltip data to funnel column header
     /*$.ajax({
         url: url
     }).done(function(data){console.log("data",data)}).fail(function() { console.log( "error" )
     })*/

    //Hardcoding the abook funnels configs json till the ajax endpoint is working consistently
    var data = {"funnels":{"Member_2_Member_Full_Funnel":{"aliasToActualMetrics":{"import_rate":"RATIO(importsCompleted,submits)","impressions":"impressions","m2m_rl":"RATIO(memberResultsLandingImpressions,contactsSaved)","m2m_sent":"memberInvitationsSubmitted","submit":"RATIO(submits,impressions)","avg_suggested":"RATIO(suggestedMemberInvitations,memberResultsLandingImpressions)","save_rate":"RATIO(contactsSaved,importsCompleted)","avg_sent":"RATIO(memberInvitationsSubmitted,memberCreateEvents)","create":"RATIO(memberCreateEvents,memberResultsLandingImpressions)"},"visulizationType":"HEATMAP","name":"Member_2_Member_Full_Funnel","actualMetricNames":["impressions","RATIO(submits,impressions)","RATIO(importsCompleted,submits)","RATIO(contactsSaved,importsCompleted)","RATIO(memberResultsLandingImpressions,contactsSaved)","RATIO(suggestedMemberInvitations,memberResultsLandingImpressions)","RATIO(memberCreateEvents,memberResultsLandingImpressions)","RATIO(memberInvitationsSubmitted,memberCreateEvents)","memberInvitationsSubmitted"]},"Member_2_Guest_Full_Funnel":{"aliasToActualMetrics":{"import_rate":"RATIO(importsCompleted,submits)","impressions":"impressions","submit":"RATIO(submits,impressions)","avg_suggested":"RATIO(suggestedGuestInvitations,memberResultsLandingImpressions)","save_rate":"RATIO(contactsSaved,importsCompleted)","m2g_sent":"guestInvitationsSubmitted","avg_sent":"RATIO(guestInvitationsSubmitted,guestCreateEvents)","create":"RATIO(guestCreateEvents,memberResultsLandingImpressions)","m2g_rl":"RATIO(memberResultsLandingImpressions,contactsSaved)"},"visulizationType":"HEATMAP","name":"Member_2_Guest_Full_Funnel","actualMetricNames":["impressions","RATIO(submits,impressions)","RATIO(importsCompleted,submits)","RATIO(contactsSaved,importsCompleted)","RATIO(memberResultsLandingImpressions,contactsSaved)","RATIO(suggestedGuestInvitations,memberResultsLandingImpressions)","RATIO(guestCreateEvents,memberResultsLandingImpressions)","RATIO(guestInvitationsSubmitted,guestCreateEvents)","guestInvitationsSubmitted"]}},"dimension_groups":[],"collection":"abook"}
    var metricLabels = $("#custom-funnel-section .metric-label[data-uk-tooltip]")
    var funnelName = $("#custom-funnel-section h3:first-child").html().trim()

    for(var i = 0, len = metricLabels.length; i < len; i++){
        var baseMetrics = data["funnels"][funnelName]["actualMetricNames"][i]
        var metrics = []

        if (baseMetrics.indexOf("RATIO") >= 0) {
            metrics.push( baseMetrics.substring(6, baseMetrics.length - 1).split(',').join(" ") )
        }else {
            metrics.push( baseMetrics )
        }
        $(metricLabels[i]).attr("title", metrics )
    }

    //Display time in selected timezone
    $(".funnel-table-time").each(function(i, cell){
            var tz = getTimeZone();
            var cellObj = $(cell)
            var currentUTCMillis = path.currentMillis - 86400000 + ($(cell).attr("data-hour") * 3600000);
            var currentTime =  moment(currentUTCMillis)
            cellObj.html(currentTime.tz(tz).format('YYYY-MM-DD HH:mm:ss z'))
        }

    )

    //Clicking heat-map-cell should fix the related metrics in the URI and set the current time to the related hour
    $("#custom-funnel-section .heat-map-cell").click(function(){
        var  columnIndex = $(this).parent().children().index($(this));
        var hour = $("td:first-child", $(this).closest("tr")).attr("data-hour")

        //Hardcoding the abook funnels configs json till the ajax endpoint is working consistently

        var funnelName = $("#custom-funnel-section h3:first-child").html().trim()
        var baseMetrics = data["funnels"][funnelName]["actualMetricNames"][columnIndex-1]
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
        path.currentMillis = Number(path.currentMillis) + (hour * 3600000)
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

        var  columnIndex = $(this).parent().children().index($(this))
        if(columnIndex > 0){

            //Find the related metrics
            var funnelName = $("#custom-funnel-section h3:first-child").html().trim()
            var baseMetrics = data["funnels"][funnelName]["actualMetricNames"][columnIndex-1]
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
        }
    })

});