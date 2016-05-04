//Form submit (Go button)
$("#main-view").on("click",".form-submit-btn",function(){

    var currentTab = $(this).attr("rel")

    //Close uikit dropdowns
    $("[data-uk-dropdown]").removeClass("uk-open");
    $("[data-uk-dropdown]").attr("aria-expanded", false);

    //Update hash
    //update hash.dateset
    var selectedDateset = $("#selected-dateset").attr("value");
    if(selectedDateset != "none" && selectedDateset != undefined && selectedDateset != ""){
        hash.dateset= selectedDateset;
    }

    //update hash.dashboard
    if(currentTab == "dashboard"){
       hash.dashboard = $("#selected-dashboard").attr("value");
    }

    //update hash.metrics
    delete hash.metrics
    var selectedMetrics = $(".view-metric-selector[rel='"+ currentTab +"'] .added-item");

    var numSelectedMetrics = selectedMetrics.length;
    var metricAry = [];
    for(var i = 0; i<numSelectedMetrics ;i++){
        metricAry.push( selectedMetrics[i].getAttribute("value"));
    }
    if(numSelectedMetrics > 0) {
        hash.metrics = metricAry.toString();
    }

    //update hash.dimensions
    delete  hash.dimensions
    var selectedDimensions = $(".view-dimension-selector[rel='"+ currentTab +"'] .added-item");
    var numSelectedDimensions = selectedDimensions.length;
    var dimensionAry = [];
    for(var i = 0; i<numSelectedDimensions ;i++){
        dimensionAry.push(selectedDimensions[i].getAttribute("value"));
    }
    if(numSelectedDimensions > 0){
        hash.dimensions = dimensionAry.toString();
    }
    hash.compareMode = $(".compare-mode[rel='"+ currentTab +"']").html().trim();


    //update hash.filters
    delete  hash.filters
    var filters = readFiltersAppliedInCurrentView(currentTab);
    hash.filters = encodeURIComponent(JSON.stringify(filters));



    //Validate form
    var errorMessage = $("#"+ hash.view +"-time-input-form-error p");
    var errorAlert = $("#"+ hash.view +"-time-input-form-error");

    //Validation

    //Check if dataset is selected
    if( !hash.hasOwnProperty("dataset")) {
        errorMessage.html("Please select a dataset.");
        errorAlert.fadeIn(100);
        return
    }

    //Check if dashboard is selected in case the current view is dashboard
    if( hash.hasOwnProperty("view") && hash.view == "dashboard") {
        if(!hash.hasOwnProperty("dashboard")){
            errorMessage.html("Please select a dashboard.");
            errorAlert.fadeIn(100);
            return
        }
    }

    //If the form has metric selector metric has to be selected
    if( $("#"+ hash.view +"-view-metric-selector").length > 0 ){
        if(!hash.hasOwnProperty("metrics")){
            errorMessage.html("Please select at least 1 metric.");
            errorAlert.fadeIn(100);
            return
        }
    }

    //Todo support timezone selection
    var timezone = "America/Los_Angeles";

    //Todo: support timezone selection

    // Aggregate
    //var aggregateSize = 1;
    var aggregateUnit = $(".baseline-aggregate.uk-active[rel='"+ currentTab +"']").attr("unit");
    hash.aggTimeGranularity = aggregateUnit;

    // DateTimes
    var currentStartDate = $(".current-start-date[rel='"+ currentTab +"']").text();
    if (!currentStartDate) {
        errorMessage.html("Must provide start date");
        errorAlert.fadeIn(100);
        return
    }

    var currentEndDate = $(".current-end-date[rel='"+ currentTab +"']").text();
    if (!currentEndDate) {
        errorMessage.html("Must provide end date");
        errorAlert.fadeIn(100);
        return
    }

    var currentStartTime = $(".current-start-time[rel='"+ currentTab +"']").text();
    var currentEndTime = $(".current-end-time[rel='"+ currentTab +"']").text();

    /* When time and timezone selection is supported use the following for date time selection
     var current = moment.tz(date + " " + time, timezone);*/
    var currentStart = moment.tz(currentStartDate + " " + currentStartTime, timezone);
    var currentStartMillisUTC = currentStart.utc().valueOf();
    var currentEnd = moment.tz(currentEndDate+ " " + currentEndTime, timezone);
    var currentEndMillisUTC = currentEnd.utc().valueOf();

    hash.currentStart = currentStartMillisUTC;
    hash.currentEnd = currentEndMillisUTC;


    //Check if baseline date range is present unless the viewtype is timeseries
    switch(hash.view) {
        case "timeseries":
            //remove the baseline date range
            $(".baseline-start-date[rel='"+ currentTab +"']").text("");
            $(".baseline-end-time[rel='"+ currentTab +"']").text("");
            $(".baseline-start-date[rel='"+ currentTab +"']").text("");
            $(".baseline-end-time[rel='"+ currentTab +"']").text("");
            $(".comparison-display[rel='"+ currentTab +"']").hide();
            break;
        case "compare":
        default: //when dashboard
            if($(".baseline-start-date[rel='"+ currentTab +"']").text().length == 0 || $(".baseline-end-date[rel='"+ currentTab +"']").text().length == 0){
                errorMessage.html("Provide the date range to be compared with " + $(".current-start-date[rel='"+ currentTab +"']").text() + " " +$(".current-start-time[rel='"+ currentTab +"']").text()+" - "+ $(".current-end-date[rel='"+ currentTab +"']").text() + " " + $(".current-end-time[rel='"+ currentTab +"']").text() +". Click the date input below.");
                errorAlert.fadeIn(100);
                return
            }
            break
    }


    if($(".baseline-start-date[rel='"+ currentTab +"']").text().length > 0){

        var baselineStartDate = $(".baseline-start-date[rel='"+ currentTab +"']").text();
        var baselineEndDate = $(".baseline-end-date[rel='"+ currentTab +"']").text();
        var baselineStartTime = $(".baseline-start-time[rel='"+ currentTab +"']").text();
        var baselineEndTime = $(".baseline-end-time[rel='"+ currentTab +"']").text();
        var baselineStart = moment.tz(baselineStartDate  + " " + baselineStartTime, timezone);
        var baselineStartMillisUTC = baselineStart.utc().valueOf();
        var baselineEnd = moment.tz(baselineEndDate  + " " + baselineEndTime, timezone);
        var baselineEndMillisUTC = baselineEnd.utc().valueOf();
        hash.baselineStart = baselineStartMillisUTC;
        hash.baselineEnd = baselineEndMillisUTC;
    }else{
        delete hash.baselineStart
        delete hash.baselineEnd
    }

    if(hash.hasOwnProperty("filters") && JSON.stringify(hash.filters) == "{}"){
        delete hash.filters
    }

    errorAlert.hide()

    //window.location.hash change triggers window.onhashchange event
    // that contains the ajax requests
    window.location.hash = encodeHashParameters(hash);

    //Disable form submit
    $("#" + hash.view + "-form-submit").prop("disabled", true);
});