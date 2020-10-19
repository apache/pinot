//Form submit (Go button)
$("#main-view").on("click", ".form-submit-btn", function () {
    formSubmit(this)
});

function formSubmit(target) {

    var currentTab = $(target).attr("rel")


    //Close uikit dropdowns
    $("[data-uk-dropdown]").removeClass("uk-open");
    $("[data-uk-dropdown]").attr("aria-expanded", false);
    $(".uk-dropdown").hide();


    /*Update hash*/
    //update hash.dataset
    var selectedDataset = $("#" + currentTab + "-selected-dataset").attr("value");
    if (selectedDataset != "none" && selectedDataset != undefined && selectedDataset != "") {
        hash.dataset = selectedDataset;
    }

    //update hash.dashboard
    delete hash.dashboard
    if (currentTab == "dashboard") {
        hash.dashboard = $("#selected-dashboard").attr("value");
    }

    //update hash.metrics
    delete hash.metrics

    var selectedMetrics;
    if (currentTab == "anomalies") {
        selectedMetrics = $("#selected-metric").attr("value") ? $("#selected-metric").attr("value") : "";

        if (selectedMetrics != "") {
            hash.metrics = selectedMetrics;
        }
    } else {
        selectedMetrics = $(".view-metric-selector[rel='" + currentTab + "'] .added-item");

        var numSelectedMetrics = selectedMetrics.length;
        var metricAry = [];
        for (var i = 0; i < numSelectedMetrics; i++) {
            metricAry.push(selectedMetrics[i].getAttribute("value"));
        }
        if (numSelectedMetrics > 0) {
            hash.metrics = metricAry.toString();
        }

    }

    //update hash.dimensions
    delete  hash.dimensions
    var selectedDimensions = $(".view-dimension-selector[rel='" + currentTab + "'] .added-item");
    var numSelectedDimensions = selectedDimensions.length;
    var dimensionAry = [];
    for (var i = 0; i < numSelectedDimensions; i++) {
        dimensionAry.push(selectedDimensions[i].getAttribute("value"));
    }
    if (numSelectedDimensions > 0) {
        hash.dimensions = dimensionAry.toString();
    }

    //update hash.compareMode
    hash.compareMode = $(".compare-mode[rel='" + currentTab + "']").html().trim();


    //update hash.filters
    delete  hash.filters
    var filters = readFiltersAppliedInCurrentView(currentTab);
    hash.filters = encodeURIComponent(JSON.stringify(filters));


    //update hash.aggTimeGranularity
    //If the form has aggregate selector
    if ($(".baseline-aggregate[rel='" + currentTab + "']").length > 0) {
        // Aggregate
        //var aggregateSize = 1;
        var aggregateUnit = $(".baseline-aggregate.uk-active[rel='" + currentTab + "']").attr("unit");
        hash.aggTimeGranularity = aggregateUnit;
    }

    /* Validate form */

    var errorMessage = $("#" + hash.view + "-time-input-form-error p");
    var errorAlert = $("#" + hash.view + "-time-input-form-error");

    //Check if dataset is selected
    if (!hash.hasOwnProperty("dataset")) {
        errorMessage.html("Please select a dataset.");
        errorAlert.fadeIn(100);
        return
    }

    //Check if dashboard is selected in case the current view is dashboard
    if (hash.view == "dashboard") {
        if (!hash.dashboard) {
            errorMessage.html("Please select a dashboard.");

            errorAlert.attr("data-error-source", "dashboard-option");
            errorAlert.fadeIn(100);
            return
        }
    }

    //If the form has metric selector metric has to be selected
    if ($("#" + hash.view + "-view-metric-selector").length > 0) {
        if (!hash.hasOwnProperty("metrics")) {
            errorMessage.html("Please select at least 1 metric.");
            errorAlert.attr("data-error-source", "metric-option");
            errorAlert.fadeIn(100);
            return
        }
    }

    //If the form has single-metric selector metric has to be selected
    if ($("#" + hash.view + "-view-single-metric-selector").length > 0) {
        if ($("#selected-metric").attr("value") == "") {
            errorMessage.html("Please select a metric.");
            errorAlert.attr("data-error-source", "metric-option");
            errorAlert.fadeIn(100);
            return
        }
    }


    //Validate date time selection and update hash params: currentStart, currentEnd, baselineStart, baselineEnd
    var timezone = getTimeZone();

    //Todo: support timezone selection

    // DateTimes
    var currentStartDate = $(".current-start-date[rel='" + currentTab + "']").text();
    if (!currentStartDate) {
        errorMessage.html("Must provide start date");
        errorAlert.fadeIn(100);
        return
    }

    var currentEndDate = $(".current-end-date[rel='" + currentTab + "']").text();
    if (!currentEndDate) {
        errorMessage.html("Must provide end date");
        errorAlert.fadeIn(100);
        return
    }

    var currentStartTime = $(".current-start-time[rel='" + currentTab + "']").text();
    var currentEndTime = $(".current-end-time[rel='" + currentTab + "']").text();

    var currentStart = moment.tz(currentStartDate + " " + currentStartTime, timezone);
    var currentStartMillisUTC = currentStart.utc().valueOf();
    var currentEnd = moment.tz(currentEndDate + " " + currentEndTime, timezone);
    var currentEndMillisUTC = currentEnd.utc().valueOf();

    if (currentStartMillisUTC >= currentEndMillisUTC) {

        errorMessage.html("Please choose a start date that is earlier than the end date.");
        errorAlert.fadeIn(100);
        return

    }

    hash.currentStart = currentStartMillisUTC;
    hash.currentEnd = currentEndMillisUTC;

    //Check if baseline date range is present unless the viewtype is timeseries anomalies
    switch (hash.view) {
        case "anomalies":
        case "timeseries":
            break;
        case "compare":
        default: //when dashboard
            if ($(".baseline-start-date[rel='" + currentTab + "']").text().length == 0 || $(".baseline-end-date[rel='" + currentTab + "']").text().length == 0) {
                errorMessage.html("Provide the date range to be compared with " + $(".current-start-date[rel='" + currentTab + "']").text() + " " + $(".current-start-time[rel='" + currentTab + "']").text() + " - " + $(".current-end-date[rel='" + currentTab + "']").text() + " " + $(".current-end-time[rel='" + currentTab + "']").text() + ". Click the date input below.");
                errorAlert.fadeIn(100);
                return
            }
            break
    }

    if ($(".baseline-start-date[rel='" + currentTab + "']").text().length > 0) {

        var baselineStartDate = $(".baseline-start-date[rel='" + currentTab + "']").text();
        var baselineEndDate = $(".baseline-end-date[rel='" + currentTab + "']").text();
        var baselineStartTime = $(".baseline-start-time[rel='" + currentTab + "']").text();
        var baselineEndTime = $(".baseline-end-time[rel='" + currentTab + "']").text();
        var baselineStart = moment.tz(baselineStartDate + " " + baselineStartTime, timezone);
        var baselineStartMillisUTC = baselineStart.utc().valueOf();
        var baselineEnd = moment.tz(baselineEndDate + " " + baselineEndTime, timezone);
        var baselineEndMillisUTC = baselineEnd.utc().valueOf();
        hash.baselineStart = baselineStartMillisUTC;
        hash.baselineEnd = baselineEndMillisUTC;
    } else {
        delete hash.baselineStart
        delete hash.baselineEnd
    }

    if (hash.hasOwnProperty("filters") && JSON.stringify(hash.filters) == "{}") {
        delete hash.filters
    }

    errorAlert.hide()
    if (hash.view == "anomalies") {
        hash.rand = (Math.random() + "").substring(3, 6);
    }
    /* window.location.hash change triggers window.onhashchange event
     that contains the ajax requests */
    window.location.hash = encodeHashParameters(hash);

    //Disable form submit
    //$("#" + hash.view + "-form-submit").prop("disabled", true);
}