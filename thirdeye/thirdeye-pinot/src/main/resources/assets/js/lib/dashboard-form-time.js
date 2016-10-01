/** Time-input-form related eventlisteners  **/


$("#main-view").on("click", ".baseline-aggregate", function () {
    selectAggregate(this)
});

//When any form field has changed enable Go (submit) button
$("#main-view").on("change",
        ".current-start-date, .current-start-time, " +
        ".baseline-start-date,.baseline-start-time, " +
        ".current-end-date,.current-end-time, " +
        ".baseline-end-date,.baseline-end-time",
    function () {
        enableFormSubmit()
    });

$("#main-view").on("change", ".current-date-range-selector", function () {
    selectCurrentDateRange(this)
});

$("#main-view").on("click", ".time-input-compare-checkbox", function () {
    toggleTimeComparison(this)
});

$("#main-view").on("change", ".compare-mode-selector", function () {
    selectBaselineDateRange(this)
});


$("#main-view").on("hide.uk.datepicker",
    ".time-range-selector-dropdown .current-start-date-input[data-uk-datepicker], " +
    ".time-range-selector-dropdown .current-end-date-input[data-uk-datepicker]", function () {
    selectBaselineDateRange(this)
    enableButton($(".time-input-apply-btn[rel='" + hash.view + "']"));
});

$("#main-view").on("change, keyup, focus, blur, input, paste", ".current-start-date-input, .current-start-time-input, .current-end-date-input, .current-end-time-input", function () {
    selectBaselineDateRange(this)
    enableButton($(".time-input-apply-btn[rel='" + hash.view + "']"));
});

$("#main-view").on("change, keyup", ".baseline-start-date-input, .baseline-start-time-input, .baseline-end-date-input, .baseline-end-time-input", function () {
    enableButton($(".time-input-apply-btn[rel='" + hash.view + "']"))
});

//Date & Time selector dropdown apply
$("#main-view").on("click", ".time-input-apply-btn", function () {
    applyTimeRangeSelection(this)
});


/** DASHBOARD FORM TIME RELATED METHODS **/
function selectAggregate(target) {

    var currentView = hash.view
    var unit = $(target).attr("unit")

    //update hash
    hash.aggTimeGranularity = unit;

    if ($("#" + hash.view + "-form-tip").attr("data-tip-source") == "daily-aggregate") {
        $("#" + hash.view + "-form-tip").hide();
    } else if ($("#" + hash.view + "-form-tip").attr("data-tip-source") == "hourly-aggregate") {
        $("#" + hash.view + "-form-tip").hide();
    }

    //Display message to user if daily granularity selected and only 1 day set the timerange to last 7 days with the latest available data
    if (unit == "DAYS") {

        var maxMillis = window.datasetConfig.maxMillis;
        var hh = maxMillis ? moment(maxMillis).format("HH") : "00";
        //Set the time selectors to midnight
        $("#" + currentView + "-current-end-time-input").val(hh + ":00");
        $("#" + currentView + "-current-start-time-input").val(hh + ":00");
        $("#" + currentView + "-baseline-start-time-input").val(hh + ":00");
        $("#" + currentView + "-baseline-end-time-input").val(hh + ":00");
        $(".time-input-apply-btn[rel='" + currentView + "']").click();

        //close uikit dropdown
        $("[data-uk-dropdown]").removeClass("uk-open");
        $("[data-uk-dropdown]").attr("aria-expanded", false);
        $(".uk-dropdown").hide();


        //Display message to the user if he would query only one datapoint

        var currentStartDate = $(".current-start-date[rel='" + currentView + "']").text()
        var currentEndDate = $(".current-end-date[rel='" + currentView + "']").text()

        //Using UTC timezone in comparison since timezone doesn't matter in this case
        var currentStartDateMillis = moment.tz(currentStartDate, "UTC").valueOf()
        var currentEndDateMillis = moment.tz(currentEndDate, "UTC").valueOf()

        var diff = currentEndDateMillis - currentStartDateMillis
        if (diff <= 86400000) {

            var tipMessage = $("#" + hash.view + "-form-tip p");
            var tipContainer = $("#" + hash.view + "-form-tip");
            tipContainer.attr("data-tip-source", "daily-aggregate");
            tipMessage.html("Select a broader date range to receive more meaningful data, more data points.");
            tipContainer.fadeIn(100);
        }
    } else if (unit == "HOURS")


    //Enable form submit
        enableFormSubmit()
}

function selectCurrentDateRange(target) {

    var currentTab = $(target).attr('rel');
    var tz = getTimeZone();
    var maxMillis = window.datasetConfig.maxMillis

    switch ($(target).val()) {
        case "today":

            var today = moment().format("YYYY-MM-DD");

            var hh = maxMillis ? moment(maxMillis).format("HH") : moment().format("HH");

            // set the input field values
            $(".current-start-date-input[rel='" + currentTab + "']").val(today);
            if (hh > 0) {
                $(".current-end-date-input[rel='" + currentTab + "']").val(today);
            } else {
                var yesterday = moment().add(-1, 'days').format("YYYY-MM-DD");
                $(".current-end-date-input[rel='" + currentTab + "']").val(today);
            }

            $(".current-start-time-input[rel='" + currentTab + "']").val("00:00");
            $(".current-end-time-input[rel='" + currentTab + "']").val(hh + ":00");

            break;

        case "yesterday":

            // from yesterday 12am to today 12am
            var today = moment().format("YYYY-MM-DD");
            var yesterday = moment().add(-1, 'days').format("YYYY-MM-DD");

            // set the input field values
            $(".current-end-date-input[rel='" + currentTab + "']").val(today);
            $(".current-end-time-input[rel='" + currentTab + "']").val("00:00");
            $(".current-start-date-input[rel='" + currentTab + "']").val(yesterday);
            $(".current-start-time-input[rel='" + currentTab + "']").val("00:00");

            break;
        case "7": //last7days not full days

            var currentEndMillis = moment().valueOf();
            if (maxMillis) {
                currentEndMillis = maxMillis;
            }

            //subtract 7 days in milliseconds
            var currentStartMillis = currentEndMillis - 86400000 * 7;

            var currentEndDateString = moment(currentEndMillis).tz(tz).format("YYYY-MM-DD")
            var currentEndTimeString = moment(currentEndMillis).tz(tz).format("HH:00")

            var currentStartDateString = moment(currentStartMillis).tz(tz).format("YYYY-MM-DD")

            $(".current-start-date-input[rel='" + currentTab + "']").val(currentStartDateString);
            $(".current-end-date-input[rel='" + currentTab + "']").val(currentEndDateString);
            $(".current-start-time-input[rel='" + currentTab + "']").val(currentEndTimeString);
            $(".current-end-time-input[rel='" + currentTab + "']").val(currentEndTimeString);
            $(".current-start-date-input[rel='" + currentTab + "']").attr("readonly");
            $(".current-end-date-input[rel='" + currentTab + "']").attr("readonly");

            break;
        case "24": //last24hours not full days

            var currentEndMillis = moment().valueOf();

            if (maxMillis) {

                currentEndMillis = maxMillis;
            }


            //subtract 7 days in milliseconds
            var currentStartMillis = currentEndMillis - 86400000;

            var currentEndDateString = moment(currentEndMillis).tz(tz).format("YYYY-MM-DD")
            var currentEndTimeString = moment(currentEndMillis).tz(tz).format("HH:00")

            var currentStartDateString = moment(currentStartMillis).tz(tz).format("YYYY-MM-DD")

            $(".current-start-date-input[rel='" + currentTab + "']").val(currentStartDateString);
            $(".current-end-date-input[rel='" + currentTab + "']").val(currentEndDateString);
            $(".current-start-time-input[rel='" + currentTab + "']").val(currentEndTimeString);
            $(".current-end-time-input[rel='" + currentTab + "']").val(currentEndTimeString);
            $(".current-start-date-input[rel='" + currentTab + "']").attr("readonly");
            $(".current-end-date-input[rel='" + currentTab + "']").attr("readonly");
            break;
        default: //case "custom"
        break;



    }

    //If the comparison is enabled the comparison inputfileds will be updated every time the current date input is updated
    $(".compare-mode-selector[rel='" + currentTab + "']").change();

    //Enable apply btn
    $(".time-input-apply-btn[rel='" + currentTab + "']").prop("disabled", false);
}

function toggleTimeComparison(target) {
    var currentTab = $(target).attr('rel');
    if ($(target).is(':checked')) {
        $(".compare-mode-selector[rel='" + currentTab + "']").change();
    } else {
        $(".time-input-apply-btn[rel='" + currentTab + "']").prop("disabled", false);
    }
}

function selectBaselineDateRange(target) {
    var currentTab = $(target).attr('rel');
    var tz = getTimeZone();

    var currentStartDateString = $(".current-start-date-input[rel='" + currentTab + "']").val();
    var currentEndDateString = $(".current-end-date-input[rel='" + currentTab + "']").val();

    var currentStartDate = moment.tz(currentStartDateString, tz);
    var currentEndDate = moment.tz(currentEndDateString, tz);

    var currentStartDateMillisUTC = currentStartDate.utc().valueOf();
    var currentEndDateMillisUTC = currentEndDate.utc().valueOf();
    var optionValue = $("#" + currentTab + "-compare-mode-selector").val();

    switch (optionValue) {
        case "7":
        case "14":
        case "21":
        case "28":
            var baselineStartDateUTCMillis = moment(currentStartDateMillisUTC - parseInt(optionValue) * 86400000).tz('UTC');
            var baselineEndDateUTCMillis = moment(currentEndDateMillisUTC - parseInt(optionValue) * 86400000).tz('UTC');

            var baselineStartDate = baselineStartDateUTCMillis.clone().tz(tz);
            var baselineEndDate = baselineEndDateUTCMillis.clone().tz(tz);

            var baselineStartDateString = baselineStartDate.format("YYYY-MM-DD");

            var baselineEndDateString = baselineEndDate.format("YYYY-MM-DD");

            $(".baseline-start-date-input[rel='" + currentTab + "']").val(baselineStartDateString);
            $(".baseline-end-date-input[rel='" + currentTab + "']").val(baselineEndDateString);
            $(".baseline-start-time-input[rel='" + currentTab + "']").val($("#" + currentTab + "-current-start-time-input").val());
            $(".baseline-end-time-input[rel='" + currentTab + "']").val($("#" + currentTab + "-current-end-time-input").val());
            $(".baseline-start-date-input[rel='" + currentTab + "']").attr("disabled", true);
            $(".baseline-end-date-input[rel='" + currentTab + "']").attr("disabled", true);
            $(".baseline-start-time-input[rel='" + currentTab + "']").attr("disabled", true);
            $(".baseline-end-time-input[rel='" + currentTab + "']").attr("disabled", true);

            break;
        case "custom":
        case "1":

            //var yesterday = moment().add(-1, 'days').format("YYYY-MM-DD");
            //$(".baseline-start-date-input[rel='"+ currentTab +"']").val(yesterday);
            //$(".baseline-end-date-input[rel='"+ currentTab +"']").val(yesterday);
            $(".baseline-start-time-input[rel='" + currentTab + "']").val($("#" + currentTab + "-current-start-time-input").val());
            $(".baseline-end-time-input[rel='" + currentTab + "']").val($("#" + currentTab + "-current-end-time-input").val());
            $(".baseline-start-date-input[rel='" + currentTab + "']").attr("disabled", false);
            $(".baseline-end-date-input[rel='" + currentTab + "']").attr("disabled", false);
            $(".baseline-start-time-input[rel='" + currentTab + "']").attr("disabled", false);
            $(".baseline-end-time-input[rel='" + currentTab + "']").attr("disabled", false);
            break;

    }
    $(".time-input-apply-btn[rel='" + currentTab + "']").prop("disabled", false);
}

function applyTimeRangeSelection(target) {

    var currentTab = $(target).attr('rel');
    var maxMillis = window.datasetConfig.maxMillis

    var currentStartDateString = $(".current-start-date-input[rel='" + currentTab + "']").val();
    var currentEndDateString = $(".current-end-date-input[rel='" + currentTab + "']").val();
    var currentStartTimeString = $(".current-start-time-input[rel='" + currentTab + "']").val();
    var currentEndTimeString = $(".current-end-time-input[rel='" + currentTab + "']").val();

    //Todo: error handling: handle when comparison is selected and no baseline is present


    var errorMsg = $(".time-input-logic-error[rel='" + currentTab + "'] p");
    var errorAlrt = $(".time-input-logic-error[rel='" + currentTab + "']");

    //hide error message
    errorAlrt.hide();

    //turn into milliseconds
    // DateTimes

    var currentStartDate = $(".current-start-date-input[rel='" + currentTab + "']").val();
    if (!currentStartDate) {
        errorMsg.html("Must provide start date");
        disableButton($(".time-input-apply-btn[rel='" + currentTab + "']"))
        errorAlrt.fadeIn(100);

        //show the dropdown
        $(".time-range-selector-dropdown[data-uk-dropdown]").addClass("uk-open");
        $(".time-range-selector-dropdown[data-uk-dropdown]").attr("aria-expanded", true);
        $(".time-input-apply-btn[rel='" + currentTab + "']").closest(".uk-dropdown").show();
        return
    }

    var currentEndDate = $(".current-end-date-input[rel='" + currentTab + "']").val();
    if (!currentEndDate) {
        errorMsg.html("Must provide end date");
        errorAlrt.fadeIn(100);


        disableButton($(".time-input-apply-btn[rel='" + currentTab + "']"))

        //show the dropdown
        $(".time-range-selector-dropdown[data-uk-dropdown]").addClass("uk-open");
        $(".time-range-selector-dropdown[data-uk-dropdown]").attr("aria-expanded", true);
        $(".time-input-apply-btn[rel='" + currentTab + "']").closest(".uk-dropdown").show();
        return
    }

    var currentStartTime = $(".current-start-time-input[rel='" + currentTab + "']").val();
    if (!currentStartTime) {
        errorMsg.html("Start time is required.");
        errorAlrt.fadeIn(100);
        disableButton($(".time-input-apply-btn[rel='" + currentTab + "']"))

        //show the dropdown
        $(".time-range-selector-dropdown[data-uk-dropdown]").addClass("uk-open");
        $(".time-range-selector-dropdown[data-uk-dropdown]").attr("aria-expanded", true);
        $(".time-input-apply-btn[rel='" + currentTab + "']").closest(".uk-dropdown").show();
        return
    }

    var currentEndTime = $(".current-end-time-input[rel='" + currentTab + "']").val();
    if (!currentEndTime) {
        errorMsg.html("End time is required.");
        errorAlrt.fadeIn(100);
        disableButton($(".time-input-apply-btn[rel='" + currentTab + "']"))

        //show the dropdown
        $(".time-range-selector-dropdown[data-uk-dropdown]").addClass("uk-open");
        $(".time-range-selector-dropdown[data-uk-dropdown]").attr("aria-expanded", true);
        $(".time-input-apply-btn[rel='" + currentTab + "']").closest(".uk-dropdown").show();
        return
    }

    var timezone = getTimeZone();
    var currentStart = moment.tz(currentStartDate + " " + currentStartTime, timezone);

    var currentStartMillisUTC = currentStart.utc().valueOf();
    var currentEnd = moment.tz(currentEndDate + " " + currentEndTime, timezone);
    var currentEndMillisUTC = currentEnd.utc().valueOf();


    //Error handling
    if (currentStartMillisUTC >= currentEndMillisUTC) {

        errorMsg.html("Please choose a start date that is earlier than the end date.");
        errorAlrt.fadeIn(100);
        disableButton($(".time-input-apply-btn[rel='" + currentTab + "']"))

        //show the dropdown
        $(".time-range-selector-dropdown[data-uk-dropdown]").addClass("uk-open");
        $(".time-range-selector-dropdown[data-uk-dropdown]").attr("aria-expanded", true);
        $(".time-input-apply-btn[rel='" + currentTab + "']").closest(".uk-dropdown").show();
        return

    }

    //The following error can be checked when time selection is in place
    /*else if(currentStartMillisUTC == currentEndMillisUTC){
     errorMsg.html("Please choose an end date that is later than the start date");
     errorAlrt.fadeIn(100);
     return
     }*/

    if (maxMillis) {
        if (currentStartMillisUTC > maxMillis || currentEndMillisUTC > maxMillis) {

            errorMsg.html("The data is available till: " + moment(maxMillis).format("YYYY-MM-DD h a") + ". Please select a time range prior to this date and time.");
            errorAlrt.fadeIn(100);
            disableButton($(".time-input-apply-btn[rel='" + currentTab + "']"));

            //show the dropdown
            $(".time-range-selector-dropdown[data-uk-dropdown]").addClass("uk-open");
            $(".time-range-selector-dropdown[data-uk-dropdown]").attr("aria-expanded", true);
            $(".time-input-apply-btn[rel='" + currentTab + "']").closest(".uk-dropdown").show();
            return
        }

    }


    if ($(".time-input-compare-checkbox[rel='" + currentTab + "']").is(':checked')) {
        var baselineStartDate = $(".baseline-start-date-input").val();
        var baselineEndDate = $(".baseline-end-date-input").val();
        var baselineStartTime = $(".baseline-start-time-input").val();
        var baselineEndTime = $(".baseline-end-time-input").val();

        var baselineStart = moment.tz(baselineStartDate + baselineStartTime, timezone);
        var baselineStartMillisUTC = baselineStart.utc().valueOf();
        var baselineEnd = moment.tz(baselineEndDate + baselineEndTime, timezone);
        var baselineEndMillisUTC = baselineEnd.utc().valueOf();

        if (baselineEndMillisUTC > currentStartMillisUTC && baselineStartMillisUTC < currentStartMillisUTC) {

            errorMsg.html("The compared time-periods overlap each-other, please adjust the request.");
            disableButton($(".time-input-apply-btn[rel='" + currentTab + "']"))
            errorAlrt.fadeIn(100);

            //show the dropdown
            $("[data-uk-dropdown]").addClass("uk-open");
            $("[data-uk-dropdown]").attr("aria-expanded", true);
            $(".time-input-apply-btn[rel='" + currentTab + "']").closest(".uk-dropdown").show();
            return

        }

        if (baselineStartMillisUTC > currentEndMillisUTC && baselineStartMillisUTC < currentStartMillisUTC) {

            errorMsg.html("Current time-period" + currentStartDate + " " + currentStartTime + "-" + currentEndDate + " " + currentEndTime + "should be a later time then baseline time-period" + baselineStartDate + " " + baselineStartTime + "-" + baselineEndDate + " " + baselineEndTime + ". Please switch the 'date range' and the 'compare to' date range.");

            //show the dropdown
            $("[data-uk-dropdown]").addClass("uk-open");
            $("[data-uk-dropdown]").attr("aria-expanded", true);
            $(".time-input-apply-btn[rel='" + currentTab + "']").closest(".uk-dropdown").show();
            return

        }
    }

    //hide user tip message about broader timerange
    if (moment(currentEndDate).valueOf() - moment(currentStartDate).valueOf() > 86400000) {

        if ($("#" + hash.view + "-form-tip").attr("data-tip-source") == "daily-aggregate") {
            $("#" + hash.view + "-form-tip").hide();
        }
    }


    //Change the value of the time fields on the main form
    $(".current-start-date[rel='" + currentTab + "']").html(currentStartDateString);
    $(".current-end-date[rel='" + currentTab + "']").html(currentEndDateString);
    $(".current-start-time[rel='" + currentTab + "']").html(currentStartTimeString);
    $(".current-end-time[rel='" + currentTab + "']").html(currentEndTimeString);

    if ($(".time-input-compare-checkbox[rel='" + currentTab + "']").is(':checked')) {
        var baselineStartDateString = $(".baseline-start-date-input[rel='" + currentTab + "']").val();
        var baselineEndDateString = $(".baseline-end-date-input[rel='" + currentTab + "']").val();
        var baselineStartTimeString = $(".baseline-start-time-input[rel='" + currentTab + "']").val();
        var baselineEndTimeString = $(".baseline-end-time-input[rel='" + currentTab + "']").val();

        $(".baseline-start-date[rel='" + currentTab + "']").html(baselineStartDateString);
        $(".baseline-end-date[rel='" + currentTab + "']").html(baselineEndDateString);
        $(".baseline-start-time[rel='" + currentTab + "']").html(baselineStartTimeString);
        $(".baseline-end-time[rel='" + currentTab + "']").html(baselineEndTimeString);
        $(".comparison-display[rel='" + currentTab + "']").show();
    } else {
        $("#baseline-start-date[rel='" + currentTab + "']").html("");
        $("#baseline-end-date[rel='" + currentTab + "']").html("");
        $(".comparison-display[rel='" + currentTab + "']").hide();
    }

    $(".compare-mode[rel='" + currentTab + "']").html($(".compare-mode-selector[rel='" + currentTab + "']").attr("unit"))

    //update hash
    //var selectedGranularity = $(".baseline-aggregate-copy[rel='" + currentTab + "'].uk-active").attr("unit");
    // $(".baseline-aggregate[rel='" + currentTab + "']").removeClass("uk-active");
    //$(".baseline-aggregate[rel='" + currentTab + "'][unit='" + selectedGranularity + "']").addClass("uk-active");

    $(target).attr("disabled", true);

    //close uikit dropdown
    $("[data-uk-dropdown]").removeClass("uk-open");
    $("[data-uk-dropdown]").attr("aria-expanded", false);
    $(".uk-dropdown").hide();

    enableFormSubmit()
}

