/** AJAX and HASH RELATED METHODS **/

function getData(url){
    console.log("request url:", url)
    return $.ajax({
        url: url,
        type: 'get',
        dataType: 'json',
        statusCode: {
            404: function() {
                $("#"+  hash.view  +"-chart-area-error").empty()
                var warning = $('<div></div>', { class: 'uk-alert uk-alert-warning' })
                warning.append($('<p></p>', { html: 'No data available. (Error code: 404)' }))
                $("#"+  hash.view  +"-chart-area-error").append(warning)
                $("#"+  hash.view  +"-chart-area-error").fadeIn(100);
                return
            },
            500: function() {
                $("#"+  hash.view  +"-chart-area-error").empty()
                var error = $('<div></div>', { class: 'uk-alert uk-alert-danger' })
                error.append($('<p></p>', { html: 'Internal server error' }))
                $("#"+  hash.view  +"-chart-area-error").append(error)
                $("#"+  hash.view  +"-chart-area-error").fadeIn(100);
                return
            }
        }
        ,
        beforeSend: showLoader()
    }).always(function(){
      hideLoader();
      if(hash.view != "anomalies"){
        $("#"+  hash.view  +"-display-chart-section").empty();
      }
    })
}

function showLoader(){
   $("#"+  hash.view  +"-chart-area-loader").show();
}

function hideLoader(){
    $("#"+  hash.view  +"-chart-area-loader").hide();

}

function parseHashParameters(hashString) {
    var params = {};

    if (hashString) {
        if (hashString.charAt(0) == '#') {
            hashString = hashString.substring(1);
        }

        var keyValuePairs = hashString.split('&');

        $.each(keyValuePairs, function(i, pair) {
            var tokens = pair.split('=');
            var key = decodeURIComponent(tokens[0]);
            var value = decodeURIComponent(tokens[1]);
            if(key != "filters") {
                params[key] = value;
            }else{
                params["filters"] = decodeURIComponent(value)
            }
        })
    }

    return params
}

function encodeHashParameters(hashParameters) {
    var keyValuePairs = [];
    $.each(hashParameters, function(key, value) {
        keyValuePairs.push(encodeURIComponent(key) + '=' + encodeURIComponent(value));
    })
    return '#' + keyValuePairs.join('&');
}

function updateHashParam(param, value){
    hash[param] = value;
}

function updateDashboardFormFromHash(){

    //Preselect dataset if present in hash
    if (!hash.hasOwnProperty('dataset')){
        $(".selected-dataset").text("Select dataset");
    }

    //Preselect header-tab if present in hash
    if (hash.hasOwnProperty('view')){
        $(".header-tab[rel='"+ hash.view +"']").click();
    }else{
        $(".header-tab[rel='dashboard']").click();
    }

    //Preselect dashboard if present in hash
    if (hash.hasOwnProperty('dashboard')){

        $(".dashboard-option[value='"+ hash.dashboard +"']").click();
    }else if (hash.view == "dashboard"){

        //Preselect first dashboard in the list
        $(".dashboard-option:first-child").click();
    }

    var currentForm =  $("#"+hash.view +"-form") ;

    //Preselect metrics if present in hash
    $(".view-metric-selector .added-item").remove();

    if (hash.hasOwnProperty('metrics')){

        //the value is a string so need to transform it into array
        var metricAry = hash.metrics.split(',');

        for (var i = 0, len = metricAry.length; i < len; i++) {

            $(".metric-option[value='" + metricAry[i] + "']", currentForm).click();

        }
    }

    //Preselect dimensions if present in hash
    $(".view-dimension-selector .added-item").remove();
    if (hash.hasOwnProperty('dimensions')) {

        //the value is a string so need to transform it into array
        var dimensionAry = hash.dimensions.split(',');

        for (var i = 0, len = dimensionAry.length; i < len; i++) {
            $(".dimension-option[value='" + dimensionAry[i] + "']", currentForm).click();
        }
    }

    $(".filter-dimension-option:first-of-type").click();

    var tz = getTimeZone();
    var maxMillis = window.datasetConfig.maxMillis;
    var currentStartDateTime;
    var currentEndDateTime;
    var baselineStartDateTime;
    var baselineEndDateTime;

    var currentStartDateString;
    var currentEndDateString;

    var currentStartTimeString;
    var currentEndTimeString;

    var baselineStartDateString;
    var baselineEndDateString;

    var baselineStartTimeString;
    var baselineEndTimeString;

    if (hash.hasOwnProperty("currentStart")) {
        currentStartDateTimeUTC = moment(parseInt(hash.currentStart)).tz('UTC');

        currentStartDateTime = currentStartDateTimeUTC.clone().tz(tz);
        currentStartDateString = currentStartDateTime
            .format("YYYY-MM-DD");
        currentStartTimeString = currentStartDateTime
            .format("HH:mm");

    } else if(maxMillis){
        // populate max date -1 day
        currentStartDateTime = moment(maxMillis).add(-1, 'days');
        currentStartDateString = currentStartDateTime.format("YYYY-MM-DD");
        currentStartTimeString = currentStartDateTime.format("HH:00");

    }else{

        // populate todays date
        currentStartDateTime = moment(parseInt(Date.now()));
        currentStartDateString = currentStartDateTime.format("YYYY-MM-DD");
        currentStartTimeString = currentStartDateTime.format("00:00");
    }

    if (hash.hasOwnProperty("currentEnd")) {
        currentEndDateTime = moment(parseInt(hash.currentEnd)).tz('UTC').clone().tz(tz);
        currentEndDateString = currentEndDateTime
            .format("YYYY-MM-DD");
        currentEndTimeString = currentEndDateTime
            .format("HH:mm");
    } else if(maxMillis) {

        currentEndDateTime = moment(maxMillis);
        currentEndDateString = currentEndDateTime.format("YYYY-MM-DD");
        currentEndTimeString = currentEndDateTime.format("HH:00");
    }else{
        currentEndDateTime = moment(parseInt(Date.now()));
        currentEndDateString = currentEndDateTime.format("YYYY-MM-DD");
        currentEndTimeString = currentEndDateTime.format("HH:00");
    }

    if (hash.hasOwnProperty("baselineStart")) {

        baselineStartDateTime = moment(parseInt(hash.baselineStart)).tz('UTC').clone().tz(tz);
        baselineStartDateString = baselineStartDateTime.format("YYYY-MM-DD");
        baselineStartTimeString = currentStartTimeString;

    } else {

        baselineStartDateTime = currentStartDateTime.add(-7,'days');
        baselineStartDateString = baselineStartDateTime.format("YYYY-MM-DD");
        baselineStartTimeString = currentStartTimeString;
    }

    if (hash.hasOwnProperty("baselineEnd")) {

        baselineEndDateTimeUTC = moment(parseInt(hash.baselineEnd)).tz('UTC');

        baselineEndDateTime = baselineEndDateTimeUTC.clone().tz(tz);

        baselineEndDateString = baselineEndDateTime
            .format("YYYY-MM-DD");
        baselineEndTimeString = currentEndTimeString;

    } else {
        baselineEndDateTime = currentEndDateTime
            .add(-7, 'days');
        baselineEndDateString = baselineEndDateTime
            .format("YYYY-MM-DD");
        baselineEndTimeString = currentEndTimeString;
    }

    $("#" + hash.view + "-current-start-date").text(currentStartDateString);
    $("#" + hash.view + "-current-end-date").text(currentEndDateString);

    $("#" + hash.view + "-current-start-time").text(currentStartTimeString);
    $("#" + hash.view + "-current-end-time").text(currentEndTimeString);

    $("#" + hash.view + "-baseline-start-date").text(baselineStartDateString);
    $("#" + hash.view + "-baseline-end-date").text(baselineEndDateString);

    $("#" + hash.view + "-baseline-start-time").text(baselineStartTimeString);
    $("#" + hash.view + "-baseline-end-time").text(baselineEndTimeString);

    $("#" + hash.view + "-current-start-date-input").val(currentStartDateString);
    $("#" + hash.view + "-current-end-date-input").val(currentEndDateString);

    $("#" + hash.view + "-current-start-time-input").val(currentStartTimeString);
    $("#" + hash.view + "-current-end-time-input").val(currentEndTimeString);

    $("#" + hash.view + "-baseline-start-date-input").val(baselineStartDateString);
    $("#" + hash.view + "-baseline-end-date-input").val(baselineEndDateString);

    $("#" + hash.view + "-baseline-start-time-input").val(baselineStartTimeString);
    $("#" + hash.view + "-baseline-end-time-input").val(baselineEndTimeString);

    // Update aggTimeGranularity from hash
    if (hash.hasOwnProperty("aggTimeGranularity")) {
        $(".baseline-aggregate[rel='"+ hash.view +"'][unit='"+ hash.aggTimeGranularity + "']").click();
    }

    var compareMode ="WoW";
    //update compareMode
    if (hash.hasOwnProperty("compareMode") && hash.compareMode != "") {
        compareMode = hash.compareMode;
    }
    $(".compare-mode[rel='"+ hash.view +"']").html(compareMode);

    $(".compare-mode-selector option[unit='" + compareMode + "']").change();

    //Populate filters from hash
    if(hash.hasOwnProperty("filters")){

        var filterParams = JSON.parse(decodeURIComponent(hash.filters));

        updateFilterSelection(filterParams)

    }

    //Close dropdown
    $("[data-uk-dropdown]").removeClass("uk-open");
    $("[data-uk-dropdown]").attr("aria-expanded", false);
    $(".uk-dropdown").hide();
}



function formComponentPopulated(){
    if(window.responseDataPopulated == window.numFormComponents){
        delete window.responseDataPopulated;
        delete window.numFormComponents;

        updateDashboardFormFromHash();

        //If hash has dataset && dashboard trigger form submit

        if( hash.hasOwnProperty("dataset")){

            //If dashboard is present in hash and present in current dataset
            //If hash has dataset && dashboard or metric name trigger form submit
            if(hash.view == "dashboard" && hash.hasOwnProperty("dashboard")){

                //if the dashboard is present in the current dataset
                if( $(".dashboard-option[value='"+ hash.dashboard +"']").length>0 ){
                    //Adding random number to hash
                    //for the usecase when on pagereload the hash would not change so ajax would not be triggered
                    var rand= Math.random() + ""
                    hash.rand = rand.substring(3,6);
                    enableFormSubmit();
                    $("#" + hash.view + "-form-submit").click();
                }
            } else if(hash.hasOwnProperty("metrics")){
                //if the metric is present in the current dataset
                var metricList = hash.metrics.split(",");
                for(var i=0, len = metricList.length; i<len;i++){
                    if($(".metric-option[value='"+ metricList[i] +"']").length>0){
                        //Adding random number to hash
                        //for the usecase when on pagereload the hash would not change so ajax would not be triggered
                        var rand= Math.random() + ""
                        hash.rand = rand.substring(3,6);
                        enableFormSubmit();
                        $("#" + hash.view + "-form-submit").click();
                    }
                }

            }
        }
    }
}

/** DASHBOARD FORM RELATED METHODS **/
/* used on href="#" anchor tags prevents the default jump to the top of the page */
function eventPreventDefault(event){
    event.preventDefault();
}

/* takes a clicked anchor tag and applies active class to it's prent (li, button) */
function  radioOptions(target){
        $(target).parent().siblings().removeClass("uk-active");
        $(target).parent().addClass("uk-active");
}

function radioButtons(target){

    if(!$(target).hasClass("uk-active")) {
        $(target).siblings().removeClass("uk-active");
        $(target).addClass("uk-active");
    }
}

//Advanced settings
function closeClosestDropDown(target){

    $(target).closest($("[data-uk-dropdown]")).removeClass("uk-open");
    $(target).closest($("[data-uk-dropdown]")).attr("aria-expanded", false);
    $(target).closest(".uk-dropdown").hide();
}

function enableApplyButton(button){
    $(button).prop("disabled", false);
    $(button).removeAttr("disabled");
}

function disableApplyButton(button){
    $(button).prop("disabled", true);
    $(button).attr("disabled", true);
}

function selectDatasetNGetFormData(target){



    //Cleanup form: Remove added-item and added-filter, metrics of the previous dataset
    $("#"+  hash.view  +"-chart-area-error").hide();
    $(".view-metric-selector .added-item").remove();
    $(".view-dimension-selector .added-item").remove();
    $(".metric-list").empty();
    $(".dimension-list").empty();
    $(".filter-dimension-list").empty()
    $(".filter-panel .value-filter").remove();
    $(".added-filter").remove();
    $(".filter-panel .value-filter").remove();


    //Remove previous dataset's hash values
    delete hash.baselineStart
    delete hash.baselineEnd
    delete hash.currentStart
    delete hash.currentEnd
    delete hash.compareMode
    delete hash.dashboard
    delete hash.metrics
    delete hash.dimensions
    delete hash.filters
    delete hash.aggTimeGranularity


    var value = $(target).attr("value");
    hash.dataset = value;

    //Trigger AJAX calls
    //get the latest available data timestamp of a dataset
    getAllFormData()

    //Populate the selected item on the form element
    $(".selected-dataset").text($(target).text());
    $(".selected-dataset").attr("value",value);

    //Close uikit dropdown
    $(target).closest("[data-uk-dropdown]").removeClass("uk-open");
    $(target).closest("[data-uk-dropdown]").attr("aria-expanded", false);


}

function switchHeaderTab(target){
    radioButtons(target)
    hash.view = $(target).attr("rel");
}

function  selectDashboard(target){


    //Update hash values
    var value = $(target).attr("value");
    hash.dashboard = value;

    //Update selectors
    $("#selected-dashboard").text($(target).text());
    $("#selected-dashboard").attr("value",value);

    if($("#"+ hash.view +"-time-input-form-error").attr("data-error-source") == "dashboard-option"){
        $("#"+ hash.view +"-time-input-form-error").hide();
    }

    //close uikit dropdown
    $(target).closest("[data-uk-dropdown]").removeClass("uk-open");
    $(target).closest("[data-uk-dropdown]").attr("aria-expanded", false);

    //Remove earlier selected added-item and added-filter and update hash
    delete hash.metrics
    delete hash.dimensions
    delete hash.filters

    //Enable Go btn
    enableFormSubmit()

}


function selectMetric(target){


    //We have either dashboard or metrics param in the queries
    delete hash.dashboard

    //Update hash values
    var param = "metrics";
    var value = $(target).attr("value");

    //if key doesn't exist
    if(!hash["metrics"]){
        hash["metrics"] = value;

        //if key exist and value is not part of the array
    }else if( hash["metrics"].indexOf(value) < 0){
        hash["metrics"] =  hash["metrics"] +","+ value;
    }

    //Hide alert
    if($("#"+ hash.view +"-time-input-form-error").attr("data-error-source") == "metric-option"){
        $("#"+ hash.view +"-time-input-form-error").hide();
    }


    //Update selectors
    $(target).hide();
    $(".selected-metrics-list[rel='"+ hash.view +"']").append("<li class='added-item uk-button remove-selection' rel='"+ param + "' value='"+ value +"'><a href='#'>" + $(target).text() +  "<i class='uk-icon-close'></i></a></li>");

    //Enable Go btn
    enableFormSubmit()
}

function selectDimension(target){

    //Update hash values
    var param = "dimensions";
    var value = $(target).attr("value");

    //Update hash

    //if key doesn't exist
    if(!hash[param]){
        hash[param] = value;

        //if key exist and value is not part of the array
    }else if( hash[param].indexOf(value) < 0){
        hash[param] =  hash[param] +","+ value;
    }

    //Enable Go btn
    $("#" + hash.view + "-form-submit").prop("disabled", false);

    //Update selectors
    $(target).attr("selected", true);
    $(target).hide();
    $(".selected-dimensions-list[rel='"+ hash.view +"']").append("<li class='added-item  uk-button remove-selection' rel='"+ param + "' value='"+ value +"'><a href='#'>" + $(target).text() +  "<i class='uk-icon-close'></i></a></li>");

}

function toggleAllDimensions(target){
    //Todo:
}

function removeSelection(target){

    //remove the item from the hash
    var param = $(target).attr("rel");
    var value = $(target).attr("value");

    if(hash.hasOwnProperty(param)) {

        hash[param] = hash[param].replace(value, "")

        if (hash[param].indexOf(",") == 0) {
            hash[param] = hash[param].substr(1, hash[param].length);
        } else if (hash[param][hash[param].length - 1] == ",") {
            hash[param] = hash[param].substr(0, hash[param].length - 1);
        } else if (hash[param].indexOf(",,") > -1) {
            hash[param] = hash[param].replace(",,", ",");
        }
        if (hash[param] == "") {
            delete hash[param];
        }
    }

    //Remove the label and make the list item available
    $(target).remove();
    $("li[rel="+ param +"][value="+ value  +"]").show();

    //Enable Go btn
    enableFormSubmit()
}



/** DASHBOARD FORM TIME RELATED METHODS **/
function  selectAggregate(target) {


    //update hash
    var currentView=  hash.view
    var currentContainer = $(".current-date-range-selector[rel='"+ currentView +"']")
    var unit = $(target).attr("unit")

    //if dayly granularity selected set the timerange to last 7 days with 12am time
    if(unit == "DAYS"){

       $(".current-date-range-selector[rel='"+ currentView +"']").val("7")
        $(".current-date-range-selector[rel='"+ currentView +"']").change()

        $("#"+ currentView +"-current-end-time-input").val("00:00");
        $("#"+ currentView +"-current-start-time-input").val("00:00");
        $("#"+ currentView +"-baseline-start-time-input").val("00:00");
        $("#"+ currentView +"-baseline-end-time-input").val("00:00");
        $(".time-input-apply-btn[rel='"+ currentView +"']" ).click()

    }
    hash.aggTimeGranularity = unit;

    //Enable form submit
    enableFormSubmit()

}


//function  updateBaselineTimeRange(target){
//    var currentTab = $(target).attr("rel");
//    $(".compare-mode-selector[rel='"+ currentTab +"']").change();
//}


/** DASHBOARD FORM FILTER RELATED METHODS **/
function  selectFilterDimensionOption(target){
    $(".value-filter").hide();
    var dimension= $(target).attr("value")
    $(".value-filter[rel='"+ dimension +"' ]").show();
}

function applyFilterSelection(){

    var currentTabFilters = $("#"+hash.view+"-filter-panel");

    //Set hash params
    var filters = {};
    var labels = {};

    $(".filter-value-checkbox", currentTabFilters).each(function(i, checkbox) {
        var checkboxObj = $(checkbox);

        if (checkboxObj.is(':checked')) {
            var key = $(checkbox).attr("rel");
            var value = $(checkbox).attr("value");
            var valueAlias = $(checkbox).parent().text();

            if(filters[key]){
                filters[key].push(value) ;
                labels[key].push(valueAlias) ;
            }else{
                filters[key] = [value];
                labels[key] = [valueAlias];
            }
        }
    });

    hash.filters = encodeURIComponent(JSON.stringify(filters));

    //Disable Apply filters button and close popup
    enableApplyButton()

    //Todo: Show selected filters on dashboard
    //empty previous filters labels
    $(".added-filter[tab='"+ hash.view +"']").remove()

    //append new labels
    var html = "";
    for(k in labels){
        html +=  "<li class='added-filter uk-button remove-filter-selection' tab="+ hash.view +" rel='" + k + "' value='" + labels[k] + "' title='" + k + ": " + decodeURIComponent(labels[k]) +  "'>" + k + ": " + decodeURIComponent(labels[k]) + "<i class='uk-icon-close'></i></li>";
    }

    $(".selected-filters-list[rel='"+ hash.view +"']").append(html);

    //Enable Go btn
    enableFormSubmit()
    $("#" + hash.view +"-filter-panel").hide();
}

/*takes an object with dimensionNames as keys and an array of dimensionValues as values,
 applies them to the current form and enables form submit */
function updateFilterSelection(filterParams){
    var currentFilterContainer = $(".view-filter-selector[rel='"+ hash.view +"']");
    var elementsPresent = 1;
    for(var f in filterParams){
        var dimensionValues = filterParams[f];

        for(var v =0 , len = dimensionValues.length; v < len; v++){
            if($(".filter-value-checkbox[rel='"+ f +"'][value='"+ dimensionValues[v] +"']").length == 0){
                elementsPresent =0;
                break;
            }
            $(".filter-value-checkbox[rel='"+ f +"'][value='"+ dimensionValues[v] +"']", currentFilterContainer).attr('checked', 'checked');
        }
    }

    if(elementsPresent == 1) {
        //Enable then trigger apply btn
        $('.apply-filter-btn', currentFilterContainer).prop("disabled", false);
        $(".apply-filter-btn", currentFilterContainer).click();
        $(".apply-filter-btn", currentFilterContainer).parent("a.uk-dropdown-close").click();
    }
}

function readFiltersAppliedInCurrentView(currentTab){
    var currentFilterContainer = $(".view-filter-selector[rel='"+ currentTab +"']")
    var filters = {};

    $(".added-filter",currentFilterContainer).each(function(){
        var keyValue = $(this).attr("title").trim().split(":");
        var dimension = keyValue[0];
        var valuesAryToTrim = keyValue[1].trim().split(",")
        var valuesAry = [];
        for(var index=0, len= valuesAryToTrim.length; index < len; index++){
            var value = valuesAryToTrim[index].trim();
            if(value == "UNKNOWN"){
                value = "";
            }

            valuesAry.push(value)
        }
        filters[dimension] = valuesAry;
    })
    return filters
}

function enableFormSubmit(){

    $("#" + hash.view + "-form-submit").prop("disabled", false);
    $("#" + hash.view + "-form-submit").removeAttr("disabled");
}


function selectCurrentDateRange(target){

    var currentTab = $(target).attr('rel');
    var tz = getTimeZone();
    var maxMillis = window.datasetConfig.maxMillis

    switch ($(target).val()){
        case "today":

            var today = moment().format("YYYY-MM-DD");
            
            var hh = moment().format("HH");

            // set the input field values
            $(".current-start-date-input[rel='"+ currentTab +"']").val(today);
            if(hh >0){
              $(".current-end-date-input[rel='"+ currentTab +"']").val(today);
            } else{
              var yesterday = moment().add(-1, 'days').format("YYYY-MM-DD");
              $(".current-end-date-input[rel='"+ currentTab +"']").val(today);
            }
            
            
            $(".current-start-time-input[rel='"+ currentTab +"']").val("00:00");
            $(".current-end-time-input[rel='"+ currentTab +"']").val(hh+":00");

            //Disable inputfield edit
           // $(".current-start-date-input[rel='"+ currentTab +"']").attr("disabled", true);
           // $(".current-end-date-input[rel='"+ currentTab +"']").attr("disabled", true);
            //$(".current-start-time-input[rel='"+ currentTab +"']").attr("disabled", true);
            //$(".current-end-time-input[rel='"+ currentTab +"']").attr("disabled", true);

            break;

        case "yesterday":

            // from yesterday 12am to today 12am
            var today = moment().format("YYYY-MM-DD");
            var yesterday = moment().add(-1, 'days').format("YYYY-MM-DD");

            // set the input field values
            $(".current-end-date-input[rel='"+ currentTab +"']").val(today);
            $(".current-end-time-input[rel='"+ currentTab +"']").val("00:00");
            $(".current-start-date-input[rel='"+ currentTab +"']").val(yesterday);
            $(".current-start-time-input[rel='"+ currentTab +"']").val("00:00");

            //disable the inputfields
            $(".current-start-date-input[rel='"+ currentTab +"']").attr("disabled", true);
            $(".current-end-date-input[rel='"+ currentTab +"']").attr("disabled", true);
            $(".current-start-time-input[rel='"+ currentTab +"']").attr("disabled", true);
            $(".current-end-time-input[rel='"+ currentTab +"']").attr("disabled", true);

            break;
        case "7": //last7days not full days

            var currentEndMillis = moment().valueOf();
            if(maxMillis){
                currentEndMillis = maxMillis;
            }

            //subtract 7 days in milliseconds
            var currentStartMillis =  currentEndMillis - 86400000 * 7;

            var currentEndDateString =  moment(currentEndMillis).tz(tz).format("YYYY-MM-DD")
            var currentEndTimeString =  moment(currentEndMillis).tz(tz).format("HH:00")

            var currentStartDateString = moment(currentStartMillis).tz(tz).format("YYYY-MM-DD")

            $(".current-start-date-input[rel='"+ currentTab +"']").val(currentStartDateString);
            $(".current-end-date-input[rel='"+ currentTab +"']").val(currentEndDateString);
            $(".current-start-time-input[rel='"+ currentTab +"']").val(currentEndTimeString);
            $(".current-end-time-input[rel='"+ currentTab +"']").val(currentEndTimeString);
            $(".current-start-date-input[rel='"+ currentTab +"']").attr("readonly");
            $(".current-end-date-input[rel='"+ currentTab +"']").attr("readonly");
            //$(".current-start-date-input[rel='"+ currentTab +"']").attr("disabled", true);
            //$(".current-end-date-input[rel='"+ currentTab +"']").attr("disabled", true);
            //$(".current-start-time-input[rel='"+ currentTab +"']").attr("disabled", true);
            //$(".current-end-time-input[rel='"+ currentTab +"']").attr("disabled", true);

            break;
        case "24": //last24hours not full days

            var currentEndMillis = moment().valueOf();

            if(maxMillis){

                currentEndMillis = maxMillis;
            }


            //subtract 7 days in milliseconds
            var currentStartMillis =  currentEndMillis - 86400000;

            var currentEndDateString = moment(currentEndMillis).tz(tz).format("YYYY-MM-DD")
            var currentEndTimeString =  moment(currentEndMillis).tz(tz).format("HH:00")

            var currentStartDateString = moment(currentStartMillis).tz(tz).format("YYYY-MM-DD")

            $(".current-start-date-input[rel='"+ currentTab +"']").val(currentStartDateString);
            $(".current-end-date-input[rel='"+ currentTab +"']").val(currentEndDateString);
            $(".current-start-time-input[rel='"+ currentTab +"']").val(currentEndTimeString);
            $(".current-end-time-input[rel='"+ currentTab +"']").val(currentEndTimeString);
            $(".current-start-date-input[rel='"+ currentTab +"']").attr("readonly");
            $(".current-end-date-input[rel='"+ currentTab +"']").attr("readonly");
            //$(".current-start-date-input[rel='"+ currentTab +"']").attr("disabled", true);
            //$(".current-end-date-input[rel='"+ currentTab +"']").attr("disabled", true);
           //$(".current-start-time-input[rel='"+ currentTab +"']").attr("disabled", true);
            //$(".current-end-time-input[rel='"+ currentTab +"']").attr("disabled", true);

            break;
        case "custom":

            //Enable inputfield edit
            //$(".current-start-date-input[rel='"+ currentTab +"']").attr("disabled", false);
            //$(".current-end-date-input[rel='"+ currentTab +"']").attr("disabled", false);
            //$(".current-start-time-input[rel='"+ currentTab +"']").attr("disabled", false);
            //$(".current-end-time-input[rel='"+ currentTab +"']").attr("disabled", false);
    }

    //If the comparison is enabled the comparison inputfileds will be updated every time the current date input is updated
    $(".compare-mode-selector[rel='"+ currentTab +"']").change();

    //Enable apply btn
    $(".time-input-apply-btn[rel='"+ currentTab +"']").prop("disabled", false);
}

function toggleTimeComparison(target){
    var currentTab = $(target).attr('rel');
    if( $(target).is(':checked') ){
        $(".compare-mode-selector[rel='"+ currentTab +"']").change();
    }else{
        $(".time-input-apply-btn[rel='"+ currentTab +"']").prop("disabled", false);
    }
}

function selectBaselineDateRange(target){
    var currentTab = $(target).attr('rel');
    var tz = getTimeZone();

    var currentStartDateString = $(".current-start-date-input[rel='"+ currentTab +"']").val();
    var currentEndDateString = $(".current-end-date-input[rel='"+ currentTab +"']").val();


    var currentStartDate = moment.tz(currentStartDateString, tz);
    var currentEndDate = moment.tz(currentEndDateString, tz);

    var currentStartDateMillisUTC = currentStartDate.utc().valueOf();
    var currentEndDateMillisUTC = currentEndDate.utc().valueOf();
    var optionValue = $("#"+ currentTab +"-compare-mode-selector").val();

    switch (optionValue){
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

            $(".baseline-start-date-input[rel='"+ currentTab +"']").val(baselineStartDateString);
            $(".baseline-end-date-input[rel='"+ currentTab +"']").val(baselineEndDateString);
            $(".baseline-start-time-input[rel='"+ currentTab +"']").val($("#"+ currentTab +"-current-start-time-input").val());
            $(".baseline-end-time-input[rel='"+ currentTab +"']").val($("#"+ currentTab +"-current-end-time-input").val());
            $(".baseline-start-date-input[rel='"+ currentTab +"']").attr("disabled", true);
            $(".baseline-end-date-input[rel='"+ currentTab +"']").attr("disabled", true);
            $(".baseline-start-time-input[rel='"+ currentTab +"']").attr("disabled", true);
            $(".baseline-end-time-input[rel='"+ currentTab +"']").attr("disabled", true);

            break;

        case "1":
            //var yesterday = moment().add(-1, 'days').format("YYYY-MM-DD");
            //$(".baseline-start-date-input[rel='"+ currentTab +"']").val(yesterday);
            //$(".baseline-end-date-input[rel='"+ currentTab +"']").val(yesterday);
            $(".baseline-start-time-input[rel='"+ currentTab +"']").val($("#"+ currentTab +"-current-start-time-input").val());
            $(".baseline-end-time-input[rel='"+ currentTab +"']").val($("#"+ currentTab +"-current-end-time-input").val());
            $(".baseline-start-date-input[rel='"+ currentTab +"']").attr("disabled", false);
            $(".baseline-end-date-input[rel='"+ currentTab +"']").attr("disabled", false);
            $(".baseline-start-time-input[rel='"+ currentTab +"']").attr("disabled", false);
            $(".baseline-end-time-input[rel='"+ currentTab +"']").attr("disabled", false);
        break;

    }
    $(".time-input-apply-btn[rel='"+ currentTab +"']").prop("disabled", false);
}

function  applyTimeRangeSelection(target) {

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
        disableApplyButton($(".time-input-apply-btn[rel='"+ currentTab +"']"))
        errorAlrt.fadeIn(100);

        //show the dropdown
        $(".time-range-selector-dropdown[data-uk-dropdown]").addClass("uk-open");
        $(".time-range-selector-dropdown[data-uk-dropdown]").attr("aria-expanded", true);
        $(".time-input-apply-btn[rel='"+ currentTab +"']").closest(".uk-dropdown").show();
        return
    }

    var currentEndDate = $(".current-end-date-input[rel='" + currentTab + "']").val();
    if (!currentEndDate) {
        errorMsg.html("Must provide end date");
        errorAlrt.fadeIn(100);


        disableApplyButton($(".time-input-apply-btn[rel='"+ currentTab +"']"))

        //show the dropdown
        $(".time-range-selector-dropdown[data-uk-dropdown]").addClass("uk-open");
        $(".time-range-selector-dropdown[data-uk-dropdown]").attr("aria-expanded", true);
        $(".time-input-apply-btn[rel='"+ currentTab +"']").closest(".uk-dropdown").show();
        return
    }

    var currentStartTime = $(".current-start-time-input[rel='" + currentTab + "']").val();
    if (!currentStartTime) {
        errorMsg.html("Start time is required.");
        errorAlrt.fadeIn(100);
        disableApplyButton($(".time-input-apply-btn[rel='"+ currentTab +"']"))

        //show the dropdown
        $(".time-range-selector-dropdown[data-uk-dropdown]").addClass("uk-open");
        $(".time-range-selector-dropdown[data-uk-dropdown]").attr("aria-expanded", true);
        $(".time-input-apply-btn[rel='"+ currentTab +"']").closest(".uk-dropdown").show();
        return
    }

    var currentEndTime = $(".current-end-time-input[rel='" + currentTab + "']").val();
    if (!currentEndTime) {
        errorMsg.html("End time is required.");
        errorAlrt.fadeIn(100);
        disableApplyButton($(".time-input-apply-btn[rel='"+ currentTab +"']"))

        //show the dropdown
        $(".time-range-selector-dropdown[data-uk-dropdown]").addClass("uk-open");
        $(".time-range-selector-dropdown[data-uk-dropdown]").attr("aria-expanded", true);
        $(".time-input-apply-btn[rel='"+ currentTab +"']").closest(".uk-dropdown").show();
        return
    }

    var timezone = getTimeZone();
    var currentStart = moment.tz(currentStartDate + " " + currentStartTime, timezone);

    var currentStartMillisUTC = currentStart.utc().valueOf();
    var currentEnd = moment.tz(currentEndDate + " " + currentEndTime, timezone);
    var currentEndMillisUTC = currentEnd.utc().valueOf();


    //Error handling
    if (currentStartMillisUTC > currentEndMillisUTC) {

        errorMsg.html("Please choose an end date that is later than the start date.");
        errorAlrt.fadeIn(100);
        disableApplyButton($(".time-input-apply-btn[rel='"+ currentTab +"']"))

        //show the dropdown
        $(".time-range-selector-dropdown[data-uk-dropdown]").addClass("uk-open");
        $(".time-range-selector-dropdown[data-uk-dropdown]").attr("aria-expanded", true);
        $(".time-input-apply-btn[rel='"+ currentTab +"']").closest(".uk-dropdown").show();
        return

    }

    //The following error can be checked when time selection is in place
    /*else if(currentStartMillisUTC == currentEndMillisUTC){
     errorMsg.html("Please choose an end date that is later than the start date");
     errorAlrt.fadeIn(100);
     return
     }*/

    if(maxMillis){
        if(currentStartMillisUTC > maxMillis || currentEndMillisUTC > maxMillis) {

            errorMsg.html("The data is available till: " + moment(maxMillis).format("YYYY-MM-DD h a") + ". Please select a time range prior to this date and time.");
            errorAlrt.fadeIn(100);
            disableApplyButton($(".time-input-apply-btn[rel='" + currentTab + "']"));

            //show the dropdown
            $(".time-range-selector-dropdown[data-uk-dropdown]").addClass("uk-open");
            $(".time-range-selector-dropdown[data-uk-dropdown]").attr("aria-expanded", true);
            $(".time-input-apply-btn[rel='"+ currentTab +"']").closest(".uk-dropdown").show();
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
            disableApplyButton($(".time-input-apply-btn[rel='"+ currentTab +"']"))
            errorAlrt.fadeIn(100);

            //show the dropdown
            $("[data-uk-dropdown]").addClass("uk-open");
            $("[data-uk-dropdown]").attr("aria-expanded", true);
            $(".time-input-apply-btn[rel='"+ currentTab +"']").closest(".uk-dropdown").show();
            return

        }

        if (baselineStartMillisUTC > currentEndMillisUTC && baselineStartMillisUTC < currentStartMillisUTC) {

            errorMsg.html("Current time-period" + currentStartDate + " " + currentStartTime + "-" + currentEndDate + " " + currentEndTime + "should be a later time then baseline time-period" + baselineStartDate + " " + baselineStartTime + "-" + baselineEndDate + " " + baselineEndTime + ". Please switch the 'date range' and the 'compare to' date range.");

            //show the dropdown
            $("[data-uk-dropdown]").addClass("uk-open");
            $("[data-uk-dropdown]").attr("aria-expanded", true);
            $(".time-input-apply-btn[rel='"+ currentTab +"']").closest(".uk-dropdown").show();
            return

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



/** CHART RELATED METHODS **/

//Assigns an hexadecimal colorcode to each element of an array
//If you change this method, change the assignColorByID handlebars helper too the 2 serves all the time series type charts
function assignColorByID(len, index){

    var colorAry =  colorScale(len)

    return "#" + colorAry[index]


}

/*takes the number of items and returns an array with colors on the full 256^3 colorscale */
//If you change this method, change the assignColorByID handlebars helper too the 2 serves all the time series type charts
function colorScale(len){

    //colorscale 16777216 = 256 ^ 3
    var diff = parseInt(16777216 / len);

    //array of integers from 0 to 16777216
    var diffAry = [];
    for (x=0; x<len; x++){
        diffAry.push( diff * x)
    }

    //create array
    var colorAry = [];
    var integer;

    //even elements take the color code from the blue range (0,0,255)
    //odd elements take the color code from the red range (255,0,0)
    for  (y=0; y<len; y++){

        if(y%2 == 0){
            integer = diffAry[y/2]
        }else{
            integer = diffAry[Math.floor(len - y/2)]
        }

        var str = (integer.toString(16) + "dddddd")
        var hex = integer.toString(16).length < 6 ? str.substr(0,6) : integer.toString(16)
        colorAry.push( hex )
    }

    return colorAry


}





/** DATE, TIME RELATED METHODS **/

/** @return A {"size": x, "unit": y} object that best describes @param millis */
function describeMillis(millis) {
    var descriptors = [
        [2592000000, "MONTHS"],
        [604800000, "WEEKS"],
        [86400000, "DAYS"],
        [3600000, "HOURS"]
    ]

    for (var i = 0; i < descriptors.length; i++) {
        if (millis >= descriptors[i][0] && millis % descriptors[i][0] == 0) {
            return {
                "sizeMillis": descriptors[i][0],
                "size": millis / descriptors[i][0],
                "unit": descriptors[i][1]
            }
        }
    }

    return null
}

function toMillis(size, unit) {
    if (unit == 'SECONDS') {
        return size * 1000
    } else if (unit == 'MINUTES') {
        return size * 60 * 1000
    } else if (unit == 'HOURS') {
        return size * 60 * 60 * 1000
    } else if (unit == 'DAYS') {
        return size * 24 * 60 * 60 * 1000
    } else if (unit == 'WEEKS') {
        return size * 7 * 24 * 60 * 60 * 1000
    } else if (unit == 'MONTHS') {
        return size * 30 * 24 * 60 * 60 * 1000
    }
}

function getLocalTimeZone() {
    var timeZone = jstz()
    var utcOffset = timeZone.utc_offset
    var utcOffsetHours = Math.abs(utcOffset) / 60
    var utcOffsetMinutes = Math.abs(utcOffset) % 60
    var utcOffsetMagnitude = Math.abs(utcOffsetHours)

    var formatted = ""
    formatted += utcOffset < 0 ? "-" : ""
    formatted += utcOffsetMagnitude < 10 ? "0" + utcOffsetMagnitude : utcOffsetMagnitude
    formatted += ":"
    formatted += utcOffsetMinutes < 10 ? "0" + utcOffsetMinutes : utcOffsetMinutes
    formatted += " " + timeZone.timezone_name

    return formatted
}

/**
 * Get Time Zone
 * @function
 * @public
 * @returns {String} Local timezone from getLocalTimeZone() or hash params if present
 * timezone
 */
function getTimeZone() {
    var timeZone = jstz()
    if(window.location.hash) {
        var params = parseHashParameters(window.location.hash)
        if(params.timezone) {
            var tz = params.timezone.split('-').join('/')
        } else {
            var tz = timeZone.timezone_name
        }
    } else {
        var tz = timeZone.timezone_name
    }
    return tz
}

/**
 * @function
 * @public
 * @returns   Assign background color value to  heat-map-cell **/
function  calcHeatMapCellBackground(cell){

    var cellObj = $(cell)
    var value = parseFloat(cellObj.attr('value'))
        value = value / 100;

    var absValue = Math.abs(value)

    if (value < 0) {
        cellObj.css('background-color', 'rgba(255,0,0,' + absValue + ')') // red
    } else {
        cellObj.css('background-color', 'rgba(0,0,255,' + absValue + ')') // blue
    }

    var colorIsLight = function (a) {
        // Counting the perceptive luminance

        return (a < 0.5);
    }
    var textColor = colorIsLight(absValue) ? 'black' : 'white';
    cellObj.css('color', textColor);
};


/** Transform UTC time into user selected or browser's timezone and display the date value **/
function transformUTCToTZDate(element){
    var elementObj = $(element);
    var tz = getTimeZone()
    var currentTime = moment(elementObj.attr('currentUTC'));
    elementObj.html(currentTime.tz(tz).format('YY-MM-DD z'));
    var baselineTime = moment(elementObj.attr('title'));
    elementObj.attr('title', baselineTime.tz(tz).format('MM-DD HH:mm z'));
};

/** Transform UTC time into user selected or browser's timezone and display the time value **/
function transformUTCToTZTime(cell, format){

    var cellObj = $(cell);
    var tz = getTimeZone()
    var currentTime = moment(cellObj.attr('currentUTC'));
    cellObj.html(currentTime.tz(tz).format(format));
    var baselineTime = moment(cellObj.attr('title'));
    cellObj.attr('title', baselineTime.tz(tz).format('MM-DD HH:mm'));
};

/** Transform UTC time into user selected or browser's timezone and display the date value
 + * takes DOM element, date format returns date string in date format **/
    function transformUTCMillisToTZDate(element, format){
    var elementObj = $(element);
    var tz = getTimeZone();

    var baselineMillis = parseInt(elementObj.attr('title'));
    var currentMillis = parseInt(elementObj.attr('currentUTC'));

    var baselineTime = moment(baselineMillis);
    var currentTime = moment(currentMillis );

    elementObj.html(currentTime.tz(tz).format('YY-MM-DD z'));
    elementObj.attr('title', baselineTime.tz(tz).format(format));
};

/** Transform UTC time into user selected or browser's timezone and display the time value **/
    /** Transform UTC time into user selected or browser's timezone and display the time value,
      * takes DOM element, time format returns date string in date format**/
        function transformUTCMillisToTZTime(cell, format){

        var cellObj = $(cell);
        var tz = getTimeZone();

        var currentMillis = parseInt(cellObj.attr('currentUTC'));
        var baselineMillis = parseInt(cellObj.attr('title'));

        var currentTime = moment(currentMillis);
        var baselineTime = moment(baselineMillis);

        cellObj.html(currentTime.tz(tz).format(format));
        cellObj.attr('title', baselineTime.tz(tz).format(format));
    };

/** Transform UTC time into user selected or browser's timezone **/
function transformUTCToTZ() {

    //Contributors view
    $(".contributors-table-date").each(function (dindex, cell) {
        var dateFormat = "YYYY-MM-DD";
        transformUTCMillisToTZDate(cell, dateFormat)
    });
    $(".table-time-cell").each(function (i, cell) {
        var dateTimeFormat = "h a";
        if(hash.hasOwnProperty("aggTimeGranularity") && hash.aggTimeGranularity == "DAYS"){
            dateTimeFormat = "MM-DD h a"
        }

        transformUTCMillisToTZTime(cell, dateTimeFormat);
    });

    //Dashboard and tabular view
    $(".funnel-table-time").each(function (i, cell) {
        var dateTimeFormat = "h a";

        if(hash.hasOwnProperty("aggTimeGranularity") && hash.aggTimeGranularity == "DAYS"){
           
            dateTimeFormat = "MM-DD h a"
        }
        transformUTCToTZTime(cell, dateTimeFormat);
    });

};

/**Transform UTC time into user selected or browser's timezone **/
function formatMillisToTZ() {
    $(".table-time-cell").each(function (i, cell) {


        var dateTimeFormat = "h a";
        if(hash.hasOwnProperty("aggTimeGranularity") && hash.aggTimeGranularity == "DAYS"){

            dateTimeFormat = "MM-DD h a"
        }
        var cellObj = $(cell);
        var tz = getTimeZone()
        var currentTime = moment(parseInt(cellObj.attr('currentStartUTC')));
        cellObj.html(currentTime.tz(tz).format(dateTimeFormat));
    });

};

/** CONTRIBUTORS TABLE RELATED METHODS **/
/** Loop through each columns that's not displaying ratio values,
 take the total of the cells' value in the column (if the row of the cell is checked  and the value id not N/A) and place the total into the total row.
 Then calculate the sum row ratio column cell value based on the 2 previous column's value.  **/
function sumColumn(col){

    var currentTable =  $(col).closest("table");

    var currentMetric =  $(col).closest(".metric-section-wrapper").attr("rel");
    var firstDataRow = $("tr.data-row", currentTable)[0];
    var columns = $("td",firstDataRow);
    var isCumulative = $($("input.cumulative")[0]).hasClass("uk-active");

    //Work with the cumulative or hourly total row

    var sumRow = (isCumulative) ?  $("tr.cumulative-values.sum-row",  currentTable)[0] : $("tr.discrete-values.sum-row",  currentTable)[0];

    //Only summarize for primitive metrics. Filter out derived metrics ie. RATIO(), for those metrics total value is N/A since that would add up the nominal % values
    if(currentMetric.indexOf("RATIO(") == -1 ){

        //Loop through each column, except for column index 0-2 since those have string values
        for(var z= 3, len = columns.length; z < len; z++){


            //Filter out ratio columns only calc with value columns
            if( (z + 1 ) % 3 !== 0 ){

                var rows =  (isCumulative) ? $("tr.data-row", currentTable) : $("tr.data-row", currentTable)

                //Check if cumulative table is displayed
                var sum = 0;

                for(var i= 0, rlen = rows.length; i < rlen; i++){

                    //Check if checkbox of the row is selected
                    if( $("input", rows[i]).is(':checked')) {
                        var currentRow = rows[i];
                        var currentCell = $("td", currentRow)[z];
                        var currentCellVal =  parseInt($(currentCell).html().trim().replace(/[\$,]/g, ''));
                        //NaN value will be skipped
                        if (!isNaN(currentCellVal)) {
                            sum = sum + currentCellVal;
                        }
                    }
                }

                //Display the sum in the current column of the sumRow
                var sumCell = $("th", sumRow)[z];
                $(sumCell).html(sum);

                //In case of ratio columns calculate them based on the baseline and current values of the timebucket
            }else{
                //take the 2 previous total row elements
                var baselineValCell = $("th", sumRow)[z-2];
                var currentValCell = $("th", sumRow)[z-1];
                var baselineVal = parseInt($(baselineValCell).html().trim().replace(/[\$,]/g, ''));
                var currentVal = parseInt($(currentValCell).html().trim().replace(/[\$,]/g, ''));
                var sumCell = $("th", sumRow)[z];
                //Round the ratio to 2 decimal places, add 0.00001 to prevent Chrome rounding 0.005 to 0.00
                var ratioVal = (Math.round(((currentVal - baselineVal) / baselineVal + 0.00001) * 1000)/10).toFixed(1);

                $(sumCell).html(ratioVal + "%");
                $(sumCell).attr('value' , (ratioVal /100));
                calcHeatMapCellBackground(sumCell);
            }
        }

        //If the metric is a derived metric = has RATIO() form display N/A in the total row
    }else{
        var sumCells = $("th", sumRow);
        for(var i = 3, tLen = sumCells.length; i< tLen; i++){
            $(sumCells[i]).html("N/A");
        }
    }

}

/** @function Assign background color value to each heat-map-cell
 * @public
 * @returns  background color **/
function calcHeatMapBG(){
    $(".heat-map-cell").each(function (i, cell) {
        calcHeatMapCellBackground(cell);
    })
};





