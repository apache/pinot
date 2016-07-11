/** AJAX and HASH RELATED METHODS **/

function getData(url, tab){
    console.log("request url:", url)

    tab = tab ? tab : hash.view;
    return $.ajax({
        url: url,
        type: 'get',
        dataType: 'json',
        statusCode: {
            404: function() {
                $("#"+  tab  +"-chart-area-error").empty()
                var warning = $('<div></div>', { class: 'uk-alert uk-alert-warning' })
                warning.append($('<p></p>', { html: 'No data available. (Error code: 404)' }))
                $("#"+  tab  +"-chart-area-error").append(warning)
                $("#"+  tab +"-chart-area-error").fadeIn(100);
                return
            },
            500: function() {
                $("#"+  tab  +"-chart-area-error").empty()
                var error = $('<div></div>', { class: 'uk-alert uk-alert-danger' })
                error.append($('<p></p>', { html: 'Internal server error' }))
                $("#"+  tab  +"-chart-area-error").append(error)
                $("#"+  tab  +"-chart-area-error").fadeIn(100);
                return
            }
        }
        ,
        beforeSend: showLoader(tab)
    }).always(function(){
      hideLoader(tab);
      if(tab != "anomalies"){
        $("#"+  tab  +"-display-chart-section").empty();
      }
    })
}

function submitData(url){
    return $.ajax({
        url: url,
        type: 'post',
        dataType: 'json',
        //contentType: 'application/json',
        //data: data,
        //processData: false,
        success: function( data, textStatus, jQxhr ){
            //$('#response pre').html( JSON.stringify( data ) );
        },
        error: function( jqXhr, textStatus, errorThrown ){
            console.log( errorThrown );
        }
        //,
        //beforeSend: showLoader(tab)
    }).always(function(){

    })

}

function showLoader(tab){
   $("#"+  tab  +"-chart-area-loader").show();
}

function hideLoader(tab){
    $("#"+  tab +"-chart-area-loader").hide();

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

            if(hash.view == "anomalies"){
                $(".single-metric-option[value='" + metricAry[i] + "']", currentForm).click();
            }else{
                $(".metric-option[value='" + metricAry[i] + "']", currentForm).click();
            }
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

    //click the first filter-dimension-option so the dimension values are visible for that dimension
    $(".filter-dimension-option:first-of-type").click();


    //UPDATE DATE TIME
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
        currentStartDateString = currentStartDateTime.format("YYYY-MM-DD");
        currentStartTimeString = currentStartDateTime.format("HH:mm");

    } else if(maxMillis){
        // populate max date -1 day
        currentStartDateTime = moment(maxMillis).add(-1, 'days');

        //If time granularity is DAYS have default 7 days in the time selector on pageload
        if(window.datasetConfig.dataGranularity && window.datasetConfig.dataGranularity == "DAYS"){
            currentStartDateTime = moment(maxMillis).add(-7, 'days');
        }

        currentStartDateString = currentStartDateTime.format("YYYY-MM-DD");
        currentStartTimeString = currentStartDateTime.format("HH:00");

        if(datasetConfig.dataGranularity && datasetConfig.dataGranularity == "DAYS"){
            currentStartTimeString = "00:00"
        }
    }else{

        // populate todays date
        currentStartDateTime = moment();
        currentStartDateString = currentStartDateTime.format("YYYY-MM-DD");
        currentStartTimeString = currentStartDateTime.format("00:00");
    }

    if (hash.hasOwnProperty("currentEnd")) {
        currentEndDateTime = moment(parseInt(hash.currentEnd)).tz('UTC').clone().tz(tz);
        currentEndDateString = currentEndDateTime.format("YYYY-MM-DD");
        currentEndTimeString = currentEndDateTime.format("HH:mm");
    } else if(maxMillis) {

        currentEndDateTime = moment(maxMillis);
        currentEndDateString = currentEndDateTime.format("YYYY-MM-DD");
        currentEndTimeString = currentEndDateTime.format("HH:00");

        if(datasetConfig.dataGranularity && datasetConfig.dataGranularity == "DAYS"){
            currentEndTimeString = "00:00"
        }
    }else{
        currentEndDateTime = moment();
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

    }else{
        $(".remove-filter-selection[tab='" + hash.view + "']").each(function(index,label){
            $(label).click()
        })
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

        //Trigger form submit if enough elements present for a query

        //If hash has dataset && dashboard trigger form submit
        if( hash.hasOwnProperty("dataset")){

            //If dashboard is present in hash and present in current dataset
            //If hash has dataset && dashboard or metric name trigger form submit
            if(hash.view == "dashboard" && hash.hasOwnProperty("dashboard")){

                //if the dashboard is present in the current dataset
                if( $(".dashboard-option[value='"+ hash.dashboard +"']").length>0 ){
                    //Adding random number to hash
                    //for the usecase of pagereload: the hash would not change so ajax would not be triggered
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

/* Event listeners used in multiple instances in FORM area*/
function enableApplyButton(button){
    $(button).prop("disabled", false);
    $(button).removeAttr("disabled");
}




/* Event listeners used in FORM area and chart area*/
function closeAllUIKItDropdowns(){

    $("[data-uk-dropdown]").removeClass("uk-open");
    $("[data-uk-dropdown]").attr("aria-expanded", false);
    $(".uk-dropdown").hide();
}


/** CHART RELATED METHODS **/

//Assigns an hexadecimal colorcode to each element of an array
//If you change this method, change the assignColorByID handlebars helper too the 2 serves all the time series type charts
function assignColorByID(len, index){

    var colorAry =  colorScale(len)

    return colorAry[index]
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
        var hex = integer.toString(16).length < 6 ? "#" + str.substr(0,6) : "#" + integer.toString(16)
        colorAry.push( hex )
    }

    return colorAry

}

function toggleCumulative(){
    $(".cumulative").toggleClass("uk-active");
    $(".discrete-values").toggleClass("hidden");
    $(".cumulative-values").toggleClass("hidden");

    // Todo: feature: redraw metric timeseries using cumulative data
    if ($("#metric-time-series-placeholder").length > 0) {
    }
}

function calcCummulativeTotal(target){
    $(".contributors-table .select_all_checkbox[rel='cumulative']").each(function(){
        $(target).trigger("click");
    });
}


function toggleSumDetails(target){
    if (!$(target).hasClass("uk-active")) {

        $(target).addClass("uk-active");
        $(target).siblings().removeClass("uk-active");

        var timeCells = document
            .getElementsByClassName("table-time-cell");
        var nextState = timeCells[0].getAttribute('colspan') == 1 ? 3
            : 1;
        for (var index = 0, len = timeCells.length; index < len; index++) {
            timeCells[index].setAttribute('colspan', nextState)
        }

        var detailsCells = document
            .getElementsByClassName("details-cell");
        //Alert
        if (detailsCells.length > 1000) {
            console.log('details cells to paint:')
            console.log(detailsCells.length)
        }

        for (var dIndex = 0, detailsLen = detailsCells.length; dIndex < detailsLen; dIndex++) {
            detailsCells[dIndex].classList.toggle("hidden");
        }

        var subheaderCells = document
            .getElementsByClassName("subheader");
        for (var sIndex = 0, subheaderLen = subheaderCells.length; sIndex < subheaderLen; sIndex++) {
            subheaderCells[sIndex].classList.toggle("hidden");
        }

    }
}

function showContributors(target){
    // Change the view to contributors

//    //either dashboard or metrics param is present in hash
//    delete hash.dashboard;
//
//    //switch to time ver time tab
//    hash.view = "compare";
//
//    //set start and end date to the starte and end date of the table
//    var timeBuckets = $("#timebuckets>span")
//    var numTimeBuckets = timeBuckets.length;
//
//    var firstTimeBucketInRow = $("#timebuckets>span")[0];
//    var lastTimeBucketInRow = $("#timebuckets>span")[numTimeBuckets - 1];
//
//    var currentStartUTC = $($("span", firstTimeBucketInRow)[0]).text().trim();
//    var baselineStartUTC = $($("span", firstTimeBucketInRow)[2]).text().trim();
//
//    var currentEndUTC = $($("span", lastTimeBucketInRow)[1]).text().trim();
//    var baselineEndUTC = $($("span", lastTimeBucketInRow)[3]).text().trim();
//
//    hash.baselineStart = baselineStartUTC;
//    hash.baselineEnd = baselineEndUTC;
//    hash.currentStart = currentStartUTC;
//    hash.currentEnd = currentEndUTC;
//
//
//    //check the current granularity of the data on the table
//    var endOfFirstTimeBucket =  $($("span", firstTimeBucketInRow)[1]).text().trim();
//    var diff = parseInt(endOfFirstTimeBucket) - parseInt(currentStartUTC)
//    var diffProperties = describeMillis(diff)
//    var aggTimeGranularity = diffProperties ? diffProperties.unit : "HOURS"
//
//    hash.aggTimeGranularity = aggTimeGranularity
//
//    //set the metrics
//    metrics = [];
//    // Todo: if metric label it's a derived metric so title contains
//    metrics.push($(target).attr("title"))
//    hash.metrics = metrics.toString();
//
//    //select only the first dimension to retrieve less data
//    hash.dimensions = $($(".dimension-option")[0]).attr("value");
//
//    //update hash will trigger window.onhashchange event:
//    // update the form area and trigger the ajax call
//    window.location.hash = encodeHashParameters(hash);



}


/** Compare/Tabular view and dashboard view heat-map-cell click switches the view to compare/heat-map
 * focusing on the timerange of the cell or in case of cumulative values it query the cumulative timerange **/
function showHeatMap(target){
    hash.view = "compare"
    hash.aggTimeGranularity = "aggregateAll"
    cellObj = $(target)
    var timeIndex = cellObj.attr("timeIndex");
    var timeBucketForColumnIndex = $("#timebuckets>span")[timeIndex];

    var currentStartUTC;
    var baselineStartUTC;
    var currentEndUTC;
    var baselineEndUTC;


    var currentSection = $(target).closest(".display-chart-section")
    if( $(".cumulative", currentSection).is(':checked') ){

        var firstTimeBucketInRow = $("#timebuckets>span")[0]
        currentStartUTC = $($("span", firstTimeBucketInRow)[0]).text().trim();
        baselineStartUTC = $($("span", firstTimeBucketInRow)[2]).text().trim();

    }else{
        currentStartUTC = $($("span", timeBucketForColumnIndex)[0]).text().trim();
        baselineStartUTC = $($("span", timeBucketForColumnIndex)[2]).text().trim();
    }

    currentEndUTC = $($("span", timeBucketForColumnIndex)[1]).text().trim();
    baselineEndUTC = $($("span", timeBucketForColumnIndex)[3]).text().trim();

    hash.baselineStart = baselineStartUTC;
    hash.baselineEnd = baselineEndUTC;
    hash.currentStart = currentStartUTC;
    hash.currentEnd = currentEndUTC;
    delete hash.dashboard;
    metrics = [];
    var metricName = cellObj.attr("metricIndex")
    metrics.push(metricName)
    hash.metrics = metrics.toString();

    //update hash will trigger window.onhashchange event:
    // update the form area and trigger the ajax call
    window.location.hash = encodeHashParameters(hash);
}

/**
 * @function
 * @public
 * @returns   Assign background color value to  heat-map-cell **/
function  calcHeatMapCellBackground(cell){

    var cellObj = $(cell)

    var baseForLtZero = 'rgba(255,0,0,'; //lt zero is default red
    var baseForGtZero = 'rgba(0,0,255,'; //gt zero is default blue

    var metric = cellObj.attr('data-metric-name')
    var invertColorMetrics = window.datasetConfig.invertColorMetrics;
    if (typeof invertColorMetrics !== "undefined" && invertColorMetrics.indexOf(metric) > -1) { // invert
      baseForLtZero = 'rgba(0,0,255,'; //lt zero becomes blue
      baseForGtZero = 'rgba(255,0,0,'; //gt zero becomes red
    }

    var value = parseFloat(cellObj.attr('value'))
    value = value / 100;

    var absValue = Math.abs(value)

    if (value < 0) {
        cellObj.css('background-color', baseForLtZero + absValue + ')') // red
    } else {
        cellObj.css('background-color', baseForGtZero + absValue + ')') // blue
    }

    var colorIsLight = function (a) {
        // Counting the perceptive luminance

        return (a < 0.5);
    }
    var textColor = colorIsLight(absValue) ? 'black' : 'white';
    cellObj.css('color', textColor);
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
 * //with adding timezone to the hash we would enable the usage of timezone other than the users local timezone
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
        var baselineMillis = parseInt(cellObj.attr('baselineUTC'));

        var currentTime = moment(currentMillis);
        var baselineTime = moment(baselineMillis);

        cellObj.html(currentTime.tz(tz).format(format));
        cellObj.attr('title', "baseline: "+ baselineTime.tz(tz).format(format));
    };

/** Transform UTC time into user selected or browser's timezone **/
function transformUTCToTZ() {

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






