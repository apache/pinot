
/**--- Eventlisteners on anomalies form ---**/

/** Dataset selection **/
$("#main-view").on("click",".anomaly-dataset-option", function(){
    selectAnomalyDataset(this)
});

/** Metric selection **/
$("#main-view").on("click",".anomaly-metric-option", function(){
    selectAnomalyMetric(this)
});

/** Condition selection **/
$("#main-view").on("click",".anomaly-condition-option", function(){
    selectAnomalyCondition(this)
});

/** Compare mode selection **/
$("#main-view").on("click",".anomaly-compare-mode-option", function(){
    selectAnomalyCompareMode(this)
});
/** Monitoring window unit selection **/
$("#main-view").on("click",".anomaly-monitoring-window-unit-option", function(){
    selectMonitoringWindowUnit(this)
});

/** Monitoring unit selection**/
$("#main-view").on("click",".anomaly-monitoring-repeat-unit-option", function(){
    selectAnomalyMonitoringRepeatUnit(this)
});

$("#main-view").on("click","#save-alert", function(){
    saveAlert()
});

function selectAnomalyDataset(target){
    var value = $(target).attr("value");
    //Populate the selected item on the form element
    $("#selected-anomaly-dataset").text($(target).text());
    $("#selected-anomaly-dataset").attr("value",value);

    //close uikit dropdown
    $(target).closest("[data-uk-dropdown]").removeClass("uk-open");
    $(target).closest("[data-uk-dropdown]").attr("aria-expanded", false);
}

function selectAnomalyMetric(target){
    var value = $(target).attr("value");
    //Populate the selected item on the form element
    $("#selected-anomaly-metric").text($(target).text());
    $("#selected-anomaly-metric").attr("value",value);
};

function selectAnomalyCondition(target){
    var value = $(target).attr("value");
    //Populate the selected item on the form element
    $("#selected-anomaly-condition").text($(target).text());
    $("#selected-anomaly-condition").attr("value",value);
};

function selectAnomalyCompareMode(target){
    var value = $(target).attr("unit");
    //Populate the selected item on the form element
    $("#selected-anomaly-compare-mode").text($(target).text());
    $("#selected-anomaly-compare-mode").attr("value",value);
};


function selectMonitoringWindowUnit(target){

    var unit = $(target).attr("unit");

    //Update selectors
    $("#selected-anomaly-monitoring-window-unit").text($(target).text());
    $("#selected-anomaly-monitoring-window-unit").attr("unit", unit);

    //close uikit dropdown
    $(target).closest("[data-uk-dropdown]").removeClass("uk-open");
    $(target).closest("[data-uk-dropdown]").attr("aria-expanded", false);

};

function selectAnomalyMonitoringRepeatUnit(target){

    var unit = $(target).attr("unit");

    //Update selectors
    $("#selected-anomaly-monitoring-repeat-unit").text($(target).text());
    $("#selected-anomaly-monitoring-repeat-unit").attr("unit", unit);
    //close uikit dropdown
    $(target).closest("[data-uk-dropdown]").removeClass("uk-open");
    $(target).closest("[data-uk-dropdown]").attr("aria-expanded", false);

    if(unit == "DAYS" ){
        $("#monitoring-schedule").removeClass("hidden")
    }

};

function saveAlert(){


//    //Close uikit dropdowns
    $("[data-uk-dropdown]").removeClass("uk-open");
    $("[data-uk-dropdown]").attr("aria-expanded", false);
    $(".uk-dropdown").hide();

    var dataset = $("#selected-anomaly-dataset").attr("value");

    var functionName = $("#anomaly-name").val();

    var metric = $("#selected-anomaly-metric").attr("value");

    //Currently only supporting 'user rule' type alert configuration on the front end
    // KALMAN and SCAN Statistics are set up by the backend
    var type = "USER_RULE";

    var windowSize = $("#anomaly-monitoring-window-size").attr("value");

    var windowUnit = $("#selected-anomaly-monitoring-window-unit").attr("unit");

    var repeatEverySize = $("#monitoring-repeat-size").val()
    var repeatEveryUnit = $("#selected-anomaly-monitoring-repeat-unit").attr("unit")


   if($("#active-alert").is(':checked')){
       var isActive = true;
    }else{
       var isActive = false;
    }



    var tendency =  ( $("#selected-anomaly-condition ").val() == "DROP" ) ? "-" : "";
    var changeThreshold = parseFloat( $("#anomaly-threshold-input").val() / 100)  ;
    console.log('$("#anomaly-threshold-input").val() ' )
    console.log($("#anomaly-threshold-input").val()  )


    /* Validate form */

    /* Grab the values */
//    var dataset = "feed_sessions_additive";
//    var functionName = "TEST06104";
//    var metric = "feed_sessions_additive";
//    var type = "USER_RULE";
//    var windowSize = "1";
//    var windowUnit = "DAYS";
//    var repeatEverySize = "1";
//    var repeatEveryUnit =  "HOURS";
//    var isActive = true;



    var windowDelay =  "1";//consider max time  //Milliseconds?
//  var scheduleStartIso = "2016-06-23T08:15:30-05:00";
    var scheduleMinute = "00";
    var scheduleHour = "12";

    var exploreDimension = "";
    var properties = "";

    /* Submit form */
    var url = "/dashboard/anomaly-function/create?dataset=" + dataset + "&metric=" + metric + "&type=" + type + "&functionName=" + functionName + "&windowSize=" + windowSize + "&windowUnit=" + windowUnit + "&windowDelay=" + windowDelay + "&scheduleMinute=" + scheduleMinute  + "&scheduleHour=" + scheduleHour + "&repeatEverySize=" + repeatEverySize + "&repeatEveryUnit=" + repeatEveryUnit + "&exploreDimension=" + exploreDimension + "&isActive=" +  isActive + "&properties=baseline=" + "w/w" + ";changeThreshold=" + tendency + changeThreshold + ";";

    submitData(url).done(function(){
        console.log("/dashboard/anomaly-function/create response")
    })
}