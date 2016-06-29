
/**--- Eventlisteners on anomalies form ---**/

/** Dataset selection **/
$("#main-view").on("click",".dataset-option-manage-alert", function(){
    selectAnomalyDataset(this)
});

$("#main-view").on("keyup", "#rule", function(){
    nameRule();
})

/** Metric selection **/
$("#main-view").on("click",".single-metric-option-manage-alert", function(){
    selectAnomalyMetric(this)
});

/** Condition selection **/
$("#main-view").on("click",".anomaly-condition-option", function(){
    selectAnomalyCondition(this)
});

/** Threshold **/
$("#main-view").on("keyup","#anomaly-threshold", function(){
    setAnomalyThreshold()
});


/** Compare mode selection **/
$("#main-view").on("click",".anomaly-compare-mode-option", function(){
    selectAnomalyCompareMode(this)
});

/** Monitoring window size selection **/
$("#main-view").on("keyup, click","#monitoring-window-size", function(){
    setMonitoringWindowSize()
});

/** Monitoring repeat size selection **/
$("#main-view").on("click",".monitoring-window-unit-option", function(){
    selectMonitoringWindowUnit(this)

});

/** Monitoring window unit selection **/
$("#main-view").on("keyup, click","#monitoring-repeat-size", function(){
    setMonitoringRepeatSize()
});

/** Monitoring repeat unit selection**/
$("#main-view").on("click",".anomaly-monitoring-repeat-unit-option", function(){
    selectAnomalyMonitoringRepeatUnit(this)
});

$("#main-view").on("click","#save-alert", function(){
    saveAlert()
});


function nameRule(){
    //If previously error was shown hide it
    if($("#manage-alert-error").attr("data-error-source") == "rule"){
        $("#manage-alert-error").hide();
    }

    //Hide success message
    $("#manage-alert-success").hide();
}

function selectAnomalyDataset(target){
    var value = $(target).attr("value");
    //Populate the selected item on the form element
    $("#selected-anomaly-dataset").text($(target).text());
    $("#selected-anomaly-dataset").attr("value",value);


    var url = "/dashboard/data/metrics?dataset=" + value;
    getData(url).done(function (data) {

        /* Handelbars template for manage anomalies form metric list */
        var anomalyFormMetricListData = {data: data, scope: "-manage-alert", singleMetricSelector: true};
        var result_anomaly_form_metric_list_template = HandleBarsTemplates.template_metric_list(anomalyFormMetricListData);
        $(".manage-alert-metric-list").each(function(){ $(this).html(result_anomaly_form_metric_list_template)});
    });

    //close uikit dropdown
    $(target).closest("[data-uk-dropdown]").removeClass("uk-open");
    $(target).closest("[data-uk-dropdown]").attr("aria-expanded", false);

    //If previously error was shown hide it
    if($("#manage-alert-error").attr("data-error-source") == "dataset-option-manage-alert"){
        $("#manage-alert-error").hide();
    }

    //Hide success message
    $("#manage-alert-success").hide();
}

function selectAnomalyMetric(target){
    var value = $(target).attr("value");

    //Populate the selected item on the form element
    $("#selected-metric-manage-alert").text($(target).text());
    $("#selected-metric-manage-alert").attr("value",value);

    //If previously error was shown hide it
    if($("#manage-alert-error").attr("data-error-source") == "single-metric-option-manage-alert") {
        $("#manage-alert-error").hide();
    }

    //Hide success message
    $("#manage-alert-success").hide();
};

function selectAnomalyCondition(target){
    var value = $(target).attr("value");
    //Populate the selected item on the form element
    $("#selected-anomaly-condition").text($(target).text());
    $("#selected-anomaly-condition").attr("value",value);

    //If previously error was shown hide it
    if($("#manage-alert-error").attr("data-error-source") == "anomaly-condition") {
        $("#manage-alert-error").hide();
    }

    //Hide success message
    $("#manage-alert-success").hide();
};


function setAnomalyThreshold(){

    //If previously error was shown hide it
    if($("#manage-alert-error").attr("data-error-source") == "anomaly-threshold") {
        $("#manage-alert-error").hide();
    }

    //Hide success message
    $("#manage-alert-success").hide();
};

function selectAnomalyCompareMode(target){
    var value = $(target).attr("unit");
    //Populate the selected item on the form element
    $("#selected-anomaly-compare-mode").text($(target).text());
    $("#selected-anomaly-compare-mode").attr("value",value);
};


function setMonitoringWindowSize(){

    //If previously error was shown hide it
    if($("#manage-alert-error").attr("data-error-source") == "monitoring-window-size") {
        $("#manage-alert-error").hide();
    }

    //Hide success message
    $("#manage-alert-success").hide();

};


function selectMonitoringWindowUnit(target){

    var unit = $(target).attr("unit");

    //Update selectors
    $("#selected-monitoring-window-unit").text($(target).text());
    $("#selected-monitoring-window-unit").attr("unit", unit);

    //close uikit dropdown
    $(target).closest("[data-uk-dropdown]").removeClass("uk-open");
    $(target).closest("[data-uk-dropdown]").attr("aria-expanded", false);

};

function setMonitoringRepeatSize(){

    //If previously error was shown hide it
    if($("#manage-alert-error").attr("data-error-source") == "monitoring-repeat-size") {
        $("#manage-alert-error").hide();
    }

    //Hide success message
    $("#manage-alert-success").hide();

};

function selectAnomalyMonitoringRepeatUnit(target){

    var unit = $(target).attr("unit");

    //Update selectors
    $("#selected-monitoring-repeat-unit").text($(target).text());
    $("#selected-monitoring-repeat-unit").attr("unit", unit);
    //close uikit dropdown
    $(target).closest("[data-uk-dropdown]").removeClass("uk-open");
    $(target).closest("[data-uk-dropdown]").attr("aria-expanded", false);

    if(unit == "DAYS" ){
        $("#monitoring-schedule").removeClass("hidden");
    }else if(unit == "HOURS"){
        $("#monitoring-schedule").addClass("hidden");
        $("#monitoring-schedule-time").val("")
    }

};

function saveAlert(){

    //Close uikit dropdowns
    $("[data-uk-dropdown]").removeClass("uk-open");
    $("[data-uk-dropdown]").attr("aria-expanded", false);
    $(".uk-dropdown").addClass("hidden");


    //Currently only supporting 'user rule' type alert configuration on the front end
    // KALMAN and SCAN Statistics are set up by the backend
    var type = "USER_RULE";
    var windowDelay =  "1";  //Todo:consider max time ?


    //Collect the form values
    var functionName = $("#rule").val();
    var dataset = $("#selected-anomaly-dataset").attr("value");
    var metric = $("#selected-metric-manage-alert").attr("value");
    var condition =( $("#selected-anomaly-condition").attr("value") == "DROPS" ) ? "-" :  ( $("#selected-anomaly-condition").attr("value") == "INCREASES" )  ? "" : null;
    var changeThreshold = parseFloat( $("#anomaly-threshold").val() / 100);
    var windowSize = $("#monitoring-window-size").val();
    var windowUnit = $("#selected-monitoring-window-unit").attr("unit");
    var repeatEverySize = $("#monitoring-repeat-size").val();
    var repeatEveryUnit = $("#selected-monitoring-repeat-unit").attr("unit");

    var monitoringScheduleTime = $("#monitoring-schedule-time").val() == "" ? $("#monitoring-schedule-time").val() : "00:00"; //Todo: in case of daily data granularity set the default schedule to time when datapoint is created
    var scheduleMinute = monitoringScheduleTime.substring(3, monitoringScheduleTime.length);
    var scheduleHour = monitoringScheduleTime.substring(0, monitoringScheduleTime.length -3);

    if($("#active-alert").is(':checked')){
       var isActive = true;
    }else{
       var isActive = false;
    }

    var exploreDimension = "";
    var properties = "";


    /* Validate form */

    var errorMessage = $("#manage-alert-error p");
    var errorAlert = $("#manage-alert-error");

    //Check if rule name is present
    if(functionName == ""){
        errorMessage.html("Please give a name to the rule in the 'Rule' field.");
        errorAlert.attr("data-error-source", "rule");
        errorAlert.fadeIn(100);
        return
    }

    //Check if rule name is alphanumeric
    function isAlphaNumeric(str) {
        for (var i = 0, len = str.length; i < len; i++) {
            var code = str.charCodeAt(i);
            if (!(code > 47 && code < 58) && // numeric (0-9)
                !(code > 64 && code < 91) && // upper alpha (A-Z)
                !(code > 96 && code < 123)) { // lower alpha (a-z)
                return false;
            }
        }
        return true;
    };
    if(!isAlphaNumeric(functionName)){
        errorMessage.html("Please only use alphanumeric (0-9, A-Z, a-z) characters in 'Rule' field. No space, no '_', no '-' is accepted.");
        errorAlert.attr("data-error-source", "rule");
        errorAlert.fadeIn(100);
        return
    }

    //Check if dataset is selected
    if(dataset == "") {
        errorMessage.html("Please select a dataset.");
        errorAlert.attr("data-error-source", "dataset-option-manage-alert");
        errorAlert.fadeIn(100);
        return
    }

    //Check if metric is selected
    if(metric == "") {
        errorMessage.html("Please select a metric.");
        errorAlert.attr("data-error-source", "single-metric-option-manage-alert");
        errorAlert.fadeIn(100);
        return
    }

    //Check if condition is selected
    if(condition == null ) {
        errorMessage.html("Please select a condition ie. DROP, INCREASE.");
        errorAlert.attr("data-error-source", "anomaly-condition");
        errorAlert.fadeIn(100);
        return
    }

    //Check if threshold < 0 or the value of the input is not a number
    if(!changeThreshold || changeThreshold < 0.01) {
        errorMessage.html("Please provide a threshold percentage using positive numbers greater than 1. Example: write 5 for a 5% threshold. <br> The ratio will be calculated as: (current value - baseline value) / baseline value");
        errorAlert.attr("data-error-source", "anomaly-threshold");
        errorAlert.fadeIn(100);
        return
    }

    //Check if windowSize has value
    if(!windowSize || windowSize < 0){
        errorMessage.html("Please fill in how many consecutive hours/days/weeks/months should fail the threshold to trigger an alert.");
        errorAlert.attr("data-error-source", "monitoring-window-size");
        errorAlert.fadeIn(100);
        return
    }

    //Check if repeatEvery has value
    if(!repeatEverySize) {
        errorMessage.html("Please fill in how frequently should ThirdEye monitor the data.");
        errorAlert.attr("data-error-source", "monitoring-repeat-size");
        errorAlert.fadeIn(100);
        return
    }


    /* Submit form */
    var url = "/dashboard/anomaly-function/create?dataset=" + dataset + "&metric=" + metric + "&type=" + type + "&functionName=" + functionName + "&windowSize=" + windowSize + "&windowUnit=" + windowUnit + "&windowDelay=" + windowDelay + "&scheduleMinute=" + scheduleMinute  + "&scheduleHour=" + scheduleHour + "&repeatEverySize=" + repeatEverySize + "&repeatEveryUnit=" + repeatEveryUnit + "&exploreDimension=" + exploreDimension + "&isActive=" +  isActive + "&properties=baseline=" + "w/w" + ";changeThreshold=" + condition + changeThreshold + ";";

   submitData(url).done(function(){

        var successMessage = $("#manage-alert-success");

        $("p", successMessage).html("success");
        successMessage.fadeIn(100);
   })
}