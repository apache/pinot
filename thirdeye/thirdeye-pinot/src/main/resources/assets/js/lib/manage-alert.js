
/**--- Eventlisteners on anomalies form ---**/



/** Metric selection **/
$("#main-view").on("click",".anomaly-metric-option", function(){
    selectAnomalyMetric(this)
})


/** Metric selection **/
$("#main-view").on("click",".anomaly-dataset-option", function(){
    selectAnomalyDataset(this)
})

$("#main-view").on("click","#save-alert", function(){
    saveAlert()
})



function selectAnomalyDataset(target){
    var value = $(target).attr("value");
    //Populate the selected item on the form element
    $(".selected-anomaly-dataset").text($(target).text());
    $(".selected-anomaly-dataset").attr("value",value);
}

function selectAnomalyMetric(target){
    var value = $(target).attr("value");
    //Populate the selected item on the form element
    $(".selected-anomaly-metric").text($(target).text());
    $(".selected-anomaly-metric").attr("value",value);
}



function saveAlert(){


    /* Grab the values */

    /* Validate form */


    var dataset = "feed_sessions_additive";
    var name = "TEST06104";
    var metric = "feed_sessions_additive";
    var type = "USER_RULE";
    var windowSize = "1";
    var windowUnit = "DAYS";
    var windowDelay =  "1";//consider max time  //Milliseconds?
    var scheduleStartIso = "2016-06-01T08:15:30-05:00";
    var repeatEverySize = "1";
    var repeatEveryUnit =  "HOURS";
    var exploreDimension = "";
    var properties = "";

    /* Submit form */
    var url = "/dashboard/anomaly-function/create?dataset=" + dataset + "&metric=" + metric + "&type=" + type + "&name=" + name + "&windowSize=" + windowSize + "&windowUnit=" + windowUnit + "&windowDelay=" + windowDelay + "&scheduleStartIso=" + scheduleStartIso  + "&repeatEverySize=" + repeatEverySize + "&repeatEveryUnit=" + repeatEveryUnit + "&exploreDimension=" + exploreDimension + "&properties=" + properties;
    submitData(url).done(function(){
        console.log("/dashboard/anomaly-function/create response")
    })
}