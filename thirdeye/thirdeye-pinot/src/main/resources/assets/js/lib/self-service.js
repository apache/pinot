
function getExistingAnomalyFunctions(dataset){
    //removing the query until it returns other than 500 error
    if(dataset) {
        var url = "/dashboard/anomaly-function/view?dataset=" + dataset;
        var tab = "self-service";
        var result_existing_anomaly_functions_template;

        getData(url, tab).done(function (data) {
            if(data){
                window.sessionStorage.setItem('existingAnomalyFunctions', JSON.stringify(data) );
            }
            /** Handelbars template for EXISTING ANOMALY FUNCTIONS TABLE **/
            result_existing_anomaly_functions_template = HandleBarsTemplates.template_existing_anomaly_functions(data);
            $("#existing-anomaly-functions-table-placeholder").html(result_existing_anomaly_functions_template);

            /** Instanciate Datatables on self service view **/
            $("#existing-anomaly-functions-table").DataTable();
        });
    }else{
        /** Handelbars template for EXISTING ANOMALY FUNCTIONS TABLE **/
        result_existing_anomaly_functions_template = HandleBarsTemplates.template_existing_anomaly_functions({});
        $("#existing-anomaly-functions-table-placeholder").html(result_existing_anomaly_functions_template);

        /** Instanciate Datatables on self service view **/
        $("#existing-anomaly-functions-table").DataTable();

    }
}


function renderSelfService(){

    /**--- Eventlisteners on manage alerts form ---**/

    /** Dataset selection **/
    $("#main-view").on("click",".dataset-option-manage-alert", function(){
        selectDatasetNGetFormData(this)
        //selectAnomalyDataset(this)
    });

    $("#main-view").on("keyup", "#name", function(){

        hideErrorAndSuccess("name")

    })

    /** Metric selection **/
    $("#main-view").on("click",".single-metric-option-manage-alert", function(){

        hideErrorAndSuccess("single-metric-option-manage-alert")

    });

    /** Condition selection **/
    $("#main-view").on("click",".anomaly-condition-option", function(){
        hideErrorAndSuccess("anomaly-condition")

    });

    /** Threshold **/
    $("#main-view").on("keyup","#anomaly-threshold", function(){
        hideErrorAndSuccess("anomaly-threshold")
    });


    /** Compare mode selection **/
    $("#main-view").on("click",".anomaly-compare-mode-option", function(){
        hideErrorAndSuccess("anomaly-compare-mode")
    });

    /** Monitoring window size selection **/
    $("#main-view").on("keyup, click","#monitoring-window-size", function(){
        hideErrorAndSuccess("monitoring-window-size")
    });

    /** ExploreDimension and filter selection selection**/
    $("#main-view").on("click",".dimension-option, .remove-filter-selection[tab='self-service'], #self-service-apply-filter-btn", function() {
        hideErrorAndSuccess("")
    });

    /** Manage anomaly tab related listeners **/

    /**  selection**/
    $("#main-view").on("click",".update-function-btn", function() {
        populateUpdateForm(this)
    });


    /** Apply filter **/
//    $("#main-view").on("click","#", function(){
//
//    });

    /** Monitoring repeat size selection **/
    $("#main-view").on("click",".monitoring-window-unit-option", function(){
        selectMonitoringWindowUnit(this)
        hideErrorAndSuccess("")
    });

    /** Monitoring window unit selection **/
    $("#main-view").on("keyup, click","#monitoring-repeat-size", function(){
        hideErrorAndSuccess("monitoring-repeat-size")
    });

    /** Monitoring repeat unit selection**/
    $("#main-view").on("click",".anomaly-monitoring-repeat-unit-option", function(){
        selectAnomalyMonitoringRepeatUnit(this)
    });

    $("#main-view").on("click","#clear-create-form", function(){
        clearCreateForm()
    });

    $("#main-view").on("click","#create-anomaly-function", function(){
        createAnomalyFunction()
    });

    $("#main-view").on("click","#delete-anomaly-function", function(){
        deleteAnomalyFunction(this)
    });

    function selectMonitoringWindowUnit(target){

        var value = $(target).attr("value");
        //Populate the selected item on the form element
        $("#selected-monitoring-window-unit").text($(target).text());
        $("#selected-monitoring-window-unit").attr("value",value);
        //close uikit dropdown
        $(target).closest("[data-uk-dropdown]").removeClass("uk-open");
        $(target).closest("[data-uk-dropdown]").attr("aria-expanded", false);
    };

    function hideErrorAndSuccess(source) {

        //If previously error was shown hide it
        if($("#manage-alert-error").attr("data-error-source") == source) {
            $("#manage-alert-error").hide();
        }

        //Hide success message
        $("#manage-alert-success").hide();
    }

    function selectAnomalyMonitoringRepeatUnit(target){
        var value = $(target).attr("value");
        if(value == "DAYS" ){

            //Display the inputfield for hours and timezone next to the hours
            var timezone = getTimeZone();  //example: America/Los_Angeles
            $("#local-timezone").html(moment().tz(timezone).format("z"));  //example: PST
            $("#monitoring-schedule").removeClass("hidden");

        }else if(value == "HOURS"){
            $("#monitoring-schedule").addClass("hidden");
            $("#monitoring-schedule-time").val("")
        }
    };

    function createAnomalyFunction(){

        //Close uikit dropdowns
        $("[data-uk-dropdown]").removeClass("uk-open");
        $("[data-uk-dropdown]").attr("aria-expanded", false);
        $(".uk-dropdown").addClass("hidden");

        //Currently only supporting 'user rule' type alert configuration on the front end
        // KALMAN and SCAN Statistics are set up by the backend
        var type = "USER_RULE";
        var metricFunction = "SUM";
        var windowDelay =  "1";  //Todo:consider max time ?


        //Collect the form values
        var functionName = $("#name").val();
        var dataset = $(".selected-dataset").attr("value");
        var metric = $("#selected-metric-manage-alert").attr("value");
        var condition =( $("#selected-anomaly-condition").attr("value") == "DROPS" ) ? "-" :  ( $("#selected-anomaly-condition").attr("value") == "INCREASES" )  ? "" : null;
        var changeThreshold = parseFloat( $("#anomaly-threshold").val() / 100);
        var baseline = $("#selected-anomaly-compare-mode").attr("value");
        var windowSize = $("#monitoring-window-size").val();
        var windowUnit = $("#selected-monitoring-window-unit").attr("value");
        var repeatEverySize = $("#monitoring-repeat-size").val();
        var repeatEveryUnit = $("#selected-monitoring-repeat-unit").attr("value");
        var monitoringScheduleTime = $("#monitoring-schedule-time").val() == "" ?  "00:00" : $("#monitoring-schedule-time").val() //Todo: in case of daily data granularity set the default schedule to time when datapoint is created
        var scheduleMinute = monitoringScheduleTime.substring(3, monitoringScheduleTime.length);
        var scheduleHour = monitoringScheduleTime.substring(0, monitoringScheduleTime.length -3);

        if($("#active-alert").is(':checked')){
           var isActive = true;
        }else{
           var isActive = false;
        }

        var filters = readFiltersAppliedInCurrentView("self-service");

        //Transform filters Todo: clarify if filters object should be consistent on FE and BE
        //filters = encodeURIComponent(JSON.stringify(filters));
        var filtersString = "";
        for(key in filters){
            var numValues = filters[key].length;
            for(var index = 0; index < numValues; index++ ){
                filtersString += key + "=" + filters[key][index] + ";";
            }
        }

        var exploreDimension = $("#self-service-view-single-dimension-selector #selected-dimension").attr("value");
        var properties = "";


        /* Validate form */

        var errorMessage = $("#manage-alert-error p");
        var errorAlert = $("#manage-alert-error");

        //Check if rule name is present
        if(functionName == ""){
            errorMessage.html("Please give a name to the anomaly function.");
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
            errorAlert.attr("data-error-source", "name");
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


        //Check if repeatEverySize is positive integer
        function isPositiveInteger(str) {
            return /^\+?[1-9][\d]*$/.test(str);
        }
        if(!isPositiveInteger(repeatEverySize)){
            errorMessage.html('Please fill in: "Monitor data every" X hours/days/weeks etc., where X should be positive integer.');
            errorAlert.attr("data-error-source", "monitoring-repeat-size");
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
        var url = "/dashboard/anomaly-function/create?dataset=" + dataset + "&metric=" + metric + "&type=" + type + "&metricFunction=" + metricFunction + "&functionName=" + functionName
        + "&windowSize=" + windowSize + "&windowUnit=" + windowUnit + "&windowDelay=" + windowDelay
        + "&scheduleMinute=" + scheduleMinute  + "&scheduleHour=" + scheduleHour
        + "&repeatEverySize=" + repeatEverySize + "&repeatEveryUnit=" + repeatEveryUnit
        + "&isActive=" +  isActive + "&properties=baseline=" + baseline + ";changeThreshold=" + condition + changeThreshold;
        url += (exploreDimension) ? "&exploreDimension=" + exploreDimension : "";
        url += (filters) ? "&filters=" + filtersString : "";

        //Disable submit btn
        $("#create-anomaly-function").prop("disabled", true);

       submitData(url).done(function(){
           //Enable submit btn
           $("#create-anomaly-function").prop("disabled", false);

            var successMessage = $("#manage-alert-success");
            $("p", successMessage).html("success");
            successMessage.fadeIn(100);
       })
    }

    function  deleteAnomalyFunction(target){
        var url = "/anomaly-function/delete";
        //Todo
    }

    function populateUpdateForm(target){

        var functionId = $(target).attr("data-row-id");
        var existingAnomalyFunctionsDataStr = window.sessionStorage.getItem('existingAnomalyFunctions');
        var existingAnomalyFunctionsData = JSON.parse(existingAnomalyFunctionsDataStr);

        console.log("functionId")
        console.log(functionId)
        console.log("existingAnomalyFunctionsData")
        console.log(existingAnomalyFunctionsData)
        console.log('existingAnomalyFunctionsData.functionId')
        console.log(existingAnomalyFunctionsData[functionId])
        //existingAnomalyFunctionsData[functionId]
        /** Handelbars template for ANOMALY FUNCTION FORM **/
        var result_anomaly_function_form_template = HandleBarsTemplates.template_anomaly_function_form(existingAnomalyFunctionsData[functionId]);
        $("#update-anomaly-functions-form-placeholder").html(result_anomaly_function_form_template);
    }

    function  updateAnomalyFunction(target){

    }

}