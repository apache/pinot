function getExistingAnomalyFunctions(dataset) {
    //removing the query until it returns other than 500 error
    if (dataset) {
        var url = "/dashboard/anomaly-function?dataset=" + dataset;
        var tab = "self-service";
        var result_existing_anomaly_functions_template;

        getData(url, tab).done(function (data) {
            if (data) {
                window.sessionStorage.setItem('existingAnomalyFunctions', JSON.stringify(data));
            }
                compileAnomalyFnTable(data);
        });
    } else {
        compileAnomalyFnTable()
    }

    function compileAnomalyFnTable(data) {
        if (!data) {
            data = {}
        }

        for (var i = 0, numAnomalyFn = data.length; i < numAnomalyFn; i++) {

            if(data[i]){
                var properties = data[i].properties;

                if (properties.substr(properties.length - 1) == ";") {
                    properties = properties.slice(0, -1);
                }

                data[i].properties = parseProperties(properties);

                if (data[i].filters) {

                    var filtersAry = data[i].filters.split(";");

                    var filterParams = {}
                    for (var f = 0, len = filtersAry.length; f < len; f++) {
                        var keyValue = filtersAry[f].split("=");
                        var key = keyValue[0]
                        var value = keyValue[1]

                        if (filterParams[key]) {
                            filterParams[key] += "," + value;
                        } else {
                            filterParams[key] = value
                        }
                    }
                    data[i].filters = filterParams;
                }
            }
        }


        /** Handelbars template for EXISTING ANOMALY FUNCTIONS TABLE **/
        result_existing_anomaly_functions_template = HandleBarsTemplates.template_existing_anomaly_functions(data);
        $("#existing-anomaly-functions-table-placeholder").html(result_existing_anomaly_functions_template);

        /** Instanciate Datatables on self service view **/
        $("#existing-anomaly-functions-table").DataTable({
            "bAutoWidth": false,
            "bPaginate":true,
            "sPaginationType":"full_numbers",
            "iDisplayLength": 50,
            "columnDefs": [
                { "targets": 0  },
                { "targets": 1  },
                { "targets": 2  },
                { "targets": 3  },

                { "targets": 4 , "width": "50px", "orderable": false},
                { "targets": 5 , "width": "50px", "orderable": false},
                { "targets": 6 , "width": "50px", "orderable": false}
            ]
        });
    }
}

function addSelfServiceListeners() {

    //Unbind previously added eventlisteners
    $("#self-service-forms-section").off("click")
    $("#self-service-forms-section").off("keyup")


    // The elements of the self-service tab are created on the anomaly-function-form template

    /**--- Eventlisteners on anomaly function form: create and update functionality ---**/

        // COMMON PARAMETERS IN EVERY ANOMALY FUNCTION TYPE
        // Dataset selection
    $("#self-service-forms-section").on("click", ".dataset-option", function () {
        var dataset = $(this).attr("value")
        getExistingAnomalyFunctions(dataset)
    });

    // Name
    $("#self-service-forms-section").on("keyup", "#name", function () {
        var form = $(this).closest("form");
        hideErrorAndSuccess("name",form);
        enableButton($("#create-anomaly-function"));
        enableButton($("#create-run-anomaly-function"));
    });

    //Function type
    $("#self-service-forms-section").on("click", ".function-type-option", function () {

        var form = $(this).closest("form");
        toggleFunctionTypeFields(this,form);
        hideErrorAndSuccess("");
    });

    // Metric
    $("#self-service-forms-section").on("click", ".metric-option", function () {
        var form = $(this).closest("form");
        hideErrorAndSuccess("metric-option",form);
    });

    // Monitoring window size
    $("#self-service-forms-section").on("keyup, click", "#min-consecutive-size", function () {
        var form = $(this).closest("form");
        hideErrorAndSuccess("min-consecutive-size",form);
    });


    // Monitoring window size
    $("#self-service-forms-section").on("keyup, click", "#monitoring-window-size", function () {
        var form = $(this).closest("form");
        hideErrorAndSuccess("monitoring-window-size",form);
    });

    // Monitoring repeat unit selection
    $("#self-service-forms-section").on("click", ".min-consecutive-unit-option", function () {
        var form = $(this).closest("form");
        hideErrorAndSuccess("",form);
    });

    // Monitoring repeat unit selection
    $("#self-service-forms-section").on("click", ".monitoring-window-unit-option", function () {
        var form = $(this).closest("form");
        hideErrorAndSuccess("",form);
    });

    // Monitoring window unit selection
    $("#self-service-forms-section").on("keyup, click", "#monitoring-repeat-size", function () {
        var form = $(this).closest("form");
        hideErrorAndSuccess("monitoring-repeat-size",form);
    });

    // Monitoring repeat unit selection
    $("#self-service-forms-section").on("click", ".anomaly-monitoring-repeat-unit-option", function () {
        var form = $(this).closest("form");
        toggleMonitoringTimeField(this, form);
    });

    //FUNCTION TYPE SPECIFIC PARAMS
    //WEEK_OVER_WEEK_RULE FUNCTION TYPE PARAMS
    // Condition selection
    $("#self-service-forms-section").on("click", ".anomaly-condition-option", function () {
        var form = $(this).closest("form");
        hideErrorAndSuccess("anomaly-condition",form);
    });

    // Condition selection
    $("#self-service-forms-section").on("click", ".anomaly-condition-min-max-option", function () {
        var form = $(this).closest("form");
        toggleMinMaxInput(this, form);
        hideErrorAndSuccess("anomaly-condition",form);
    });

    // Threshold selection
    $("#self-service-forms-section").on("keyup", "#anomaly-threshold, #anomaly-threshold-min, #anomaly-threshold-max", function () {
        var form = $(this).closest("form");
        hideErrorAndSuccess("anomaly-threshold", form);
    });

    // Compare mode selection
    $("#self-service-forms-section").on("click", ".anomaly-compare-mode-option", function () {
        var form = $(this).closest("form");
        hideErrorAndSuccess("anomaly-compare-mode", form);
    });

    // ExploreDimension and filter selection selection
    $("#self-service-forms-section").on("click", ".dimension-option, .remove-filter-selection[tab='self-service'], #self-service-apply-filter-btn", function () {
        var form = $(this).closest("form");
        hideErrorAndSuccess("", form);
    });

    //MIN_MAX_THRESHOLD FUNCTION TYPE PARAMS
    // Threshold
    $("#self-service-forms-section").on("keyup", "#anomaly-threshold-min-max", function () {
        var form = $(this).closest("form");
        hideErrorAndSuccess("anomaly-threshold", form);
    });

    $("#self-service-forms-section").on("keyup", ".fn-type-property", function () {
        var form = $(this).closest("form");
        hideErrorAndSuccess("data-type", form);
    });

    //COMMON LISTENERS

    //Clear form
    $("#self-service-forms-section").on("click", "#clear-create-form", function () {
        clearCreateForm();
    });

    //Create anomaly function
    $("#self-service-forms-section").on("click", "#create-anomaly-function", function () {
        createAnomalyFunction()
    });

    //Create than run anomaly function
    $("#self-service-forms-section").on("click", "#create-run-anomaly-function", function () {

        var callback = runAdhocAnomalyFunction;
        createAnomalyFunction(callback);

    });

    /** Manage anomaly tab related listeners **/

        //Edit button
    $("#self-service-forms-section").on("click", ".init-update-function-btn", function () {
        $("#self-service-chart-area-error").hide();
        populateUpdateForm(this)
    });

    //Delete button - opens modal for confirmation
    $("#self-service-forms-section").on("click", ".init-delete-anomaly-function", function () {
        var functionId = $(this).attr("data-function-id");
        var functionName = $(this).attr("data-function-name");
        $("#confirm-delete-anomaly-function").attr("data-function-id", functionId);
        $("#self-service-chart-area-error").hide();
        $("#function-to-delete").text(functionName);
        $("#delete-anomaly-function-success").hide();
        $("#delete-anomaly-function-error").hide();
        enableButton($("#confirm-delete-anomaly-function"));
    });

    $("#self-service-forms-section").on("click", ".init-toggle-active-state", function () {
        var rowId = $(this).attr("data-row-id");
        var functionName = $(this).attr("data-function-name");
        $("#confirm-toggle-active-state").attr("data-row-id", rowId);
        $("#close-toggle-alert-modal").attr("data-row-id", rowId);
        $("#self-service-chart-area-error").hide();
        $(".function-to-toggle").text(functionName);
        if ($(this).is(':checked')) {
            $("#turn-off-anomaly-function").hide();
            $("#turn-on-anomaly-function").show();
        }else{
            $("#turn-off-anomaly-function").show();
            $("#turn-on-anomaly-function").hide();
        }
        $("#toggle-active-state-success").hide();
        $("#toggle-active-state-error").hide();
        enableButton($("#confirm-toggle-active-state"));
    });

    //Confirm toggle button - turn on/off active state of anomaly function
    $("#self-service-forms-section").on("click", "#close-toggle-alert-modal", function () {

        var rowId = $(this).attr("data-row-id");
        var existingAnomalyFunctionsDataStr = window.sessionStorage.getItem('existingAnomalyFunctions');
        var existingAnomalyFunctionsData = JSON.parse(existingAnomalyFunctionsDataStr);
        var anomalyFunctionObj = existingAnomalyFunctionsData[rowId];

        if(anomalyFunctionObj.isActive ){
            $(".init-toggle-active-state[data-row-id='" + rowId + "']").attr("checked", true);
            $(".init-toggle-active-state[data-row-id='" + rowId + "']").prop("checked", true);
        }else{
            $(".init-toggle-active-state[data-row-id='" + rowId + "']").removeAttr("checked");
            $(".init-toggle-active-state[data-row-id='" + rowId + "']").prop("checked", false);
        }
    });


    //Confirm toggle button
    $("#self-service-forms-section").on("click", "#confirm-toggle-active-state", function () {
        toggleActiveState(this)
    });

    //Confirm delete
    $("#self-service-forms-section").on("click", "#confirm-delete-anomaly-function", function () {
        deleteAnomalyFunction(this)
    });

    //Update button
    $("#self-service-forms-section").on("click", "#update-anomaly-function", function () {
        updateAnomalyFunction()
    });


    //Function type
    $("#self-service-forms-section").on("mousemove", ".tooltip-cell", function () {

        //insert data
        var propertiesString = $(this).attr("data-properties");

        //Remove trailing ","
        if(propertiesString.substr(propertiesString.length - 1) == ","){
            propertiesString = propertiesString.substring(0, propertiesString.length-1);
        }

        var propertiesAry = propertiesString.split(",");
        var html = "";
        for (var i = 0, numProp = propertiesAry.length; i < numProp; i++) {
            var keyValue = propertiesAry[i];
            keyValue = keyValue.split(":")
            html += "<tr><td class='prop-key'>" + keyValue[0] + ": </td><td class='prop-value'>" + keyValue[1] + ",</td></tr>"

        }
        $("#existing-fn-table-tooltip").html(html);
        var conainerOffset = $("#self-service-display-chart-area").offset();
        var mouseOffset = 10;;
        $("#existing-fn-table-tooltip").css("position", "absolute");
        $("#existing-fn-table-tooltip").css("top",  event.clientY- conainerOffset.top);
        $("#existing-fn-table-tooltip").css("left", event.clientX - conainerOffset.left + mouseOffset);
        $("#existing-fn-table-tooltip").removeClass("hidden");

    });

    /** Alerts tab related listeners **/
    $("#main-view").on("click", "#configure-emails-form-table .dataset-option", function(){
       clear();
       $("#configure-emails-form-table #selected-metric").html("Select metric");
        if ($("#manage-alerts-error").attr("data-error-source") == "params") {
            $("#manage-alerts-error").hide();
        }
        $("#manage-alerts-success").hide();
    })

    $("#main-view").on("click", "#configure-emails-form-table .metric-option", function(){
        if ($("#manage-alerts-error").attr("data-error-source") == "params") {
            $("#manage-alerts-error").hide();
        }
        $("#manage-alerts-success").hide();
       fetchEmailIfPresent();
    })

    $("#self-service-forms-section").on("click", "#save-emails", function(){
        saveEmailConfig();
    })

    $("#self-service-forms-section").on("click", ".add-to-linked-function", function(){
        addFunctionToEmail( this );
    })

    $("#self-service-forms-section").on("keydown, click", "#to-address", function(){
        if ($("#manage-alerts-error").attr("data-error-source") == "to-address") {
            $("#manage-alerts-error").hide();
        }
        $("#manage-alerts-success").hide();
    })

    $("#self-service-forms-section").on("click", ".remove-linked-function", function(){
        removeFunctionFromEmail( this );
    })

    /** Event handlers **/

    function toggleFunctionTypeFields(target) {

        var functionType = $(target).attr("id");
        $(".function-type-fields").addClass("uk-hidden");
	$(".dimension-selection-fields").removeClass("uk-hidden");
        $("." + functionType + "-fields").removeClass("uk-hidden");

        //Hide autoPopulated UASER_RULE and MIN_MAX_THRESHOLD to populate only the hard coded customized fields
        $(".WEEK_OVER_WEEK_RULE-fields>table").addClass("uk-hidden");
        $(".WEEK_OVER_WEEK_RULE-fields>.exceed-txt").addClass("uk-hidden");
        $(".MIN_MAX_THRESHOLD-fields>table").addClass("uk-hidden");
        $(".MIN_MAX_THRESHOLD-fields>.exceed-txt").addClass("uk-hidden");
    }

    function toggleMinMaxInput(target, form) {
        var value = $(target).attr("value");
        if (value == "MIN") {

            $("#anomaly-threshold-max, #and").addClass("uk-hidden");
            $("#anomaly-threshold-max", form).attr("value", "");
            $("#anomaly-threshold-max", form).val("");
            $("#anomaly-threshold-min", form).removeClass("uk-hidden");

        } else if (value == "MAX") {
            $("#anomaly-threshold-min, #and").addClass("uk-hidden");
            $("#anomaly-threshold-min", form).attr("value", "");
            $("#anomaly-threshold-min", form).val("");
            $("#anomaly-threshold-max", form).removeClass("uk-hidden");

        } else {
            $("#anomaly-threshold-max, #anomaly-threshold-min, #and", form).removeClass("uk-hidden");
        }
    };

    function hideErrorAndSuccess(source, form) {

        //If previously error was shown hide it
        if ($("#manage-anomaly-fn-error").attr("data-error-source") == source) {
            $("#manage-anomaly-fn-error", form).hide();
        }

        //Hide success message
        $("#manage-anomaly-function-success", form).hide();
    }

    function toggleMonitoringTimeField(target, form) {
        var value = $(target).attr("value");
        if (value == "DAYS") {

            //Display the inputfield for hours and timezone next to the hours
            //Todo: support local timezone in cron encoding
            // var timezone = getTimeZone();  //example: America/Los_Angeles
            //$("#schedule-timezone", form).html("UTC");  //moment().tz(timezone).format("z") example: PST
            $("#monitoring-schedule", form).removeClass("uk-hidden");
            $("#monitoring-repeat-size", form).addClass("uk-hidden");
            $("#monitoring-repeat-size", form).val("")


        } else if (value == "HOURS") {
            $("#monitoring-schedule", form).addClass("uk-hidden");
            $("#monitoring-schedule-time", form).val("")
            $("#monitoring-repeat-size", form).removeClass("uk-hidden");
        }
    };

    //Takes the css selector of the placeholder of the form
    function collectAnomalyFnData(form) {
        var formData = {};
        formData.properties = {};
        formData.properties_meta = {};

        //Close uikit dropdowns
        $("[data-uk-dropdown]").removeClass("uk-open");
        $("[data-uk-dropdown]").attr("aria-expanded", false);
        $(".uk-dropdown").addClass("hidden");

        /** Collect the form values **/
        formData.dataset = $(".selected-dataset", form).attr("value");
        formData.functionName = $("#name", form).val();
        formData.metric = $("#selected-metric-manage-anomaly-fn", form).attr("value");
        formData.functionType = $("#selected-function-type", form).attr("value");
        formData.metricFunction = "SUM";
        formData.windowDelay = "1";
        formData.minConsecutiveSize = $("#min-consecutive-size", form).val(); //
        formData.minConsecutiveUnit = $("#selected-min-consecutive-unit", form).attr("value");
        formData.windowSize = $("#monitoring-window-size", form).val(); //
        formData.windowUnit = $("#selected-monitoring-window-unit", form).attr("value");
        formData.repeatEverySize = $("#monitoring-repeat-size", form).val() ? $("#monitoring-repeat-size", form).val() : 1;
        formData.repeatEveryUnit = $("#selected-monitoring-repeat-unit", form).attr("value");
        var monitoringScheduleTime = ( $("#monitoring-schedule-time", form).val() == "" ) ? "" : $("#monitoring-schedule-time", form).val() //Todo: in case of daily data granularity set the default schedule to time when datapoint is created
        if(monitoringScheduleTime){
            if(monitoringScheduleTime.indexOf(",") > -1){
                formData.scheduleHour = monitoringScheduleTime;
                formData.scheduleMinute = "";
            }else{
                //schedule time format HH:MM
                var monitoringScheduleTimeAry = monitoringScheduleTime.split(":")
                formData.scheduleHour = monitoringScheduleTimeAry[0];
                formData.scheduleMinute = monitoringScheduleTimeAry[1];
            }
        }
        if ($("#function-id", form).length > 0) {
            formData.functionId = $("#function-id", form).text();
        }

        if ($("#active-alert", form).is(':checked')) {
            formData.isActive = true;
        } else {
            formData.isActive = false;
        }

        //WEEK_OVER_WEEK_RULE & MIN_MAX_THRESHOLD
        var filters = readFiltersAppliedInCurrentView("self-service", {form: form});

        formData.filters = encodeURIComponent(JSON.stringify(filters));
        formData.exploreDimension = $("#self-service-view-single-dimension-selector #selected-dimension", form).attr("value");


        //Function type params
        //WEEK_OVER_WEEK_RULE Params
        switch(formData.functionType){
            case "WEEK_OVER_WEEK_RULE":
                formData.condition = ( $("#selected-anomaly-condition", form).attr("value") == "DROPS" ) ? "-" : ( $("#selected-anomaly-condition", form).attr("value") == "INCREASES" ) ? "" : null;
                formData.baseline = $("#selected-anomaly-compare-mode", form).attr("value");
                formData.changeThreshold = parseFloat($("#anomaly-threshold", form).val() / 100);
                //WEEK_OVER_WEEK_RULE & MIN_MAX_THRESHOLD
                var filters = readFiltersAppliedInCurrentView("self-service", {form: form});

                //Transform filters
                formData.filters = encodeURIComponent(JSON.stringify(filters));
                formData.exploreDimension = $("#self-service-view-single-dimension-selector #selected-dimension", form).attr("value");
            break;
            case "MIN_MAX_THRESHOLD":
                formData.min = $("#anomaly-threshold-min", form).val();
                formData.max = $("#anomaly-threshold-max", form).val();
                //WEEK_OVER_WEEK_RULE & MIN_MAX_THRESHOLD
                var filters = readFiltersAppliedInCurrentView("self-service", {form: form});

                //Transform filters Todo: clarify if filters object should be consistent on FE and BE
                formData.filters = encodeURIComponent(JSON.stringify(filters));
                formData.exploreDimension = $("#self-service-view-single-dimension-selector #selected-dimension", form).attr("value");
            break;
            default:
                var context = $("." + formData.functionType + "-fields", form)[0];
                var fields = $("input", context);

                for (var i=0, len =fields.length; i<len; i++) {

                    var $inputField = $(fields[i]);
                    var expectedDataType = $inputField.attr("data-expected");
                    var selector = $inputField.attr("data-property");
                    var value = $inputField.val();
                    formData["properties"][selector] = value;
                    formData["properties_meta"][selector] = expectedDataType;
                }
            break;

        }

        return formData;
    }


    //VALIDATE FORM: takes an object returns true or undefined = falsy value
    function validateAnomalyFnFormData(formData, form) {

        var valid = true;

        /* Validate form */
        var errorMessage = $("#manage-anomaly-fn-error p", form);
        var errorAlert = $("#manage-anomaly-fn-error", form);

        //Check if rule name is present
        if (formData.functionName == "") {
            errorMessage.html("Please give a name to the anomaly function.");
            errorAlert.attr("data-error-source", "name");
            errorAlert.fadeIn(100);
            return
        }

        if (!isAlphaNumeric(formData.functionName)) {
            errorMessage.html("Please only use alphanumeric (0-9, A-Z, a-z) and '_' characters in 'Name' field. No space, no '-' is accepted.");
            errorAlert.attr("data-error-source", "name");
            errorAlert.fadeIn(100);
            return
        }

        //Check if dataset is selected
        if (formData.dataset == "") {
            errorMessage.html("Please select a dataset.");
            errorAlert.attr("data-error-source", "dataset-option-manage-anomaly-fn");
            errorAlert.fadeIn(100);
            return
        }

        //Check if metric is selected
        if (!formData.metric) {
            errorMessage.html("Please select a metric.");
            errorAlert.attr("data-error-source", "metric-option");
            errorAlert.fadeIn(100);
            return
        }

        //Check if min consecutive window size has value
        if (!formData.minConsecutiveSize|| formData.minConsecutiveSize < 0 ||  !isInt(formData.minConsecutiveSize) ) {
            errorMessage.html("Please fill in how many consecutive hours/days/weeks/months should fail the threshold to trigger an alert. The value should be an integer.");
            errorAlert.attr("data-error-source", "min-consecutive-size");
            errorAlert.fadeIn(100);
            return
        }

        //Check if windowSize has value
        if (!formData.windowSize || formData.windowSize < 0) {
            errorMessage.html("Please fill in what timewindow should be tested each time when monitoring the data.");
            errorAlert.attr("data-error-source", "monitoring-window-size");
            errorAlert.fadeIn(100);
            return
        }

        if (!isPositiveInteger(formData.repeatEverySize)) {
            errorMessage.html('Please fill in: "Monitor data every" X hours/days/weeks etc., where X should be positive integer.');
            errorAlert.attr("data-error-source", "monitoring-repeat-size");
            errorAlert.fadeIn(100);
            return
        }

        //Check if repeatEvery has value
        if (!formData.repeatEverySize) {
            errorMessage.html("Please fill in how frequently should ThirdEye monitor the data.");
            errorAlert.attr("data-error-source", "monitoring-repeat-size");
            errorAlert.fadeIn(100);
            return
        }

        //Fields related to FUNCTION_TYPE PROPERTIES
        switch(formData.functionType){
            case "WEEK_OVER_WEEK_RULE":
                //Check if condition is selected
                if ( formData.condition == null) {
                    errorMessage.html("Please select a condition ie. DROP, INCREASE.");
                    errorAlert.attr("data-error-source", "anomaly-condition");
                    errorAlert.fadeIn(100);
                    return
                }

                //Check if threshold < 0 or the value of the input is not a number
                if (!formData.changeThreshold || formData.changeThreshold < 0.01) {
                    errorMessage.html("Please provide a threshold percentage using positive numbers greater than 1. Example: write 5 for a 5% threshold. <br> The ratio will be calculated as: (current value - baseline value) / baseline value");
                    errorAlert.attr("data-error-source", "anomaly-threshold");
                    errorAlert.fadeIn(100);
                    return
                }

                break;
            case "MIN_MAX_THRESHOLD":
                if( parseFloat(formData.min)  >= parseFloat(formData.max)){
                    errorMessage.html("Minimum threshold should be less than maximum threshold.");
                    errorAlert.attr("data-error-source", "anomaly-threshold");
                    errorAlert.fadeIn(100);
                    return
                }

                if ( !formData.min && !formData.max) {
                    errorMessage.html("Please provide a threshold value.");
                    errorAlert.attr("data-error-source", "anomaly-threshold");
                    errorAlert.fadeIn(100);
                    return
                }
                break;
            case "SIGN_TEST":
            case "KALMAN_FILTER":
            case "SCAN_STATISTICS":
            default:


                for (key in formData["properties"]){

                    //var $inputField = $(fields[i]);
                    var expectedDataType = formData["properties_meta"][key];

                    var value = formData["properties"][key];

                    switch (expectedDataType.toLowerCase()) {
                        case "double":
                            if(value != ""){
                                if(!isFloat(value)){
                                    errorMessage.html("In '" + key + "' field <b>" + expectedDataType + "</b> data type is expected.");
                                    errorAlert.attr("data-error-source", "data-type");
                                    errorAlert.fadeIn(100);
                                    return
                                };
                            };
                            break;
                        case "int":
                            if(value != "") {
                                if (!isInt(value)) {
                                    errorMessage.html("In <b>" + key + "</b> field <b>integer</b> data type is expected.");
                                    errorAlert.attr("data-error-source", "data-type");
                                    errorAlert.fadeIn(100);
                                    return
                                };
                            };
                            break;
                        case "boolean":
                            if(value != "") {
                                if (!isBoolean(value)) {
                                    errorMessage.html("In <b>" + key + "</b> field <b>" + expectedDataType + "</b> data type is expected.");
                                    errorAlert.attr("data-error-source", "data-type");
                                    errorAlert.fadeIn(100);
                                    return
                                };
                            };
                            break;
                        case "pattern":

                            break;
                        case "string":
                            break;
                        default:
                            break
                    }

                }
        }

        //Check if repeatEverySize is positive integer
        function isPositiveInteger(str) {
            return /^\+?[1-9][\d]*$/.test(str);
        }

        //Check if rule name contains only alphanumeric and "_" characters
        function isAlphaNumeric(str) {

            str = str.replace(/_/g, '');
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


        function isFloat(str){
          return !!parseFloat(str)
        }

        function isBoolean(str){
            isBool = typeof(str) === "boolean" || ( str + "" == "true" || str + "" == "false" );
            return typeof(str) === "boolean" || ( str + "" == "true" || str + "" == "false" );
        }

        function isInt(str){
            var strNum = Number(str);
            var isInteger = (typeof strNum === "number" && isFinite(strNum) &&  Math.floor(strNum) === strNum);
            return typeof strNum === "number" && isFinite(strNum) &&  Math.floor(strNum) === strNum;
        }
        return valid
    }

    //SUBMIT CREATED ANOMALY FUNCTION
    function createAnomalyFunction(callback) {

        var form = $("#create-anomaly-functions-tab");
        var formData = collectAnomalyFnData(form);
        var valid = validateAnomalyFnFormData(formData, form);

        if (valid) {

            //Disable submit btn
            disableButton($("#create-anomaly-function"));
            disableButton($("#create-run-anomaly-function"));

            //Submit data
            var urlParams = urlOfAnomalyFn(formData);
            submitData("/dashboard/anomaly-function?" + urlParams).done(function(id){
               //Enable submit btn
               enableButton($("#create-anomaly-function"))
               enableButton($("#create-run-anomaly-function"))

                var successMessage = $("#manage-anomaly-function-success");
                $("p", successMessage).html("success");
                successMessage.fadeIn(100);

                    if(callback){
                        callback(id);
                    }
            })
        }
    }

    function runAdhocAnomalyFunction(id){
        // Change the view to anomalies
        hash.view = "anomalies";

        var form = $("#create-anomaly-functions-tab");
        var formData = collectAnomalyFnData(form);
        hash.metrics = formData.metric;
        hash.dataset = formData.dataset;

        var maxMillis = window.datasetConfig.maxMillis ? window.datasetConfig.maxMillis : moment().valueOf();
        hash.currentEnd = maxMillis;
        hash.currentStart = moment(parseFloat(maxMillis)).add(-1, 'days').valueOf();
        hash.anomalyFunctionId = id;
        if(formData.functionType == "WEEK_OVER_WEEK_RULE") {
            switch(formData.baseline){
                case "w/w":
                    hash.fnCompareWeeks = 1;
                break;
                case "w/2w":
                    hash.fnCompareWeeks = 2;
                break;
                case "w/3w":
                    hash.fnCompareWeeks = 3;
                break;
            }
        }

        var currentStartISO = moment(parseInt(hash.currentStart)).toISOString();
        var currentEndISO = moment(parseInt(hash.currentEnd)).toISOString();
        var urlParams = "dataset=" + hash.dataset + "&startTimeIso=" + currentStartISO + "&endTimeIso=" + currentEndISO + "&metric=" + hash.metrics + "&id=" + hash.anomalyFunctionId;
        urlParams +=  hash.filter ? "&filters=" + hash.filters : "";

        var url = "/dashboard/anomaly-function/adhoc?" + urlParams;

        submitData(url).done(function(){
            //update hash will trigger window.onhashchange event:
            //update the form area and trigger the ajax call
            window.location.hash = encodeHashParameters(hash);
        })
    }

    //DELETE ANOMALY FUNCTION
    function deleteAnomalyFunction(target) {

        disableButton($("#confirm-delete-anomaly-function"));

        var functionId = $(target).attr("data-function-id");
        var url = "/dashboard/anomaly-function?id=" + functionId;


        deleteData(url, "").done(function () {
            //Remove row from the dataTable
            var table = $("#existing-anomaly-functions-table").DataTable();
            table
                .row($(".existing-function-row[data-function-id='" + functionId + "']"))
                .remove()
                .draw();

            var successMessage = $("#delete-anomaly-function-success");
            $("p", successMessage).html("success");
            successMessage.fadeIn(100);
        }).fail(function(){
            var errorMessage = $("#delete-anomaly-function-error");
            $("p", errorMessage).html("There was an error completing you request. Please try again.");
            errorMessage.fadeIn(100);
        })
    }

    function toggleActiveState(target){
        var rowId = $(target).attr("data-row-id");
        var existingAnomalyFunctionsDataStr = window.sessionStorage.getItem('existingAnomalyFunctions');
        var existingAnomalyFunctionsData = JSON.parse(existingAnomalyFunctionsDataStr);
        var anomalyFunctionObj = existingAnomalyFunctionsData[rowId];
        anomalyFunctionObj.isActive = !anomalyFunctionObj.isActive;
        //encode filters
        anomalyFunctionObj.filters = encodeURIComponent(JSON.stringify(anomalyFunctionObj.filters));

        var url = "/dashboard/anomaly-function/" + anomalyFunctionObj["id"] + "/?dataset=" + hash.dataset;
        for (key in anomalyFunctionObj){

            var value = (anomalyFunctionObj[key] + "" == "null") ? "" : anomalyFunctionObj[key];
            url += "&" + key + "=" + value;
        }

        submitData(url).done(function (id) {

        //decode filters
        anomalyFunctionObj.filters = JSON.parse(decodeURIComponent(anomalyFunctionObj.filters));
        existingAnomalyFunctionsData[rowId] = anomalyFunctionObj;

        window.sessionStorage.setItem('existingAnomalyFunctions', JSON.stringify(existingAnomalyFunctionsData));

            //Enable submit btn
            enableButton($("#confirm-toggle-active-state"));
            var successMessage = $("#toggle-active-state-success");
            $("p", successMessage).html("success");
            successMessage.fadeIn(100);
            $("#close-toggle-alert-modal").click();
            $("#self-service-chart-area-error").hide();

        }).fail(function(){
            var errorMessage = $("#toggle-active-state-error");
            $("p", errorMessage).html("There was an error completing you request. Please try again.");
            errorMessage.fadeIn(100);
        })
    }

    //Populate modal with data of the selected anomaly function
    function populateUpdateForm(target) {

        var rowId = $(target).attr("data-row-id");

        /** Handelbars template for ANOMALY FUNCTION FORM **/
        var existingAnomalyFunctionsDataStr = window.sessionStorage.getItem('existingAnomalyFunctions');
        var existingAnomalyFunctionsData = JSON.parse(existingAnomalyFunctionsDataStr);
        var anomalyFunctionObj = existingAnomalyFunctionsData[rowId];
        var fnProperties = parseProperties(anomalyFunctionObj.properties);
        var schedule = parseCron(anomalyFunctionObj.cron);
        var filters = anomalyFunctionObj.filters ? anomalyFunctionObj.filters.split(";"): "";

        //parse filters
        var filterParams = {};
        for(var i= 0,len=filters.length; i<len; i++){
            var keyValue = filters[i];
            var keyValueAry = keyValue.split("=")
            var dimension = keyValueAry[0];
            var value = keyValueAry[1];

            if(filterParams.hasOwnProperty(dimension)){
                filterParams[dimension].push(value)
            }else{
                filterParams[dimension] = [];
                filterParams[dimension].push(value)
            }
        }

        var anomalyFunctionTypeMetaDataStr = window.sessionStorage.getItem('anomalyFunctionTypeMetaData');
        var anomalyFunctionTypeMetaData = JSON.parse(anomalyFunctionTypeMetaDataStr);
        var templateData = {data: anomalyFunctionObj, fnProperties: fnProperties, schedule :schedule, propertyDefs: anomalyFunctionTypeMetaData.propertyDefs, fnTypeMetaData : anomalyFunctionTypeMetaData.fnTypeMetaData };
        var result_anomaly_function_form_template = HandleBarsTemplates.template_anomaly_function_form(templateData);
        $("#update-anomaly-functions-form-placeholder").html(result_anomaly_function_form_template);


        //Store the rowId on the update form btn
        $("#update-anomaly-function").attr("data-row-id", rowId )


        //Set function type
        $(".function-type-option[value='" + existingAnomalyFunctionsData[rowId].type + "']").click()

        //Set min max condition and populate inputfield
        if(fnProperties && fnProperties.hasOwnProperty("min") && fnProperties.hasOwnProperty("max")){
            $(".anomaly-condition-min-max-option[value='MINMAX']").click()
        }else if(fnProperties && fnProperties.hasOwnProperty("min")){
            $(".anomaly-condition-min-max-option[value='MIN']").click()
        }else{
            $(".anomaly-condition-min-max-option[value='MAX']").click()
        }


        /** Populate dimension list **/
        var dimnsionList = window.datasetConfig.datasetDimensions;
        var dimensionListHtml = "";
        var filterDimensionListHtml = "";
        for (var index = 0, numDimensions = dimnsionList.length; index < numDimensions; index++) {
            dimensionListHtml += "<li class='dimension-option' rel='dimensions' value='" + dimnsionList[index] + "'><a href='#' class='uk-dropdown-close'>" + dimnsionList[index] + "</a></li>";
            filterDimensionListHtml += "<li class='filter-dimension-option' value='" + dimnsionList[index] + "'><a href='#' class='radio-options'>" + dimnsionList[index] + "</a></li>";
        }

        enableButton($("#update-anomaly-function"));

        $("#update-function-modal .dimension-list").html(dimensionListHtml);

        //append filter dimension list
        $("#update-function-modal .filter-dimension-list").html(filterDimensionListHtml);

        /** Handelbars template for dimensionvalues in filter dropdown **/
        var datasetFilters = window.datasetConfig.datasetFilters;
        var result_filter_dimension_value_template = HandleBarsTemplates.template_filter_dimension_value(datasetFilters)
        $("#update-function-modal .dimension-filter" ).each(function () {
            $(this).after(result_filter_dimension_value_template)
        });

        updateFilterSelection(filterParams, { scope: $("#update-function-modal")} )

        //value set in display block mode
       var firstDimension = $("#update-function-modal .filter-dimension-option:first-of-type")
        firstDimension.click();

    }

    function updateAnomalyFunction() {

        var form = $("#update-function-modal");
        var formData = collectAnomalyFnData(form);
        var valid = validateAnomalyFnFormData(formData, form);

        if (valid) {

            disableButton($("#update-anomaly-function"));

            var urlParams = urlOfAnomalyFn(formData)

            updateData("/dashboard/anomaly-function/" + formData.functionId + "?" + urlParams).done(function () {
                //existingAnomalyFunctionsData[rowId] = {formData}
                getExistingAnomalyFunctions(formData.dataset);

                //Enable submit btn
                enableButton($("#update-anomaly-function"));
                var rowId = $("#update-anomaly-function").attr("data-row-id");
                var successMessage = $("#manage-anomaly-function-success", form);
                $("p", successMessage).html("success");
                successMessage.fadeIn(100);
                $("#update-function-modal .uk-modal-close.uk-close").click();
            })
        }
    }

    function urlOfAnomalyFn(formData){

        //Todo: remove the condition once the create and update form both has the inputfields for cron
        if(formData.repeatEveryUnit){
            var cron = encodeCron(formData.repeatEveryUnit, formData.repeatEverySize, formData.scheduleMinute, formData.scheduleHour )
        }
        /* Submit form */
        var url = "dataset=" + formData.dataset + "&metric=" + formData.metric + "&type=" + formData.functionType + "&metricFunction=" + formData.metricFunction + "&functionName=" + formData.functionName
            + "&windowSize=" + formData.windowSize + "&windowUnit=" + formData.windowUnit + "&windowDelay=" + formData.windowDelay

            + "&isActive=" + formData.isActive + "&properties="

        url += (formData.functionType == "WEEK_OVER_WEEK_RULE") ? "baseline=" + formData.baseline + ";changeThreshold=" + formData.condition + formData.changeThreshold + ";minConsecutiveSize=" + formData.minConsecutiveSize + ";minConsecutiveUnit=" + formData.minConsecutiveUnit : "";
        url += (formData.functionType == "MIN_MAX_THRESHOLD" && formData.min ) ? "min=" + formData.min + ";" : "";
        url += (formData.functionType == "MIN_MAX_THRESHOLD" && formData.max ) ? "max=" + formData.max + ";" : "";
        url += (formData.functionType != "MIN_MAX_THRESHOLD" && formData.functionType != "WEEK_OVER_WEEK_RULE" ) ? stringifyProperties(formData.properties) : "";
        url += (formData.repeatEveryUnit) ? "&cron=" + cron : "";
        url += (formData.repeatEveryUnit) ? "&repeatEvery=" + formData.repeatEveryUnit : "";
        url += (formData.repeatEveryUnit == "DAYS") ?  "&scheduleMinute=" + formData.scheduleMinute + "&scheduleHour=" + formData.scheduleHour : "";
        url += (formData.exploreDimension) ? "&exploreDimension=" + formData.exploreDimension : "";
        url += (formData.filters && formData.filters != encodeURIComponent(JSON.stringify({}))) ? "&filters=" + formData.filters : "";
        url += (formData.functionId ) ? "&id=" + formData.functionId : "" ;

        return url
    }

}
