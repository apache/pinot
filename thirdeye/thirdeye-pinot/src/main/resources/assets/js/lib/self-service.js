function getExistingAnomalyFunctions(dataset) {
    //removing the query until it returns other than 500 error
    if (dataset) {
        var url = "/dashboard/anomaly-function/view?dataset=" + dataset;
        var tab = "self-service";
        var result_existing_anomaly_functions_template;

        getData(url, tab).done(function (data) {
            if (data) {
                window.sessionStorage.setItem('existingAnomalyFunctions', JSON.stringify(data));
            }
            compileAnomalyFnTable(data)
        });
    } else {
        compileAnomalyFnTable()
    }

    function compileAnomalyFnTable(data) {
        if (!data) {
            data = {}
        }

        /** Handelbars template for EXISTING ANOMALY FUNCTIONS TABLE **/
        result_existing_anomaly_functions_template = HandleBarsTemplates.template_existing_anomaly_functions(data);
        $("#existing-anomaly-functions-table-placeholder").html(result_existing_anomaly_functions_template);

        /** Instanciate Datatables on self service view **/
        $("#existing-anomaly-functions-table").DataTable({
            "bAutoWidth": false,
            "columnDefs": [
                { "targets": 0 , "width": "100px" },
                { "targets": 1 , "width": "100px" },
                { "targets": 2 , "width": "100px" },
                { "targets": 3 , "width": "100px" },

                { "targets": 4 , "width": "50px", "orderable": false},
                { "targets": 5 , "width": "50px", "orderable": false},
                { "targets": 6 , "width": "50px", "orderable": false},
            ]
        });
    }
}


function renderSelfService() {

    // The elements of the self-service tab are created on the anomaly-function-form template

    /**--- Eventlisteners on anomaly function form: create and update functionality ---**/

        // COMMON PARAMETERS IN EVERY ANOMALY FUNCTION TYPE
        // Dataset selection
    $("#self-service-forms-section").on("click", ".dataset-option-manage-anomaly-fn", function () {
        selectDatasetNGetFormData(this);
    });

    // Name
    $("#self-service-forms-section").on("keyup", "#name", function () {
        var form = $(this).closest("form");
        hideErrorAndSuccess("name",form);
        enableButton($("#create-anomaly-function"));
    });

    //Function type
    $("#self-service-forms-section").on("click", ".function-type-option", function () {
        var form = $(this).closest("form");
        toggleFunctionTypeFields(this,form);
        hideErrorAndSuccess("");
    })

    // Metric
    $("#self-service-forms-section").on("click", ".metric-option", function () {
        var form = $(this).closest("form");
        hideErrorAndSuccess("metric-option",form);
    });


    // Monitoring window size
    $("#self-service-forms-section").on("keyup, click", "#monitoring-window-size", function () {
        var form = $(this).closest("form");
        hideErrorAndSuccess("monitoring-window-size",form);
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
    //USER_RULE FUNCTION TYPE PARAMS
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


    //COMMON LISTENERS

    //Clear form
    $("#self-service-forms-section").on("click", "#clear-create-form", function () {
        clearCreateForm();
    });

    //Create anomaly function
    $("#self-service-forms-section").on("click", "#create-anomaly-function", function () {
        createAnomalyFunction()
    });

    /** Manage anomaly tab related listeners **/

        //Edit button
    $("#self-service-forms-section").on("click", ".init-update-function-btn", function () {
        populateUpdateForm(this)
    });

    //Delete button - opens modal for confirmation
    $("#self-service-forms-section").on("click", ".init-delete-anomaly-function", function () {
        var functionId = $(this).attr("data-function-id");
        var functionName = $(this).attr("data-function-name");
        $("#confirm-delete-anomaly-function").attr("data-function-id", functionId);
        $("#function-to-delete").text(functionName);
        $("#delete-anomaly-function-success").hide();
        $("#delete-anomaly-function-error").hide();
        enableButton($("#confirm-delete-anomaly-function"));
    });

    //Confirm delete button
    $("#self-service-forms-section").on("click", "#confirm-delete-anomaly-function", function () {
        deleteAnomalyFunction(this)
    });

    //Update button
    $("#self-service-forms-section").on("click", "#update-anomaly-function", function () {
        updateAnomalyFunction()
    });

    /** Event handlers **/

    function toggleFunctionTypeFields(target) {
        var functionType = $(target).attr("id");
        $(".function-type-fields").addClass("uk-hidden");
        $("." + functionType + "-fields").removeClass("uk-hidden");
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
    function collectAnomalyFnFormValues(form) {
        var formData = {};

        //Close uikit dropdowns
        $("[data-uk-dropdown]").removeClass("uk-open");
        $("[data-uk-dropdown]").attr("aria-expanded", false);
        $(".uk-dropdown").addClass("hidden");

        /** Collect the form values **/

            //Currently only supporting 'user rule' type alert configuration on the front end-
            // KALMAN and SCAN Statistics are set up by the backend
        formData.dataset = $(".selected-dataset", form).attr("value");
        formData.functionName = $("#name", form).val();
        formData.metric = $("#selected-metric-manage-anomaly-fn", form).attr("value");
        formData.functionType = $("#selected-function-type", form).attr("value");
        formData.metricFunction = "SUM";
        formData.windowDelay = "1";  //Todo:consider max time ?
        formData.windowSize = $("#monitoring-window-size", form).val();
        formData.windowUnit = $("#selected-monitoring-window-unit", form).attr("value");

        //Todo add monitoring schedule time to update form when it's available

        formData.repeatEverySize = $("#monitoring-repeat-size", form).val();
        formData.repeatEveryUnit = $("#selected-monitoring-repeat-unit", form).attr("value");
        var monitoringScheduleTime = ( $("#monitoring-schedule-time", form).val() == "" ) ? "" : $("#monitoring-schedule-time", form).val() //Todo: in case of daily data granularity set the default schedule to time when datapoint is created
        if(monitoringScheduleTime){
            formData.scheduleMinute = monitoringScheduleTime.substring(3, monitoringScheduleTime.length);
            formData.scheduleHour = monitoringScheduleTime.substring(0, monitoringScheduleTime.length - 3);
        }

        if ($("#function-id", form).length > 0) {
            formData.functionId = $("#function-id", form).text();
        }

        if ($("#active-alert", form).is(':checked')) {
            formData.isActive = true;
        } else {
            formData.isActive = false;
        }

        //USER_RULE & MIN_MAX_THRESHOLD
        var filters = readFiltersAppliedInCurrentView("self-service", {form: form});

        //Transform filters Todo: clarify if filters object should be consistent on FE and BE
        formData.filters = encodeURIComponent(JSON.stringify(filters));
        formData.exploreDimension = $("#self-service-view-single-dimension-selector #selected-dimension", form).attr("value");

        //USER_RULE Params
        if (formData.functionType == "USER_RULE") {

            formData.condition = ( $("#selected-anomaly-condition", form).attr("value") == "DROPS" ) ? "-" : ( $("#selected-anomaly-condition", form).attr("value") == "INCREASES" ) ? "" : null;
            formData.baseline = $("#selected-anomaly-compare-mode", form).attr("value");
            formData.changeThreshold = parseFloat($("#anomaly-threshold", form).val() / 100);

            //MIN_MAX_THRESHOLD PARAMS
        } else if (formData.functionType == "MIN_MAX_THRESHOLD") {

            formData.min = $("#anomaly-threshold-min", form).val();
            formData.max = $("#anomaly-threshold-max", form).val();
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

        if( formData.functionType == "USER_RULE"){

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
        }

        if(formData.functionType == "MIN_MAX_THRESHOLD"){

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

        }

        //Check if windowSize has value
        if (!formData.windowSize || formData.windowSize < 0) {
            errorMessage.html("Please fill in how many consecutive hours/days/weeks/months should fail the threshold to trigger an alert.");
            errorAlert.attr("data-error-source", "monitoring-window-size");
            errorAlert.fadeIn(100);
            return
        }


        //Check if repeatEverySize is positive integer
        function isPositiveInteger(str) {
            return /^\+?[1-9][\d]*$/.test(str);
        }

        //Todo:Remove this condition if the repeatEverySize and repeatEveryUnit are available on all forms all
        if (formData.hasOwnProperty("repeatEverySize")) {

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
        }
        return valid
    }


    //SUBMIT CREATED ANOMALY FUNCTION
    function createAnomalyFunction() {
        var form = $("#create-anomaly-functions-tab");
        var formData = collectAnomalyFnFormValues(form);
        var valid = validateAnomalyFnFormData(formData, form);

        if (valid) {

            //Disable submit btn
            disableButton($("#create-anomaly-function"));


            var urlParams = urlOfAnomalyFn(formData);
            submitData("/dashboard/anomaly-function/create?" + urlParams).done(function(){
               //Enable submit btn
               enableButton($("#create-anomaly-function"))

                var successMessage = $("#manage-anomaly-function-success");
                $("p", successMessage).html("success");
                successMessage.fadeIn(100);
            })
        }
    }

    //DELETE ANOMALY FUNCTION
    function deleteAnomalyFunction(target) {

        disableButton($("#confirm-delete-anomaly-function"));

        var functionId = $(target).attr("data-function-id");
        var url = "/dashboard/anomaly-function/delete?id=" + functionId;


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
            successMessage.fadeIn(100);
        })
    }


    //Populate modal with data of the selected anomaly function
    function populateUpdateForm(target) {

        var rowId = $(target).attr("data-row-id");

        /** Handelbars template for ANOMALY FUNCTION FORM **/
        var existingAnomalyFunctionsDataStr = window.sessionStorage.getItem('existingAnomalyFunctions');
        var existingAnomalyFunctionsData = JSON.parse(existingAnomalyFunctionsDataStr);
        var anomalyFunctionObj = existingAnomalyFunctionsData[rowId];
        var fnProperties = parseFnProperties(anomalyFunctionObj.properties);
        var schedule = parseCron(anomalyFunctionObj.cron);

        var templateData = {data: anomalyFunctionObj, fnProperties: fnProperties, schedule :schedule}

        var result_anomaly_function_form_template = HandleBarsTemplates.template_anomaly_function_form(templateData );
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

        /** Handelbars template for dataset dropdown **/
        var datasetList = JSON.parse(window.sessionStorage.getItem('datasetList'));
        var queryFormDatasetData = {data: datasetList};
        var result_datasets_template = HandleBarsTemplates.template_datasets(queryFormDatasetData);
        $("#update-function-modal .landing-dataset").each(function () {
            $(this).html(result_datasets_template)
        });


        /** Handelbars template for query form multi select metric list **/
        var metricList = window.datasetConfig.datasetMetrics;
        var queryFormMetricListData = {data: metricList};
        var result_query_form_metric_list_template = HandleBarsTemplates.template_metric_list(queryFormMetricListData);
        $("#update-function-modal .metric-list").each(function () {
            $(this).html(result_query_form_metric_list_template)
        });


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
        $(".dimension-filter").each(function () {
            $(this).after(result_filter_dimension_value_template)
        });

        //set value set in display block mode
        $("#update-function-modal  .filter-dimension-option:first-of-type").each(function () {
            $(this).click();
            $(".radio-options", this).click();
        });


    }

    function updateAnomalyFunction() {

        var form = $("#update-function-modal");
        var formData = collectAnomalyFnFormValues(form);
        var valid = validateAnomalyFnFormData(formData, form);

        if (valid) {

            disableButton($("#update-anomaly-function"));

            var urlParams = urlOfAnomalyFn(formData)

            submitData("/dashboard/anomaly-function/update?" + urlParams).done(function () {

                //existingAnomalyFunctionsData[rowId] = {formData}
                getExistingAnomalyFunctions(formData.dataset);

                //Enable submit btn
                enableButton($("#update-anomaly-function"));
                $("#close-update-fn-modal").click();
                var rowId = $("#update-anomaly-function").attr("data-row-id");
                var successMessage = $("#manage-anomaly-function-success", form);
                $("p", successMessage).html("success");
                successMessage.fadeIn(100);
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
        url += (formData.functionType == "USER_RULE") ? "baseline=" + formData.baseline + ";changeThreshold=" + formData.condition + formData.changeThreshold : "";
        url += (formData.functionType == "MIN_MAX_THRESHOLD" && formData.min ) ? "min=" + formData.min + ";" : "";
        url += (formData.functionType == "MIN_MAX_THRESHOLD" && formData.max ) ? "max=" + formData.max : "";
        url += (formData.repeatEveryUnit) ? "&cron=" + cron : "";
        url += (formData.repeatEveryUnit) ? "&repeatEvery=" + formData.repeatEveryUnit : "";
        url += (formData.repeatEveryUnit == "DAYS") ?  "&scheduleMinute=" + formData.scheduleMinute + "&scheduleHour=" + formData.scheduleHour : "";
        url += (formData.exploreDimension) ? "&exploreDimension=" + formData.exploreDimension : "";
        url += (formData.filters && formData.filters != encodeURIComponent(JSON.stringify({}))) ? "&filters=" + formData.filters : "";
        url += (formData.functionId ) ? "&id=" + formData.functionId : "" ;

        return url
    }

    function parseFnProperties(properties){
        var fnProperties = {}
        var propertiesAry = properties.split(";");
        for (var i = 0, numProp = propertiesAry.length; i < numProp; i++) {
            var keyValue = propertiesAry[i];
            keyValue = keyValue.split("=")
            var key = keyValue[0];
            fnProperties[key] = keyValue[1];
        }
        return fnProperties
    }
}