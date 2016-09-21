function getDataSetList() {

    var url = "/dashboard/data/datasets";
    getData(url).done(function (data) {
        validateFormData(data, "No dataset list arrived from the server.", "datasets")

        //cache the data
        window.sessionStorage.setItem('datasetList', JSON.stringify(data));

        /* Handelbars template for dataset dropdowns */
        var queryFormDatasetData = {data: data}
        var result_datasets_template = HandleBarsTemplates.template_datasets(queryFormDatasetData);
        $(".landing-dataset").each(function () {
            $(this).html(result_datasets_template)
        });

        $(".selected-dataset").text("Select dataset");

        if (hash.hasOwnProperty('dataset')) {
            //Populate the selected item on the form element
            $(".selected-dataset").text(hash.dataset);
            $(".selected-dataset").attr("value", hash.dataset);

            //Trigger AJAX calls
            //get the latest available data timestamp of a dataset
            getAllFormData()
        }

    });
};

/** GET DATASET LIST RELATED METHODS **/

//takes the data the message
function validateFormData(data, message, errorSource) {

    var errorMessage = $(".time-input-form-error p");
    var errorAlert = $(".time-input-form-error");

    if (!data) {

        errorMessage.html(message + " Error: data = " + data);
        errorAlert.fadeIn(100);
        errorAlert.attr("data-error-source", errorSource);
        return
    } else {
        $(".time-input-form-error[data-error-source= '" + errorSource + "']").hide()
    }
}

function getAllFormData() {

    //Todo: remove these 2 global variables and work with $.when and deferreds
    window.responseDataPopulated = 0;
    window.numFormComponents = 4;
    getDatasetConfig();
    getDashboardList();
    getMetricList();
    getDimensionNValueList();
}

function getDashboardList() {

    //Till the endpoint is ready no ajax call is triggered and works with  hardcoded data in local data variable
    var url = "/dashboard/data/dashboards?dataset=" + hash.dataset;

    getData(url).done(function (data) {

        validateFormData(data, "No dashboard list arrived from the server.", "dashboard-list");

        /* Create dashboard dropdown */
        var dashboardListHtml = "";
        for (var i = 0, len = data.length; i < len; i++) {
            dashboardListHtml += "<li class='dashboard-option' rel='dashboard' value='" + data[i] + "'><a href='#'>" + data[i] + "</a></li>";
        }
        $("#dashboard-list").html(dashboardListHtml);
        $("#selected-dashboard").text("Select dashboard");

        window.responseDataPopulated++
        formComponentPopulated()
    });
};

function getMetricList() {

    //Create metric dropdown
    var url = "/dashboard/data/metrics?dataset=" + hash.dataset;

    getData(url).done(function (data) {

        validateFormData(data, "No metrics available in the server.", "metrics")

        window.datasetConfig.datasetMetrics = data;

        /* Handelbars template for query form multi select metric list */
        var queryFormMetricListData = {data: data};
        var result_query_form_metric_list_template = HandleBarsTemplates.template_metric_list(queryFormMetricListData);
        $(".metric-list").each(function () {
            $(this).html(result_query_form_metric_list_template)
        });

        window.responseDataPopulated++
        formComponentPopulated()
    });
}

function getDimensionNValueList() {

    //Create dimension dropdown and filters
    var url = "/dashboard/data/filters?dataset=" + hash.dataset;
    getData(url).done(function (data) {

        validateFormData(data, "No dimension or dimension values available.", "filters")


        /* Create dimensions and filter dimensions dropdown */
        var dimensionListHtml = "";
        var filterDimensionListHtml = "";
        window.datasetConfig.datasetDimensions = [];
        window.datasetConfig.datasetFilters = data;

        for (var k in  data) {
            dimensionListHtml += "<li class='dimension-option' rel='dimensions' value='" + k + "'><a href='#' class='uk-dropdown-close'>" + k + "</a></li>";
            filterDimensionListHtml += "<li class='filter-dimension-option' value='" + k + "'><a href='#' class='radio-options'>" + k + "</a></li>";
            window.datasetConfig.datasetDimensions.push(k)
        }

        $(".dimension-list").html(dimensionListHtml);

        //append filter dimension list
        $(".filter-dimension-list").html(filterDimensionListHtml);

        /* Handelbars template for dimensionvalues in filter dropdown */
        var result_filter_dimension_value_template = HandleBarsTemplates.template_filter_dimension_value(data)
        $(".dimension-filter").each(function () {
            $(this).after(result_filter_dimension_value_template)
        });


        $(".filter-dimension-option:first-of-type").each(function () {
            $(this).click();
            $(".radio-options", this).click();
        });


        window.responseDataPopulated++
        formComponentPopulated()
    });
};


function getDatasetConfig() {

    //Till the endpoint is ready no ajax call is triggered and works with  hardcoded data in local data variable
    var url = "/dashboard/data/info?dataset=" + hash.dataset;

    getData(url).done(function (data) {

        validateFormData(data, "No dataset config info available.", "info")

        /** MIN MAX DATE TIME **/

        //global
        window.datasetConfig.maxMillis = parseInt(data["maxTime"]);
        var maxMillis = window.datasetConfig.maxMillis;

        var currentStartDateTime = moment(maxMillis).add(-1, 'days');

        //If time granularity is DAYS have default 7 days in the time selector on pageload
        if (data["dataGranularity"] && data["dataGranularity"] == "DAYS") {
            currentStartDateTime = moment(maxMillis).add(-7, 'days')
        }

        var currentStartDateString = currentStartDateTime.format("YYYY-MM-DD");
        var currentStartTimeString = currentStartDateTime.format("HH" + ":00");

        //If time granularity is DAYS have default 12am on pageload
        if (data["dataGranularity"] && data["dataGranularity"] == "DAYS") {
            currentStartTimeString = "00:00";
        }

        //Max date time
        var currentEndDateTime = moment(maxMillis);
        var currentEndDateString = currentEndDateTime.format("YYYY-MM-DD");
        var currentEndTimeString = currentEndDateTime.format("HH:00");
        //If time granularity is DAYS have default 12am on pageload
        if (data["dataGranularity"] && data["dataGranularity"] == "DAYS") {
            currentEndTimeString = "00:00";
        }

        //Populate WoW date
        var baselineStartDateTime = currentStartDateTime.add(-7, 'days');
        var baselineStartDateString = baselineStartDateTime.format("YYYY-MM-DD");
        var baselineStartTimeString = currentStartTimeString;

        //Populate WoW time
        var baselineEndDateTime = currentEndDateTime.add(-7, 'days');
        var baselineEndDateString = baselineEndDateTime.format("YYYY-MM-DD");
        var baselineEndTimeString = currentEndTimeString;

        $(".current-start-date").text(currentStartDateString);
        $(".current-end-date").text(currentEndDateString);

        $(".current-start-time").text(currentStartTimeString);
        $(".current-end-time").text(currentEndTimeString);

        $(".baseline-start-date").text(baselineStartDateString);
        $(".baseline-end-date").text(baselineEndDateString);

        $(".baseline-start-time").text(baselineStartTimeString);
        $(".baseline-end-time").text(baselineEndTimeString);

        $(".current-start-date-input").val(currentStartDateString);
        $(".current-end-date-input").val(currentEndDateString);

        $(".current-start-time-input").val(currentStartTimeString);
        $(".current-end-time-input").val(currentEndTimeString);

        $(".baseline-start-date-input").val(baselineStartDateString);
        $(".baseline-end-date-input").val(baselineEndDateString);

        $(".baseline-start-time-input").val(baselineStartTimeString);
        $(".baseline-end-time-input").val(baselineEndTimeString);


        var maxDate = moment(maxMillis).format("YYYY-MM-DD");
        var tz = getTimeZone();

        //Add max and min time as a label time selection dropdown var minMillis = data["minTime"];
        var maxDateTime = maxMillis ? moment(maxMillis).tz(tz).format("YYYY-MM-DD h a z") : "n.a.";
        $(".max-time").text(maxDateTime);

        //todo add min time to info endpoint
        var minMillis = parseInt(data["minTime"]);
        var minDateTime = minMillis ? moment(minMillis).tz(tz).format("YYYY-MM-DD h a") : "n.a.";
        $(".min-time").text(minDateTime);


        //if no data available for today 12am hide today and yesterday option from time selection
        //if( moment(parseInt(maxMillis)) < moment(parseInt(Date.now())) ){
        var dateToday = moment().format("YYYY-MM-DD")


        if (moment(dateToday).isAfter(moment(maxDate))) {

            $(".current-date-range-option[value='today']").addClass("uk-hidden")
            $(".current-date-range-option[value='yesterday']").addClass("uk-hidden");

        }


        /**CONFIG: DATA GRANULARITY **/
        if (data.hasOwnProperty("dataGranularity")) {


            window.datasetConfig.dataGranularity = data["dataGranularity"];

            /** Render the form elements **/

            var dataGranularity = data["dataGranularity"];

            //Todo: you may remove the following if else if the set of values are known
            if (dataGranularity.toLowerCase().indexOf("minutes") > -1) {
                dataGranularity = "MINUTES";
            } else if (dataGranularity.toLowerCase().indexOf("hours") > -1) {
                dataGranularity = "HOURS";
            } else if (dataGranularity.toLowerCase().indexOf("days") > -1) {
                dataGranularity = "DAYS";
            }

            switch (dataGranularity) {

                case "MINUTES":
                    $(".baseline-aggregate[unit='10_MINUTES']").removeClass("uk-hidden");
                    $(".baseline-aggregate[unit='HOURS']").removeClass("uk-hidden");
                    $(".granularity-btn-group").addClass("vertical");

                    break;
                case "HOURS":
                    $(".baseline-aggregate[unit='10_MINUTES']").addClass("uk-hidden");
                    $(".baseline-aggregate[unit='HOURS']").removeClass("uk-hidden");
                    $(".granularity-btn-group").removeClass("vertical");

                    break;
                case "DAYS":
                    $(".baseline-aggregate[unit='10_MINUTES']").addClass("uk-hidden");
                    $(".baseline-aggregate[unit='HOURS']").addClass("uk-hidden");
                    //Select DAYS as default granularity
                    $(".baseline-aggregate[unit='HOURS']").removeClass("uk-active");
                    $(".baseline-aggregate[unit='DAYS']").addClass("uk-active");
                    $(".granularity-btn-group").removeClass("vertical");

                    break;
                default:
                    $(".baseline-aggregate[unit='10_MINUTES']").addClass("uk-hidden");
                    $(".baseline-aggregate[unit='HOURS']").removeClass("uk-hidden");
                    $(".baseline-aggregate[unit='DAYS']").removeClass("uk-hidden");
                    $(".granularity-btn-group").removeClass("vertical");
            }

            $("#self-service-chart-area .aggregate-granularity").text(dataGranularity)
        }


        /**CONFIG: INVERT COLOR METRICS **/
        if (data["invertColorMetrics"]) {
            window.datasetConfig.invertColorMetrics = data["invertColorMetrics"];
        }

        //Check if all form components are populated
        window.responseDataPopulated++
        formComponentPopulated()

    })
}
