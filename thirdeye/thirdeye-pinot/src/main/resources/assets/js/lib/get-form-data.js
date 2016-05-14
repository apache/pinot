function getDataSetList(){

    var url = "/dashboard/data/datasets";
    getData(url).done( function(data){

        /* Handelbars template for datasets dropdown */
        var result_datasets_template = HandleBarsTemplates.template_datasets(data);
        $(".landing-dataset").each(function(){ $(this).html(result_datasets_template)});

        $(".selected-dataset").text("Select dataset");

        //populate dataset from hash and get related dashboard list
        if (hash.hasOwnProperty('dataset')){
            $($(".dataset-option[value='"+ hash.dataset +"']")[0]).click();
        }
    });
};

function getDashboardList(){

    //Till the endpoint is ready no ajax call is triggered and works with  hardcoded data in local data variable
    var url = "/dashboard/data/dashboards?dataset=" + hash.dataset;

    getData(url).done( function(data){

    /* Create dashboard dropdown */
    var dashboardListHtml = "";
    for(var i= 0, len = data.length; i<len; i++){
        dashboardListHtml += "<li class='dashboard-option' rel='dashboard' value='"+ data[i] +"'><a href='#'>"+ data[i] +"</a></li>";
    }
    $("#dashboard-list").html(dashboardListHtml);
    $("#selected-dashboard").text("Select dashboard");

    //Preselect dashboard if present in hash
    if (hash.hasOwnProperty('dashboard')){
        $(".dashboard-option[value='"+ hash.dashboard +"']").click();
    }else{
        var defaultDashboard = 'Default_All_Metrics_Dashboard'
        $("#selected-dashboard").attr("value", defaultDashboard )
        $("#selected-dashboard").text(defaultDashboard)
    //    $(".dashboard-option[value='" + defaultDashboard +"']").click();
    }

    });
};

function getMetricNFilterList() {

    //Create metric dropdown
    var url = "/dashboard/data/metrics?dataset=" + hash.dataset;

    getData(url).done(function (data) {


        //empty previous metriclist
        $(".metric-list").empty()


        var errorMessage = $("#"+ hash.view +"-time-input-form-error p");
        var errorAlert = $("#"+ hash.view +"-time-input-form-error");
        if(!data){
            errorMessage.html("No metrics available in the server. Error: data = " + data);
            errorAlert.fadeIn(100);
            return
        }else{
            errorAlert.hide()
        }

        /* Create metrics dropdown */
        var metricListHtml = "";
        for (var i = 0, len = data.length; i < len; i++) {
            metricListHtml += "<li class='metric-option' rel='metrics' value='" + data[i] + "'><a href='#' class='uk-dropdown-close'>" + data[i] + "</a></li>";
        }
        $(".metric-list").html(metricListHtml);

        //Preselect metrics if present in hash
        $(".view-metric-selector .added-item").remove();
//        if (hash.hasOwnProperty('metrics') && hash.hasOwnProperty('view') ){
//
//            var currentTab = $(".dashboard-settings-tab-content[rel='"+ hash.view +"']");
//
//            //this value is a string so need to transform it into array
//            var metricAry = hash.metrics.split(',');
//
//            for (var i = 0, len = metricAry.length; i < len; i++) {
//                $(".metric-option[value='" + metricAry[i] + "']", currentTab).click();
//            }
//        }
    });

    //Create dimension dropdown and filters
    var url = "/dashboard/data/filters?dataset=" + hash.dataset;
    getData(url).done(function (data) {

        //empty previous dimension list
        $(".dimension-list").empty()
        $(".filter-dimension-list").empty()


        var errorMessage = $("#"+ hash.view +"-time-input-form-error p");
        var errorAlert = $("#"+ hash.view +"-time-input-form-error");
        if(!data){
            errorMessage.html("No dimension or dimension values available. Error: dimension data = " + data);
            errorAlert.fadeIn(100);
            return
        }else{
            errorAlert.hide()
        }


        /* Create dimensions and filter dimensions dropdown */
        var dimensionListHtml = "";
        var filterDimensionListHtml = "";

        //Global - public
        datasetDimensions = []

        for (var k in  data) {
            dimensionListHtml += "<li class='dimension-option' rel='dimensions' value='" + k + "'><a href='#' class='uk-dropdown-close'>" + k + "</a></li>";
            filterDimensionListHtml += "<li class='filter-dimension-option' value='" + k + "'><a href='#' class='radio-options'>" + k + "</a></li>";
            datasetDimensions.push(k)
        }

        console.log('datasetDimensions: ')
        console.log(datasetDimensions)
        $(".dimension-list").html(dimensionListHtml);

        //Preselect dimensions if present in hash
        $(".view-dimension-selector .added-item").remove();

        if (hash.hasOwnProperty('dimensions') && hash.hasOwnProperty('view') ){
            var currentTab = $(".dashboard-settings-tab-content[rel='"+ hash.view +"']");

            //this value is a string so need to transform it into array
            var dimensionAry = hash.dimensions.split(',');

            for (var i = 0, len = dimensionAry.length; i < len; i++) {
                $(".dimension-option[value='" + dimensionAry[i] + "']", currentTab).click();
            }
        }

        //append filter dimension list
        $(".filter-dimension-list").html(filterDimensionListHtml);

        $(".filter-panel .value-filter").remove();


        /* Handelbars template for treemap table */
        var result_filter_dimension_value_template = HandleBarsTemplates.template_filter_dimension_value(data)
        $(".dimension-filter").each(function(){
            $(this).after(result_filter_dimension_value_template)
        });

        $(".filter-dimension-option:first-of-type").each(function(){
            $(this).click();
            $(".radio-options",this).click();
        });

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

    });
};


function getMaxDateTime() {

    //Till the endpoint is ready no ajax call is triggered and works with  hardcoded data in local data variable
    var url = "/dashboard/data/info?dataset=" + hash.dataset;

    getData(url).done(function (data) {

        var errorMessage = $("#"+ hash.view +"-time-input-form-error p");
        var errorAlert = $("#"+ hash.view +"-time-input-form-error");
        if(!data){
            errorMessage.html("No dataset info available. Error: data/info?dataset data = " + data);
            errorAlert.attr("data-error-source", "datasetinfo");
            errorAlert.fadeIn(100);
            return
        }else{
            $("#"+ hash.view +"-time-input-form-error[data-error-source= 'datasetinfo']").hide()
        }

        //Max date time
        var maxMillis = data["maxTime"]
        var currentStartDateTime = moment(parseInt(maxMillis)).add(-1, 'days');
        var currentStartDateString = currentStartDateTime.format("YYYY-MM-DD");
        var currentStartTimeString = currentStartDateTime.format("HH" + ":00");

        //Max date time
        var currentEndDateTime = moment(parseInt(maxMillis));
        var currentEndDateString = currentEndDateTime.format("YYYY-MM-DD");
        var currentEndTimeString = currentEndDateTime.format("HH" + ":00");

        //Populate WoW date
        var baselineStartDateTime = currentStartDateTime.add(-7,'days');
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

        //Set the max date on the datepicker dropdowns
        var maxDate = moment(parseInt(maxMillis)).format("YYYY-MM-DD");
        UIkit.datepicker(UIkit.$('.current-start-date-input'), { maxDate: maxDate, format:'YYYY-MM-DD' });
        UIkit.datepicker(UIkit.$('.current-end-date-input'),  { maxDate: maxDate, format:'YYYY-MM-DD' });
        UIkit.datepicker(UIkit.$('.baseline-start-date-input'),  { maxDate: maxDate, format:'YYYY-MM-DD' });
        UIkit.datepicker(UIkit.$('.baseline-end-date-input'),  { maxDate: maxDate, format:'YYYY-MM-DD' });

        //Add max and min time as a label time selection dropdown var minMillis = data["minTime"];
        var maxDateTime = maxMillis ? moment(parseInt(maxMillis)).format("YYYY-MM-DD h a") : "n.a.";
        $(".max-time").text(maxDateTime);

        //todo add min time to info endpoint
        var minMillis = data["minTime"];
        var minDateTime = minMillis ? moment(parseInt(minMillis)).format("YYYY-MM-DD h a") : "n.a.";
        $(".min-time").text(minDateTime);

    })
}
