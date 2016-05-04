function getDataSetList(){

    var url = "/dashboard/data/datasets";
    getData(url).done( function(data){
        /* Handelbars template for datasets dropdown */
        var result_datasets_template = HandleBarsTemplates.template_datasets(data);
        $(".landing-dataset").each(function(){ $(this).html(result_datasets_template)});

        $(".selected-dataset").text("Select dataset");

        //populate dataset from hash and get related dashboard list
        if (hash.hasOwnProperty('dataset')){
            // Todo: get that dataset
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


        /* Create dimensions and filter dimensions dropdown */
        var dimensionListHtml = "";
        var filterDimensionListHtml = "";

        for (var k in  data) {
            dimensionListHtml += "<li class='dimension-option' rel='dimensions' value='" + k + "'><a href='#' class='uk-dropdown-close'>" + k + "</a></li>";
            filterDimensionListHtml += "<li class='filter-dimension-option' value='" + k + "'><a href='#' class='radio-options'>" + k + "</a></li>";
        }

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
