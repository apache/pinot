$(document).ready( function() {


    /** Handelbars template for tabs **/
    //DASHBOARD
    var dasboard_tab_options = {
        tabName: "dashboard",
        showChartSection: true,
        showSelfServiceForms: false
    }
    var result_tab_template = HandleBarsTemplates.template_tab(dasboard_tab_options)
    $("#dashboard").append(result_tab_template);

    //COMPARE
    var compare_tab_options = {
        tabName: "compare",
        showChartSection: true,
        showSelfServiceForms: false
    }
    var result_tab_template = HandleBarsTemplates.template_tab(compare_tab_options)
    $("#compare").append(result_tab_template);

    //TIMESERIES
    var timseries_tab_options = {
        tabName: "timeseries",
        showChartSection: true,
        showSelfServiceForms: false
    }
    var result_tab_template = HandleBarsTemplates.template_tab(timseries_tab_options)
    $("#timeseries").append(result_tab_template);

    //ANOMALIES
    var anomalies_tab_options = {
        tabName: "anomalies",
        showChartSection: true,
        showSelfServiceForms: false
    }
    var result_tab_template = HandleBarsTemplates.template_tab(anomalies_tab_options)
    $("#anomalies").append(result_tab_template);


    //SELF SERVICE
    //ANOMALIES
    var self_service_tab_options = {
        tabName: "self-service",
        showChartSection: false,
        showSelfServiceForms: true
    }
    var result_tab_template = HandleBarsTemplates.template_tab(self_service_tab_options)
    $("#self-service").append(result_tab_template);



    /** Handelbars template for forms on tabs* */
    //DASHBOARD
    var dasboard_section_options = {
        tabName: "dashboard",
        needQueryForm: true,
        showDashboardSelection: true,
        showMultiMetricSelection: false,
        showSingleMetricSelection: false,
        showDimensionSelection: false,
        showFilterSelection: false,
        showGranularity: true,
        showAggregateAllGranularity: false,
        needComparisonTimeRange: true,
        showSelfServiceBoard: false
    }
    var result_form_template = HandleBarsTemplates.template_form(dasboard_section_options)
    $("#dashboard-section #form-area").append(result_form_template);

    //COMPARE
    var compare_section_options = {
        tabName: "compare",
        needQueryForm: true,
        showDashboardSelection: false,
        showMultiMetricSelection: true,
        showSingleMetricSelection: false,
        showDimensionSelection: true,
        showFilterSelection: true,
        showGranularity: true,
        showAggregateAllGranularity: true,
        needComparisonTimeRange: true,
        showSelfServiceBoard: false
    }
    var result_form_template = HandleBarsTemplates.template_form(compare_section_options)
    $("#compare-section #form-area").append(result_form_template);

    //TIMESERIES
    var timseries_section_options = {
        tabName: "timeseries",
        needQueryForm: true,
        showDashboardSelection: false,
        showMultiMetricSelection: true,
        showSingleMetricSelection: false,
        showDimensionSelection: true,
        showFilterSelection: true,
        showGranularity: true,
        showAggregateAllGranularity: false,
        needComparisonTimeRange: false,
        showSelfServiceBoard: false
    }

    var result_form_template = HandleBarsTemplates.template_form(timseries_section_options)
    $("#timeseries-section #form-area").append(result_form_template);

    //ANOMALIES
    var anomalies_section_options = {
        tabName: "anomalies",
        needQueryForm: true,
        showDashboardSelection: false,
        showMultiMetricSelection: false,
        showSingleMetricSelection: true,
        showDimensionSelection: false,
        showFilterSelection: false,
        showGranularity: false,
        showAggregateAllGranularity: false,
        needComparisonTimeRange: false,
        showSelfServiceBoard: false
    }

    var result_form_template = HandleBarsTemplates.template_form(anomalies_section_options)
    $("#anomalies-section #form-area").append(result_form_template);

    //SELF SERVICE
    var self_service_section_options = {
        tabName: "self-service",
        needQueryForm: false,
        showDashboardSelection: false,
        showMultiMetricSelection: false,
        showSingleMetricSelection: false,
        showDimensionSelection: false,
        showFilterSelection: false,
        showGranularity: false,
        showAggregateAllGranularity: false,
        needComparisonTimeRange: false,
        showSelfServiceBoard: true
    }

    var result_form_template = HandleBarsTemplates.template_form(self_service_section_options)
    $("#self-service-section #form-area").append(result_form_template);


    /** Handelbars template for main content
     * Todo: based on the requirements of the manage existing anomaly functions decide to use or remove this template **/
//    //SELF SERVICE TAB
//    var result_self_service_template = HandleBarsTemplates.template_self_service("");
//    $("#self-service-display-main-content-section").append(result_self_service_template);


    //Global object with the query params
    hash = parseHashParameters(window.location.hash);
    hash.view = hash.hasOwnProperty("view") ? hash.view : "dashboard";
    $("#" + hash.view + "-header-tab").click();


    /** --- ) Set initial view on pageload ---**/

    //getDataSetList as a callback triggers ajax calls for all data on the form:
    // dashboard list, metric, dimension and dimension value list, and the dataset configs
    //ie. maxDate etc.
    getDataSetList();

    //Add onhashchange event listener to window object to enable back button usage on the browser
    window.onhashchange = locationHashChanged;
    function locationHashChanged() {

        hash = parseHashParameters(window.location.hash);

        //'rand' parameter's role is to trigger hash change in case of page reload
        delete hash.rand

        updateDashboardFormFromHash();


        //close all uikit dropdown
        closeAllUIKItDropdowns()

        //If hash has dataset and (dashboard or (view & metric )trigger form submit
        if( hash.hasOwnProperty("dataset")){

            if( hash.hasOwnProperty("dashboard") || hash.hasOwnProperty("metrics") ){

                switch (hash.view) {
                    case "timeseries":
                        var tab = "timeseries";
                        getTimeSeries(tab);
                        break;
                    case "compare":

                        var tab = "compare";
                        if (hash.aggTimeGranularity.toLowerCase() == "aggregateall") {

                            getHeatmap(tab);
                            //else aggTimeGranularity == HOURS or DAY
                        } else if (hash.hasOwnProperty("metrics")) {

                            if (hash.hasOwnProperty("dimensions")) {
                                getContributors(tab);
                            } else {
                                getTabular(tab);
                            }
                        }
                    break;
                    case "anomalies":

                        var tab = "anomalies";
                      getAnomalies(tab);
                    break;
                    default://dashboard tab

                        var tab = "dashboard"
                        getCustomDashboard(tab);
                    break;
                }

            }
        }
    }
})
