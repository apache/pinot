$(document).ready( function() {


    /** Handelbars template for tabs **/
    //DASHBOARD
    var dasboard_tab_options = {
        tabName: "dashboard"
    }
    var result_tab_template = HandleBarsTemplates.template_tab(dasboard_tab_options)
    $("#dashboard").append(result_tab_template);

    //COMPARE
    var compare_tab_options = {
        tabName: "compare"
    }
    var result_tab_template = HandleBarsTemplates.template_tab(compare_tab_options)
    $("#compare").append(result_tab_template);

    //TIMESERIES
    var timseries_tab_options = {
        tabName: "timeseries"
    }
    var result_tab_template = HandleBarsTemplates.template_tab(timseries_tab_options)
    $("#timeseries").append(result_tab_template);

    //ANOMALIES
    var anomalies_tab_options = {
        tabName: "anomalies"
    }
    var result_tab_template = HandleBarsTemplates.template_tab(anomalies_tab_options)
    $("#anomalies").append(result_tab_template);

    /** Handelbars template for forms on tabs* */
    //DASHBOARD
    var dasboard_section_options = {
        tabName: "dashboard",
        showDashboardSelection: true,
        showMetricSelection: false,
        showDimensionSelection: false,
        showFilterSelection: false,
        showTimeSelection: true,
        showGranularity: true,
        showAggregateAllGranularity: false,
        needComparisonTimeRange: true
    }
    var result_form_template = HandleBarsTemplates.template_form(dasboard_section_options)
    $("#dashboard-section #form-area").append(result_form_template);

    //COMPARE
    var compare_section_options = {
        tabName: "compare",
        showDashboardSelection: false,
        showMetricSelection: true,
        showDimensionSelection: true,
        showFilterSelection: true,
        showTimeSelection: true,
        showGranularity: true,
        showAggregateAllGranularity: true,
        needComparisonTimeRange: true
    }
    var result_form_template = HandleBarsTemplates.template_form(compare_section_options)
    $("#compare-section #form-area").append(result_form_template);

    //TIMESERIES
    var timseries_section_options = {
        tabName: "timeseries",
        showDashboardSelection: false,
        showMetricSelection: true,
        showDimensionSelection: true,
        showFilterSelection: true,
        showTimeSelection: true,
        showGranularity: true,
        showAggregateAllGranularity: false,
        needComparisonTimeRange: false
    }

    var result_form_template = HandleBarsTemplates.template_form(timseries_section_options)
    $("#timeseries-section #form-area").append(result_form_template);

    //ANOMALIES
    var anomalies_section_options = {
        tabName: "anomalies",
        showDashboardSelection: false,
        showMetricSelection: true,
        showDimensionSelection: false,
        showFilterSelection: true,
        showTimeSelection: true,
        showGranularity: false,
        showAggregateAllGranularity: false,
        needComparisonTimeRange: false
    }

    var result_form_template = HandleBarsTemplates.template_form(anomalies_section_options)
    $("#anomalies-section #form-area").append(result_form_template);


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

        //close all uikit dropdowns
        closeAllUIKItDropdowns()

        //Render the charts:
        //if the data is cached already (ie. user hit back button) renderChartFromCachedData
        //else trigger ajax call.

        //Check if query is the same as an earlier query
        //in the ajax calls where chart data is received we updated the sessionStorage with the hashstring as key and the data as value
        //here we check if the sessionStorage already contains this query data
        var hashString = window.location.hash.substring(1);
        if (sessionStorage.getItem(hashString)) {
            renderChartFromCachedData(hashString);
        }else{
            //trigger ajax call if hash has dataset and (dashboard or (view & metric )
            requestChartData()
        }
    }

    function renderChartFromCachedData(hashString){
        var data = JSON.parse( sessionStorage.getItem(hashString) );

        //if data was falsy trigger the ajax call
        if(!data){
            requestChartData()
        }

        var hashParams = parseHashParameters(hashString);
        if( hashParams.hasOwnProperty("dataset")){

            if( hashParams.hasOwnProperty("dashboard") || hashParams.hasOwnProperty("metrics") ){

                switch (hashParams.view) {
                    case "timeseries":
                        //passing the tab name (ie: dachboard, compare, timesries etc.)
                        // to the render function so the charts will be displayed on the correct tab
                        // even if user switches the tab while waiting for the ajax response
                        var tab = "timeseries";
                        showLoader(tab);

                        $("#"+  tab  +"-display-chart-section").empty();

                        renderTimeSeries(data, tab)

                        hideLoader(tab)

                        break;
                    case "compare":
                        var tab = "compare";
                        showLoader(tab);
                        $("#"+  tab  +"-display-chart-section").empty();

                        if (hash.aggTimeGranularity.toLowerCase() == "aggregateall") {
                            renderHeatmap(data, tab);
                            //else aggTimeGranularity == HOURS or DAY
                        } else if (hash.hasOwnProperty("metrics")) {

                            if (hash.hasOwnProperty("dimensions")) {
                                renderContributors(data, tab);
                            } else {
                                renderTabular(data, tab);
                            }
                        }
                        hideLoader(tab)
                        break;
                    case "anomalies":
                        var tab = "anomalies";
                        var anomalyData = data.anomalyData;
                        var timeSeriesData = data.timeSeriesData;
                        showLoader(tab);
                        $("#"+  tab  +"-display-chart-section").empty();

                            renderAnomalyLineChart(timeSeriesData, anomalyData, tab);
                            renderAnomalyTable(anomalyData, tab);
                        hideLoader(tab)
                        break;
                    default://dashboard tab
                        showLoader(tab);
                        var tab = "dashboard";
                        $("#"+  tab  +"-display-chart-section").empty();
                        renderTabular(data, tab);
                        hideLoader(tab);
                        break;
                }
            }
        }


    }

    function requestChartData(){
        //If hash has dataset and (dashboard or (view & metric )trigger ajax call
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
