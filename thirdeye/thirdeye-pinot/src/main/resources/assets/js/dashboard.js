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

    /** Handelbars template for forms on tabs* */
    //DASHBOARD
    var dasboard_section_options = {
        tabName: "dashboard",
        showDashboardSelection: true,
        showMetricSelection: false,
        showDimensionSelection: false,
        showFilterSelection: false,
        showTimeSelection: true,
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
        showAggregateAllGranularity: false,
        needComparisonTimeRange: false
    } 

    var result_form_template = HandleBarsTemplates.template_form(timseries_section_options)
    $("#timeseries-section #form-area").append(result_form_template);

    //Global object with the query params
    hash = parseHashParameters(window.location.hash);
    hash.view = hash.hasOwnProperty("view") ? hash.view : "dashboard";
    $("#" + hash.view + "-header-tab").click();

    //populate default time in form
    populateTodayDateTime()
    /** --- ) Set initial view on pageload ---**/

    //getDataSetList as a callback triggers ajax calls for all data on the form:
    // dashboard list, metric, dimension and dimension value list
    getDataSetList();


    //Add onhashchange event listener to window object to enable back button usage on the browser
    window.onhashchange = locationHashChanged;
    function locationHashChanged() {

        hash = parseHashParameters(window.location.hash);
        updateDashboardFormFromHash();


        //Todo: check what is opening the uikit popups and close them accordingly
        //close uikit dropdown
        $("[data-uk-dropdown]").removeClass("uk-open");
        $("[data-uk-dropdown]").attr("aria-expanded", false);

        //If hash has dataset and (dashboard or (view & metric )trigger form submit
        if( hash.hasOwnProperty("dataset")){

            if( hash.hasOwnProperty("dashboard") || hash.hasOwnProperty("metrics") ){

                switch (hash.view) {
                    case "timeseries":

                        getTimeSeries();
                        break;
                    case "compare":

                        if (hash.aggTimeGranularity.toLowerCase() == "aggregateall") {

                            getHeatmap();
                            //else aggTimeGranularity == HOURS or DAY
                        } else if (hash.hasOwnProperty("metrics")) {

                            if (hash.hasOwnProperty("dimensions")) {

                                getContributors();
                            } else {
                                getTabular();
                            }
                        }
                        break;
                    default://dashboard tab

                        getCustomDashboard();
                        break;
                }

            }
        }
    }
})