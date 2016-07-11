    /**--- 4) Eventlisteners ---**/

    /** Dashboard Form related eventlisteners **/

    //Prevent jumping view when href="#" anchors are clicked
    $("#main-view").on("click","a[href='#']", function(event){
       event.preventDefault();
    });


    //Radio selection will highlight selected element
    $("#main-view").on("click", ".radio-options",function(){
        radioOptions(this)
    });

    $("#main-view").on("click",".radio-buttons button",function(){
        radioButtons(this)
    });

    $("#main-view").on("click",".uk-nav-dropdown.single-select li",function(){
        populateSingleSelect(this);

    });

    /** Dataset selection **/
    $("#main-view").on("click",".dataset-option", function(){
        selectDatasetNGetFormData(this)
    });

    $("#main-view").on("click",".close-dropdown-btn", function(){
        closeClosestDropDown(this)
    });

    $("#main-view").on("click", ".close-btn", function() {
        $(this).hide();
    })

    $("#main-view").on("click", ".close-parent", function() {
        $(this).parent().hide();
    })

    /* Form tab selection */

    $("#main-view").on("click",".header-tab", function(){
        switchHeaderTab(this)
    });

    /** Dashboard selection **/
    $("#main-view").on("click",".dashboard-option", function(){
        selectDashboard(this)
    });

    /** Metric selection multi-select**/
    $("#main-view").on("click",".metric-option", function(){
        selectMetrics(this)
    });

    /** Metric selection single-select **/
    $("#main-view").on("click",".single-metric-option", function(){
        selectSingleMetric(this)
    });

    /** Dimension selection **/
    $("#main-view").on("click",".dimension-option", function(){
       selectDimension(this)
    });

    /** Dimension select all **/
    $("#main-view").on("click",".dropdown-toggle-all", function(){
        toggleAllDimensions(this)
    });

    /** Remove selected Metric or Dimension item **/
    $("#main-view").on("click",".remove-selection", function(){
        removeSelection(this)
    });



    /** DASHBOARD FORM RELATED METHODS **/

    /* takes a clicked anchor tag and applies active class to it's prent (li, button) */
    function  radioOptions(target){
        $(target).parent().siblings().removeClass("uk-active");
        $(target).parent().addClass("uk-active");
    }

    function radioButtons(target){

        if(!$(target).hasClass("uk-active")) {
            $(target).siblings().removeClass("uk-active");
            $(target).addClass("uk-active");
        }
    }

    function populateSingleSelect(target){

        var selectorRoot = $(target).closest("[data-uk-dropdown]");
        var value = $(target).attr("value");
        $("div:first-child", selectorRoot).text(value);
        $("div:first-child", selectorRoot).attr("value", value);
    }

    //Advanced settings
    function closeClosestDropDown(target){

        $(target).closest($("[data-uk-dropdown]")).removeClass("uk-open");
        $(target).closest($("[data-uk-dropdown]")).attr("aria-expanded", false);
        $(target).closest(".uk-dropdown").hide();
    }

    function disableApplyButton(button){
        $(button).prop("disabled", true);
        $(button).attr("disabled", true);
    }

    function selectDatasetNGetFormData(target){


        //Cleanup form: Remove added-item and added-filter, metrics of the previous dataset
        $("#"+  hash.view  +"-chart-area-error").hide();
        $(".view-metric-selector .added-item").remove();
        $(".view-dimension-selector .added-item").remove();
        $(".metric-list").empty();
        $(".single-metric-list").empty();
        $("#selected-metric").html("Select metric");
        $("#selected-metric").attr("value", "");
        $(".dimension-list").empty();
        $(".filter-dimension-list").empty()
        $(".filter-panel .value-filter").remove();
        $(".added-filter").remove();
        $(".filter-panel .value-filter").remove();


        //Remove previous dataset's hash values
        delete hash.baselineStart;
        delete hash.baselineEnd;
        delete hash.currentStart;
        delete hash.currentEnd;
        delete hash.compareMode;
        delete hash.dashboard;
        delete hash.metrics;
        delete hash.dimensions;
        delete hash.filters;
        delete hash.aggTimeGranularity;
        $(".display-chart-section").empty();


        var value = $(target).attr("value");
        hash.dataset = value;

        //Trigger AJAX calls
        //get the latest available data timestamp of a dataset
        getAllFormData()

        //Close uikit dropdown
        $(target).closest("[data-uk-dropdown]").removeClass("uk-open");
        $(target).closest("[data-uk-dropdown]").attr("aria-expanded", false);


    }

    function switchHeaderTab(target){
        radioButtons(target)
        hash.view = $(target).attr("rel");
    }

    function  selectDashboard(target){

        //Update hash values
        var value = $(target).attr("value");
        hash.dashboard = value;

        //Update selectors
//        $("#selected-dashboard").text($(target).text());
//        $("#selected-dashboard").attr("value",value);

        //If previously error was shown hide it
        if($("#"+ hash.view +"-time-input-form-error").attr("data-error-source") == "dashboard-option"){
            $("#"+ hash.view +"-time-input-form-error").hide();
        }

        //close uikit dropdown
        $(target).closest("[data-uk-dropdown]").removeClass("uk-open");
        $(target).closest("[data-uk-dropdown]").attr("aria-expanded", false);

        //Remove earlier selected added-item and added-filter and update hash
        delete hash.metrics
        delete hash.dimensions
        delete hash.filters

        //Enable Go btn
        enableFormSubmit()

    }


    function selectMetrics(target){

        //We have either dashboard or metrics param in the queries
        delete hash.dashboard

        //Update hash values
        var param = "metrics";
        var value = $(target).attr("value");

        //if key doesn't exist
        if(!hash["metrics"]){
            hash["metrics"] = value;

            //if key exist and value is not part of the array
        }else if( hash["metrics"].indexOf(value) < 0){
            hash["metrics"] =  hash["metrics"] +","+ value;
        }

        //Hide alert
        if($("#"+ hash.view +"-time-input-form-error").attr("data-error-source") == "metric-option"){
            $("#"+ hash.view +"-time-input-form-error").hide();
        }

        //Update selectors
        $(target).hide();
        $(".selected-metrics-list[rel='"+ hash.view +"']").append("<li class='added-item uk-button remove-selection' rel='"+ param + "' value='"+ value +"'><a href='#'>" + $(target).text() +  "<i class='uk-icon-close'></i></a></li>");

        //Enable Go btn
        enableFormSubmit()
    }

    function  selectSingleMetric(target) {

        //Update hash values
        var value = $(target).attr("value");
        hash.metrics = value;

        //Update selectors
        $("#selected-metric").text($(target).text());
        $("#selected-metric").attr("value", value)

        //Hide alert
        if($("#"+ hash.view +"-time-input-form-error").attr("data-error-source") == "metric-option"){
            $("#"+ hash.view +"-time-input-form-error").hide();
        }

        //Enable Go btn
        enableFormSubmit()

    }

    function selectDimension(target){

        //Update hash values
        var param = "dimensions";
        var value = $(target).attr("value");

        //Update hash

        //if key doesn't exist
        if(!hash[param]){
            hash[param] = value;

            //if key exist and value is not part of the array
        }else if( hash[param].indexOf(value) < 0){
            hash[param] =  hash[param] +","+ value;
        }

        //Enable Go btn
        $("#" + hash.view + "-form-submit").prop("disabled", false);

        //Update selectors
        $(target).attr("selected", true);
        $(target).hide();
        $(".selected-dimensions-list[rel='"+ hash.view +"']").append("<li class='added-item  uk-button remove-selection' rel='"+ param + "' value='"+ value +"'><a href='#'>" + $(target).text() +  "<i class='uk-icon-close'></i></a></li>");

    }

    function toggleAllDimensions(target){
        //Todo:
    }

    function removeSelection(target){

        //remove the item from the hash
        var param = $(target).attr("rel");
        var value = $(target).attr("value");

        if(hash.hasOwnProperty(param)) {

            hash[param] = hash[param].replace(value, "")

            if (hash[param].indexOf(",") == 0) {
                hash[param] = hash[param].substr(1, hash[param].length);
            } else if (hash[param][hash[param].length - 1] == ",") {
                hash[param] = hash[param].substr(0, hash[param].length - 1);
            } else if (hash[param].indexOf(",,") > -1) {
                hash[param] = hash[param].replace(",,", ",");
            }
            if (hash[param] == "") {
                delete hash[param];
            }
        }

        //Remove the label and make the list item available
        $(target).remove();
        $("li[rel="+ param +"][value="+ value  +"]").show();

        //Enable Go btn
        enableFormSubmit()
    }