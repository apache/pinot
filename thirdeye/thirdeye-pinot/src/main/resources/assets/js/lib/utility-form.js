/** DASHBOARD FORM RELATED METHODS **/
/* used on href="#" anchor tags prevents the default jump to the top of the page */
function eventPreventDefault(event){
    event.preventDefault();
}

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

//Advanced settings
function closeClosestDropDown(target){

    $(target).closest($("[data-uk-dropdown]")).removeClass("uk-open");
    $(target).closest($("[data-uk-dropdown]")).attr("aria-expanded", false);
    $(target).closest(".uk-dropdown").hide();
}

function enableApplyButton(button){
    $(button).prop("disabled", false);
    $(button).removeAttr("disabled");
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
    $(".dimension-list").empty();
    $(".filter-dimension-list").empty()
    $(".filter-panel .value-filter").remove();
    $(".added-filter").remove();
    $(".filter-panel .value-filter").remove();


    //Remove previous dataset's hash values
    delete hash.baselineStart
    delete hash.baselineEnd
    delete hash.currentStart
    delete hash.currentEnd
    delete hash.compareMode
    delete hash.dashboard
    delete hash.metrics
    delete hash.dimensions
    delete hash.filters
    delete hash.aggTimeGranularity


    var value = $(target).attr("value");
    hash.dataset = value;

    //Trigger AJAX calls
    //get the latest available data timestamp of a dataset
    getAllFormData()

    //Populate the selected item on the form element
    $(".selected-dataset").text($(target).text());
    $(".selected-dataset").attr("value",value);

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
    $("#selected-dashboard").text($(target).text());
    $("#selected-dashboard").attr("value",value);

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


function selectMetric(target){


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