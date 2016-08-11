/**--- 4) Eventlisteners ---**/

/** Dashboard Form related eventlisteners **/

    //Prevent jumping view when href="#" anchors are clicked
$("#main-view").on("click", "a[href='#']", function (event) {
    event.preventDefault();
});

$("#main-view").on("click", ".radio-options", function () {
    radioOptions(this)
});

$("#main-view").on("click", ".radio-buttons button", function () {
    radioButtons(this)
});

$("#main-view").on("click", ".uk-nav-dropdown.single-select li", function () {
    populateSingleSelect(this);

});

$("#main-view").on("click", ".close-dropdown-btn", function () {
    closeClosestDropDown(this)
});

$("#main-view").on("click", ".close-btn", function () {
    $(this).hide();
})

$("#main-view").on("click", ".close-parent", function () {
    $(this).parent().hide();
})


/** Dataset selection **/
$("#main-view").on("click", ".dataset-option", function () {
    selectDatasetNGetFormData(this)
});

/** Dashboard selection **/
$("#main-view").on("click", ".dashboard-option", function () {
    selectDashboard(this)
});

/** Metric selection multi-select**/
$("#main-view").on("click", ".metric-option", function () {
    selectMetrics(this)
});

/** Metric selection single-select **/
$("#main-view").on("click", ".single-metric-option", function () {
    selectSingleMetric(this)
});

/** Dimension selection **/
$("#main-view").on("click", ".dimension-option", function () {
    selectDimension(this)
});

/** Dimension select all **/
$("#main-view").on("click", ".dropdown-toggle-all", function () {
    toggleAllDimensions(this)
});

/** Remove selected Metric or Dimension item **/
$("#main-view").on("click", ".remove-selection", function () {
    removeSelection(this)
});


/** Remove selected Metric or Dimension item **/
$("#main-view").on("click", "#get-existing-anomaly-functions", function () {
    getExistingAnomalyFunctions(hash.dataset);
});


/** Time input related events are defined in dashboard-form-time.js
 *  Filters related events are defined in dashboard-form-filter.js **/


/** DASHBOARD FORM RELATED METHODS **/
function selectDashboard(target) {

    //Update hash values
    var value = $(target).attr("value");
    hash.dashboard = value;

    //If previously error was shown hide it
    if ($("#" + hash.view + "-time-input-form-error").attr("data-error-source") == "dashboard-option") {
        $("#" + hash.view + "-time-input-form-error").hide();
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


function selectMetrics(target) {

    //We have either dashboard or metrics param in the queries
    delete hash.dashboard

    //Update hash values
    var param = "metrics";
    var value = $(target).attr("value");

    //if key doesn't exist
    if (!hash["metrics"]) {
        hash["metrics"] = value;

        //if key exist and value is not part of the array
    } else if (hash["metrics"].indexOf(value) < 0) {
        hash["metrics"] = hash["metrics"] + "," + value;
    }

    //Hide earlier error message
    if ($("#" + hash.view + "-time-input-form-error").attr("data-error-source") == "metric-option") {
        $("#" + hash.view + "-time-input-form-error").hide();
    }

    //Update selectors: hide option if it's in multi metric selector
    if( !$(target).parent().hasClass("single-metric-list") ){
        $(target).hide();
    }

    $(".selected-metrics-list[rel='" + hash.view + "']").append("<li class='added-item uk-button remove-selection' rel='" + param + "' value='" + value + "'><a href='#'>" + $(target).text() + "<i class='uk-icon-close'></i></a></li>");

    //Enable Go btn
    enableFormSubmit()
}

function selectSingleMetric(target) {

    //Update hash values
    var value = $(target).attr("value");
    hash.metrics = value;

    //Update selectors
    $("#selected-metric").text($(target).text());
    $("#selected-metric").attr("value", value)

    //Hide alert
    if ($("#" + hash.view + "-time-input-form-error").attr("data-error-source") == "metric-option") {
        $("#" + hash.view + "-time-input-form-error").hide();
    }

    //Enable Go btn
    enableFormSubmit()

}

function selectDimension(target) {

    //Update hash values
    var param = "dimensions";
    var value = $(target).attr("value");

    //Update hash

    //if key doesn't exist
    if (!hash[param]) {
        hash[param] = value;

        //if key exist and value is not part of the array
    } else if (hash[param].indexOf(value) < 0) {
        hash[param] = hash[param] + "," + value;
    }

    //Enable Go btn
    $("#" + hash.view + "-form-submit").prop("disabled", false);

    //Update selectors
    $(target).attr("selected", true);
    $(target).hide();
    $(".selected-dimensions-list[rel='" + hash.view + "']").append("<li class='added-item  uk-button remove-selection' rel='" + param + "' value='" + value + "'><a href='#'>" + $(target).text() + "<i class='uk-icon-close'></i></a></li>");

}

function toggleAllDimensions(target) {
    //Todo:
}

function removeSelection(target) {

    //remove the item from the hash
    var param = $(target).attr("rel");
    var value = $(target).attr("value");

    if (hash.hasOwnProperty(param)) {

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
    $("li[rel=" + param + "][value=" + value + "]").show();

    //Enable Go btn
    enableFormSubmit()
}



