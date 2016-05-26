/** DASHBOARD FORM FILTER RELATED METHODS **/
function  selectFilterDimensionOption(target){
    $(".value-filter").hide();
    var dimension= $(target).attr("value")
    $(".value-filter[rel='"+ dimension +"' ]").show();
}

function applyFilterSelection(){

    var currentTabFilters = $("#"+hash.view+"-filter-panel");

    //Set hash params
    var filters = {};
    var labels = {};

    $(".filter-value-checkbox", currentTabFilters).each(function(i, checkbox) {
        var checkboxObj = $(checkbox);

        if (checkboxObj.is(':checked')) {
            var key = $(checkbox).attr("rel");
            var value = $(checkbox).attr("value");
            var valueAlias = $(checkbox).parent().text();

            if(filters[key]){
                filters[key].push(value) ;
                //using alias for "", "?" values
                labels[key].push(valueAlias) ;
            }else{
                filters[key] = [value];
                labels[key] = [valueAlias];
            }
        }
    });

    hash.filters = encodeURIComponent(JSON.stringify(filters));

    //Disable Apply filters button and close popup
    enableApplyButton()

    //Todo: Show selected filters on dashboard
    //empty previous filters labels
    $(".added-filter[tab='"+ hash.view +"']").remove()

    //append new labels
    var html = "";
    for(k in labels){
        var values = decodeURIComponent(labels[k])
        html +=  "<li class='added-filter uk-button remove-filter-selection' tab="+ hash.view +" rel='" + k + "' value='" + labels[k] + "' title='" + k + ": " + values +  "'>" + k + ": " + values + "<i class='uk-icon-close'></i></li>";
    }

    $(".selected-filters-list[rel='"+ hash.view +"']").append(html);

    //Enable Go btn
    enableFormSubmit()
    $("#" + hash.view +"-filter-panel").hide();
}

/*takes an object with dimensionNames as keys and an array of dimensionValues as values,
 applies them to the current form and enables form submit */
function updateFilterSelection(filterParams){
    var currentFilterContainer = $(".view-filter-selector[rel='"+ hash.view +"']");
    var elementsPresent = 1;
    $(".filter-value-checkbox", currentFilterContainer).prop("checked", false);
    $(".filter-select-all-checkbox").prop("checked", false);

    for(var f in filterParams){
        var dimensionValues = filterParams[f];

        for(var v =0 , len = dimensionValues.length; v < len; v++){
            if($(".filter-value-checkbox[rel='"+ f +"'][value='"+ dimensionValues[v] +"']").length == 0){
                elementsPresent =0;
                break;
            }
            $(".filter-value-checkbox[rel='"+ f +"'][value='"+ dimensionValues[v] +"']", currentFilterContainer).prop("checked", true);
        }
    }

    if(elementsPresent == 1) {
        //Enable then trigger apply btn
        $('.apply-filter-btn', currentFilterContainer).prop("disabled", false);
        $(".apply-filter-btn", currentFilterContainer).click();
        $(".apply-filter-btn", currentFilterContainer).parent("a.uk-dropdown-close").click();
    }
}

function readFiltersAppliedInCurrentView(currentTab){
    var currentFilterContainer = $(".view-filter-selector[rel='"+ currentTab +"']")
    var filters = {};

    $(".added-filter",currentFilterContainer).each(function(){
        var keyValue = $(this).attr("title").trim().split(":");
        var dimension = keyValue[0];
        var valuesAryToTrim = keyValue[1].trim().split(",")
        var valuesAry = [];
        for(var index=0, len= valuesAryToTrim.length; index < len; index++){
            var value = valuesAryToTrim[index].trim();
            if(value == "UNKNOWN"){
                value = "";
            }

            valuesAry.push(value)
        }
        filters[dimension] = valuesAry;
    })
    return filters
}

function enableFormSubmit(){

    $("#" + hash.view + "-form-submit").prop("disabled", false);
    $("#" + hash.view + "-form-submit").removeAttr("disabled");
}