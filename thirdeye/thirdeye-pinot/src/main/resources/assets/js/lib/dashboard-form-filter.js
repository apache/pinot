    //Filters related eventlisteners

    //Enable Apply btn
    $("#main-view").on("change", ".filter-value-checkbox, .filter-select-all-checkbox", function(){
        enableApplyButton( $("#"+hash.view+"-apply-filter-btn") )

    });

    //Apply filters
    $("#main-view").on("click", ".apply-filter-btn",function(){
       applyFilterSelection()

    });


    $("#main-view").on("click",".filter-select-all-checkbox", function(){
        var valueList = $(this).parent().next("div");

        if($(this).is(':checked')){

            $("input", valueList).attr('checked', 'checked');
            $("input", valueList).prop('checked', true);
        }else{
            $("input", valueList).removeAttr('checked');
        }
    });

    //Toggle dimension values selector list based on selected dimension
    $("#main-view").on("click", ".filter-panel .filter-dimension-option", function(){
       selectFilterDimensionOption(this)
    });
    
    $("#main-view").on("click",".remove-filter-selection", function(){
        var currentTabFilters = $(".filter-panel[rel='" + hash.view + "']");
        //remove the item from the hash by unchecking checkboxes on the panel and applying new selection
        var dimension = $(this).attr("rel");
        var values = $(this).attr("value").split(",");

        for(var i= 0, len = values.length; i<len; i++){
            var value = values[i].replace(/(\r\n|\n|\r)/gm,"").trim();
            $(".filter-value-checkbox[rel="+ dimension +"]", currentTabFilters).removeAttr('checked');
            $(".filter-select-all-checkbox[rel="+ dimension +"]", currentTabFilters).removeAttr('checked');
        }
        //Enable then trigger apply btn
        $(".apply-filter-btn[rel='" + hash.view + "']").prop("disabled", false);
        $(".apply-filter-btn", currentTabFilters ).click();

        //close the dropdown
        $(".filter-panel[rel='" + hash.view + "']").hide();

        //remove the label and make the list item available
        $(this).remove();


        //If no filters are selected remove filters key from hash
        if ($(".added-filter[tab='"+ hash.view +"']").length ==0){
            delete hash.filters;
        }

        //Enable Go btn
        enableFormSubmit()

    });


