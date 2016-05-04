    /** Time-input-form related eventlisteners of  **/


    $("#main-view").on("click", ".baseline-aggregate",function() {
         selectAggregate(this)
    });

    //When any form field has changed enable Go (submit) button
    $("#main-view").on( "change",
        ".current-start-date, .current-start-time, " +
            ".baseline-start-date,.baseline-start-time, " +
            ".current-end-date,.current-end-time, " +
            ".baseline-end-date,.baseline-end-time",
        function(){
         enableFormSubmit()
    });

    $("#main-view").on("change",".current-date-range-selector",function(){
        selectCurrentDateRange(this)
    });

    $("#main-view").on("click",".time-input-compare-checkbox", function(){
        //Todo: remove this checkbox in some views it's a must in others it's not needed
        toggleTimeComparison(this)
    });

    $("#main-view").on("change",".compare-mode-selector",function() {
        selectBaselineDateRange(this)
    });

    $("#main-view").on("change",".current-start-date-input, .current-start-time-input, .current-end-date-input, .current-end-time-input", function(){
        selectBaselineDateRange(this)
        enableApplyButton( $(".time-input-apply-btn[rel='"+ hash.view +"']"));
    });

    $("#main-view").on("change",".baseline-start-date-input, .baseline-start-time-input, .baseline-end-date-input, .baseline-end-time-input", function(){
        enableApplyButton( $(".time-input-apply-btn[rel='"+ hash.view +"']"))
    });


    $("#main-view").on("click",".baseline-aggregate-copy",function(){
        enableApplyButton( $(".time-input-apply-btn[rel='"+ hash.view +"']"))
    });

    //Date & Time selector dropdown apply
    $("#main-view").on("click",".time-input-apply-btn",function(){
        applyTimeRangeSelection(this)
    });