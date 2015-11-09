$(document).ready(function() {
    // Sumary and details tab toggle
    $(".funnel-tabs li").on("click", function(){
        if(!$(this).hasClass("uk-active")) {
            $(".details-cell").toggleClass("hidden")
            $(".subheader").toggleClass("hidden")
            $('.contributors-table-time').attr('colspan', function(index, attr){
                return attr == 3 ? null : 3;
            });
            $("#dimension-contributor-area table").toggleClass("fixed-table-layout")
        }
    })

    /* Assign background color value to each heat-map-cell */
    $(".heat-map-cell").each(function (i, cell) {
        calcHeatMapCellBackground(cell);
    });

    /* Transform UTC time into user selected or browser's timezone and display date */
    $(".contributors-table-date").each(function(i, cell){
        transformUTCToTZDate(cell);
    });

    /* Transform UTC time into user selected or browser's timezone and display time */
    $(".contributors-table-time").each(function(i, cell){
        var dateTimeFormat = 'MM:DD HH:mm'
        transformUTCToTZTime(cell, dateTimeFormat);
    });


    /* 1) When a checkbox is clicked loop through each columns that's index # %3 == 1 or 2 of that table and total the elements that are not N/A or checked into the total cell, then calculate the ratio

       // add them up on pageload total and cumulative
       //total should be displayed on top
       //format font in th
    */

    $("#dimension-contributor-area table").on("click", $("input[checkbox]"), function(e) {
        var checkbox = event.target
        if ($(checkbox).is(':checked')) {
            $(checkbox).attr('checked', 'checked')
        } else {
            $(checkbox).removeAttr('checked')
        }
        sumColumn(checkbox)
    })
    function sumColumn(col){
        var currentTable =  $(col).closest("table")
        var firstDataRow = $("tr.data-row", currentTable)[0]
        var columns = $("td",firstDataRow)
        var isCumulative = !$("tr.cumulative-values.hidden",  currentTable)[0]
        //Work with the cumulative or hourly total row
        var sumRow = (isCumulative) ?  $("tr.cumulative-values.sum-row",  currentTable)[0] : $("tr.hourly-values.sum-row",  currentTable)[0]

        //Loop through each column, except for column index 0-2 since those have string values
        for(var z= 3, len = columns.length; z < len; z++){

            //Filter out ratio columns
            if( (z + 1 ) % 3 !== 0 ){

                var rows =  $("tr.data-row", currentTable)
                //Check if cumulative table is displayed
                var i =  (isCumulative) ?  1 : 0
                var sum = 0
                for(var rlen = rows.length; i < rlen; i = i + 2){

                        //Check if checkbox of the row is selected
                        if( $("input", rows[i]).is(':checked')) {
                            var currentRow = rows[i]
                            var currentCell = $("td", currentRow)[z]
                            var currentCellVal =  parseInt($(currentCell).html().trim().replace(/[\$,]/g, ''))
                            //NaN value will be skipped
                            if (!isNaN(currentCellVal)) {
                                sum = sum + currentCellVal
                            }
                        }
                }

                //Display the sum in the current column of the sumRow
                var sumCell = $("th", sumRow)[z-2]
                $(sumCell).html(sum)

            //In case of ratio columns calculate them based on the baseline and current values of the timebucket
            }else{
                //take the 2 previous total row elements
                var baselineValCell = $("th", sumRow)[z-4]
                var currentValCell = $("th", sumRow)[z-3] //take the 2 previous total row elements
                var baselineVal = parseInt($(baselineValCell).html().trim().replace(/[\$,]/g, ''))
                var currentVal = parseInt($(currentValCell).html().trim().replace(/[\$,]/g, ''))
                var sumCell = $("th", sumRow)[z-2]
                //Round the ratio to 2 decimal places, add 0.00001 to prevent Chrome rounding 0.005 to 0.00
                var ratioVal = (Math.round(((currentVal - baselineVal) / baselineVal + 0.00001) * 1000)/10).toFixed(1)

                $(sumCell).html(ratioVal + "%")
                $(sumCell).attr('value' , (ratioVal /100))
                calcHeatMapCellBackground(sumCell)
            }

        }

    }

    var tableBodies = $("#dimension-contributor-area tbody")
    for(var i = 0, len = tableBodies.length; i < len; i++){
        sumColumn(tableBodies[i])
    }
})