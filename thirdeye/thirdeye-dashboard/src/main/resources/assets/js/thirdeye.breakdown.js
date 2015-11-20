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

    $(".select_all_checkbox").click(function(){

        var currentTable =  $(this).closest("table")

        if($(this).is(':checked')){
            $("input[type='checkbox']", currentTable).attr('checked', 'checked')
            $("input[type='checkbox']", currentTable).prop('checked', true)
            console.log("length" , $("#dimension-contributor-area table input[type='checkbox']").length)
        }else{
            $("input[type='checkbox']", currentTable).removeAttr('checked')
        }

    })
 /* if($(this).is(':checked')){
  console.log("checked")
  $("#dimension-contributor-area table input[checkbox]").prop('checked', true)
  }else{
  console.log("Not checked")
  console.log("length" , $("#dimension-contributor-area table input[checkbox]").length)
  $("#dimension-contributor-area table input[checkbox]").prop('checked', false)



  } */

    /* When a checkbox is clicked loop through each columns that's not displaying ratio values,
    take the total of the cells' value in the column (if the row of the cell is checked  and the value id not N/A) and place the total into the total row.
    Then calculate the sum row ratio column cell value based on the 2 previous column's value.
    */
    $("#dimension-contributor-area table").on("click", $("input[checkbox]"), function() {
        var checkbox = event.target
        if ($(checkbox).is(':checked')) {
            $(checkbox).attr('checked', 'checked')
        } else {
            $(checkbox).removeAttr('checked')
        }
        sumColumn(checkbox)
    })


    //Calculate the total of the columns on pageload
    var tableBodies = $("#dimension-contributor-area tbody")
    for(var i = 0, len = tableBodies.length; i < len; i++){
        sumColumn(tableBodies[i])
    }


})