$(document).ready(function() {

    //When a funnel thumbnail is clicked display it's content in the main funnel display section
    $("#funnel-thumbnails .funnel").click(function(){

        //Draw the selected thumbnail table and title in the main section
        $("#custom-funnel-section").html($(this).html())
        $("#custom-funnel-section h3").css("display", "inline")

        //Highlight currently selected thumbnail
        $("#funnel-thumbnails .funnel").removeClass("uk-panel-box")
        $(this).addClass("uk-panel-box")
    })

    //Assign background color value to each cells
    $(".heat-map-cell").each(function(i, cell) {
        var cellObj = $(cell)
        var value = parseFloat(cellObj.attr('value'))
        var absValue = Math.abs(value)

        if (value < 0) {
            cellObj.css('background-color', 'rgba(255,0,0,' + absValue + ')') // red
        } else {
            cellObj.css('background-color', 'rgba(0,0,255,' + absValue + ')') // blue
        }
    });

    //Preselect the 1st funnel
    $("#funnel-thumbnails .funnel:first-of-type").trigger("click")

    //Funnel and Metric Intraday tabs
    $(".funnel-tabs li").on("click", function(){
        if(!$(this).hasClass("uk-active")) {
            $("#custom-funnel-section").toggleClass("hidden")
            $("#funnel-thumbnails").toggleClass("hidden")
            $("#metric-table-area").toggleClass("hidden")
            $("#intra-day-buttons").toggleClass("hidden")
            $("#intra-day-table").toggleClass("hidden")
        }
    })

    //todo: heat-map-cell eventlistener
    $("#custom-funnel-section .heat-map-cell").click(function(){
        console.log("target heat-map-cell:",this)
        var rowIndex = $(this).parent().children().index($(this));
        var columnIndex = $(this).parent().parent().children().index($(this).parent());
        console.log("target heat-map-cell row index: ",rowIndex + ", column index: " + columnIndex )
        var hour = $("td:first-child", $(this).closest("tr")).html()
        var metric =  $("#custom-funnel-section thead tr:first-of-type th:nth-child(" + (columnIndex + 2) +")").html()
        console.log("target hour:", hour + ", metric: " + metric)
    })

});