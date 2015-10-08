$(document).ready(function() {

    //When a funnel thumbnail is clicked display it's content in the main funnel display section
    $("#funnel-thumbnails>div").click(function(){

        //Draw the selected thumbnail table and title in the main section
        $(".custom-funnel-section").html($(this).html())

        //Highlight currently selected thumbnail
        $("#funnel-thumbnails>div").removeClass("uk-panel-box")
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
    $("#funnel-thumbnails>div:first-of-type").trigger("click")

});