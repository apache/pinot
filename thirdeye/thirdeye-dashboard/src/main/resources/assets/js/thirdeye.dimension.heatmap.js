$(document).ready(function() {
    // Tabular view related code
    //Add DataTables plugin functionality to the HeatMap Tabular mode tables
    $('.dimension-heat-map-table-section table').each(function() {
        var currentID = this.id;
        $('#' + currentID).dataTable({
            "aoColumnDefs": [
			   {
			   "aTargets": [3, 4, 5],
			   "fnCreatedCell": function (nTd, sData, oData, iRow, iCol) {
			     var $cell = $(nTd);
			     var commaValue = $cell.text().replace(/(\d)(?=(\d\d\d)+(?!\d))/g, "$1,");
			     $cell.text(commaValue);
			   }
			   }
			],
            //turn off default table resizing when toggling columns
            "bAutoWidth": false,
            "iDisplayLength": 10,
            //Set cell font color to blue/red based on cell value
            "createdRow": function(row, data, index) {
                if (data[5].replace(/[\$,]/g, '') * 1 < 0) {
                    $('td', row).eq(5).css('color', 'Red');
                }else if (data[5].replace(/[\$,]/g, '') * 1 > 0){
                    $('td', row).eq(5).css('color', 'Blue');
                }else if(data[5] == null){
                    $('td', row).eq(5).html("")
                }
                if (data[6].replace(/[\$,]/g, '') * 1 < 0) {
                    $('td', row).eq(6).css('color', 'Red');
                }else if(data[6].replace(/[\$,]/g, '') * 1 > 0){
                    $('td', row).eq(6).css('color', 'Blue');
                }else if(data[6] == null){
                    $('td', row).eq(5).html("")
                }
                if (data[7].replace(/[\$,]/g, '') * 1 < 0) {
                    $('td', row).eq(7).css('color', 'Red');
                }else if(data[7].replace(/[\$,]/g, '') * 1 > 0){
                    $('td', row).eq(7).css('color', 'Blue');
                }else if(data[7] == null){
                    $('td', row).eq(5).html("")
                }
                if (data[8].replace(/[\$,]/g, '') * 1 < 0) {
                    $('td', row).eq(8).css('color', 'Red');
                }else if(data[8].replace(/[\$,]/g, '') * 1 > 0){
                    $('td', row).eq(8).css('color', 'Blue');
                }else if(data[8] == null){
                    $('td', row).eq(5).html("")
                }
                if (data[9].replace(/[\$,]/g, '') * 1 < 0) {
                    $('td', row).eq(9).css('color', 'Red');
                }else if(data[9].replace(/[\$,]/g, '') * 1 > 0){
                    $('td', row).eq(9).css('color', 'Blue');
                }else if(data[9] == null){
                    $('td', row).eq(5).html("")
                }

            },
            //Display the dimensions in descending order to be consistent with the heatmap display order
            "order": [[ 0, "desc" ]]
        }).columnFilter({
            sRangeFormat: "min {from} max {to}",
            sPlaceHolder: "head:after",
            //turn off default table resizing when toggling columns
            'bAutoWidth': false,
            aoColumns: [{
                type: "select"
            }, {
                type: "number-range"
            }, {
                type: "text"
            }, {
                type: "number-range"
            }, {
                type: "number-range"
            }, {
                type: "number-range"
            }, {
                type: "number-range"
            }, {
                type: "number-range"
            }, {
                type: "number-range"
            }, {
                type: "number-range"
            }, {
                type: "number-range"
            }]
        })




        //Rearrange the select options of the dimension column to be displayed in descending order
        var selectOptionAry = $(".select_filter[rel='0'] option", $('#' + currentID))
        //The initially selected "Dimension" option is not part of the alphabetic sort, remove then concatenate it
        var initialSelected = selectOptionAry.get().splice(1, selectOptionAry.length)
        selectOptionAry = selectOptionAry.get().reverse().pop()
        var newSelectOptionArray = initialSelected.concat(selectOptionAry)

        $(".select_filter[rel='0']").empty()

        //Append the options in the new order to the select element
        $(newSelectOptionArray).each(function(index, option){
            $(".select_filter[rel='0']").prepend(option)
        })



    });

    //Set default min/max filter values: max of Range column and min of 'Contribution to total %' column
    var dataTableMaxRangeFilter = 10;
    var dataTableMinContributionMinFilter = 1;

    $(".filter-header [rel ='rank'] input:nth-of-type(2)").val(dataTableMaxRangeFilter).keyup()

    $(".filter-header [rel ='contribution_to_total_change_ratio'] input:first-child").val(dataTableMinContributionMinFilter).keyup()

    // Hide columns (column-index 7, 8, 9) by default
    var tableWrappers = $(".dimension-heat-map-table-section")

    tableWrappers.each(function(index, tableWrapper){
        var tableID = $(".dataTable", tableWrapper).attr('id')
        var table = $('#' + tableID).DataTable()
        // Get the column API object
        var columns = table.columns([7,8,9])

        // Get the column API object
        columns.visible(false);
    })

    //Add column hide/show toggle functionality to column-index 7,8,9
    $('.dimension-tabular-column-toggle-buttons button.toggle-vis').on( 'click', function (e) {
        e.preventDefault();

        var tableWrapper = $(this).closest(".dimension-heat-map-table-section")
        var tableID = $(".dataTable", tableWrapper).attr('id')
        var table = $('#' + tableID).DataTable()
        // Get the column API object
        var column = table.column($(this).attr('data-column'))

        //Check the "eye" icon state on the button
        var iconMode = column.visible() ? "uk-icon-eye":"uk-icon-eye-slash"
        var iconNextMode = column.visible() ? "uk-icon-eye-slash":"uk-icon-eye"

        // Toggle the visibility of the column
        column.visible(!column.visible());

        //Toggle the icon on the button
        $('i', this ).removeClass(iconMode)
        $('i', this).addClass(iconNextMode)
    });

    //Add click event listener to table dimension value cells
    $(".dimension-heat-map-table-section").delegate("td[dimension-val]","click",function(){
        var currentRow = $(this).closest('tr')
        var dimension = $('td:first-child' , currentRow).html().trim()

        var value =  $(this).html().trim()
        /* when URI was handled as an object
        var dimensionValues = parseDimensionValues(window.location.search)
        dimensionValues[dimension] = value
        window.location.search = encodeDimensionValues(dimensionValues)*/

        var dimensionValues = parseDimensionValuesAry(window.location.search)
        dimensionValues.push(dimension + "=" + value)
        window.location.search = encodeDimensionValuesAry(dimensionValues)
    })

  // Treemap related code

    $(".dimension-treemap-mode").click(function() {

        var currentMode = $(this).attr('mode')

       var currentArea = $(this).closest(".dimension-heat-map-treemap-section");


        // Set in URI
        var hash = parseHashParameters(window.location.hash)
        hash['treeMapMode'] = currentMode
        window.location.hash = encodeHashParameters(hash)

        // Display related treemap
        $(".treemap-container", currentArea).hide()
        $($(".treemap-container", currentArea)[currentMode]).show()

       //Change icon on the radio buttons
        $('.dimension-treemap-mode i', currentArea).removeClass("uk-icon-eye")
        $('.dimension-treemap-mode i', currentArea).addClass("uk-icon-eye-slash")
        $('i', this).removeClass("uk-icon-eye-slash")
        $('i', this).addClass("uk-icon-eye")

    })



    //  Heat Map Tabular view & Treemap related code

    //Display baseline and current date time next to the title of the table and the treemap
    var path = parsePath(window.location.pathname)
    var tz = getTimeZone();

    var baselineDateTime = moment(parseInt(path.baselineMillis)).tz(tz).format("YYYY-MM-DD HH:mm z")
    $(".baseline-date-time").html(" " + baselineDateTime)


    var currentDateTime = moment(parseInt(path.currentMillis)).tz(tz).format("YYYY-MM-DD HH:mm z")
    $(".current-date-time").html(" " + currentDateTime)


    //Indicate baseline total value increase/decrease with red/blue colors next to the title of the table and the treemap
    $(".title-box .delta-ratio, .title-box .delta-value").each(function(index, currentDelta){

        var delta = $(currentDelta).html().trim().replace(/[\$,]/g, '') * 1

        if ( delta != 0 && !isNaN(delta)){
            var color = delta > 0 ? "blue plus-symbol" : "red"

            $(currentDelta).addClass(color)
        }
    })


    //Heatmap and Datatable Tabs
    $(".heatmap-tabs li").on("click", function(){
        if(!$(this).hasClass("uk-active")) {
            $(".dimension-heat-map-table-section").toggleClass("hidden")
            $(".dimension-heat-map-treemap-section").toggleClass("hidden")
        }
    })
})



