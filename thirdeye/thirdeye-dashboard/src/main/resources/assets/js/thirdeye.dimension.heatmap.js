$(document).ready(function() {

    var data = $("#dimension-heat-map-data")
    var container = $("#dimension-heat-map-container")
    var filterToggle = $("#dimension-heat-map-filter")

    var hash = parseHashParameters(window.location.hash)
    if (hash['filterState']) {
        filterToggle.attr('state', hash['filterState'])
    }

    var currentMode = hash['heatMapMode']

    if (!currentMode) {
        currentMode = 'self'
    }

    var options = {
        filter: function(cell) {
            if (filterToggle.attr('state') === 'on') {
                // Only show cells which make up 1% or more of the dimension
                return cell.stats['baseline_ratio'] > 0.01 || cell.stats['current_ratio'] > 0.01
            } else {
                return true;
            }
        },
        comparator: function(a, b) {
            var cmp = b.stats['current_value'] - a.stats['current_value'] // reverse
            if (cmp < 0) {
                return -1
            } else if (cmp > 0) {
                return 1
            } else {
                return 0
            }
        },
        display: function(cell) {
            var value = cell.value
            if (value === '?') {
                value = 'OTHER'
            }

            var cellContent = value + '<br/>'

            if (currentMode === 'self') {
                var currentValue = cell.stats['current_value']
                var baselineValue = cell.stats['baseline_value']
                var selfRatio = (currentValue - baselineValue) / baselineValue
                cellContent += (selfRatio * 100).toFixed(2) + '%'
            } else if (currentMode === 'others') {
                var baselineRatio = (cell.stats['baseline_ratio'] * 100).toFixed(2) + '%'
                var contributionDifference = (cell.stats['contribution_difference'] * 100).toFixed(2) + '%'
                cellContent += baselineRatio + ' (' + contributionDifference + ')'
            } else if (currentMode === 'all') {
                var volumeDifference = (cell.stats['volume_difference'] * 100).toFixed(2) + '%'
                cellContent += volumeDifference
            }

            return cellContent
        },
        backgroundColor: function(cell) {
            if (cell.stats['baseline_cdf_value'] == null) {
                return '#ffffff'
            }

            var testStatistic = null
            if (currentMode === 'self') {
                var currentValue = cell.stats['current_value']
                var baselineValue = cell.stats['baseline_value']
                testStatistic = (currentValue - baselineValue) / baselineValue
            } else if (currentMode === 'others') {
                testStatistic = cell.stats['contribution_difference']
            } else if (currentMode === 'all') {
                testStatistic = cell.stats['volume_difference']
            }

            if (testStatistic >= 0) {
                return 'rgba(136, 138, 252, ' + cell.stats['baseline_cdf_value'].toFixed(3) + ')'
            } else {
                return 'rgba(252, 136, 138, ' + cell.stats['baseline_cdf_value'].toFixed(3) + ')'
            }
        },
        groupBy: 'METRIC'
    }

    filterToggle.click(function() {
        var state = filterToggle.attr('state')
        var nextState = state === 'on' ? 'off' : 'on'
        filterToggle.attr('state', nextState)

        // Set in URI
        var hash = parseHashParameters(window.location.hash)
        hash['filterState'] = nextState
        window.location.hash = encodeHashParameters(hash)

        renderHeatMap(data, container, options)
    })

    $(".dimension-heat-map-mode").click(function() {
        currentMode = $(this).attr('mode')

        // Set in URI
        var hash = parseHashParameters(window.location.hash)
        hash['heatMapMode'] = currentMode
        window.location.hash = encodeHashParameters(hash)

        // Display correct explanation
        $("#dimension-heat-map-explanation div").hide()
        $("#dimension-heat-map-explanation-" + currentMode).show()

        // Re-render heat map
        renderHeatMap(data, container, options)

    })

    $("#dimension-heat-map-mode-" + currentMode).trigger('click')


    /* Tabular view related code */
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
            "bAutoWidth": false,
            "iDisplayLength": 10,
            "createdRow": function(row, data, index) {
                if (data[5].replace(/[\$,]/g, '') * 1 < 0) {
                    $('td', row).eq(5).css('color', 'Red');
                }
                if (data[6].replace(/[\$,]/g, '') * 1 < 0) {
                    $('td', row).eq(6).css('color', 'Red');
                }
                if (data[7].replace(/[\$,]/g, '') * 1 < 0) {
                    $('td', row).eq(7).css('color', 'Red');
                }
                if (data[8].replace(/[\$,]/g, '') * 1 < 0) {
                    $('td', row).eq(8).css('color', 'Red');
                }
                if (data[9].replace(/[\$,]/g, '') * 1 < 0) {
                    $('td', row).eq(9).css('color', 'Red');
                }

            },
            //Display the dimensions in descending order to be consistent with the heatmap display order
            "order": [[ 0, "desc" ]]
        }).columnFilter({
            sRangeFormat: "min {from} max {to}",
            sPlaceHolder: "head:after",
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

    console.log('tableWrappers',tableWrappers)
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
        var dimensionValues = parseDimensionValues(window.location.search)
        dimensionValues[dimension] = value
        window.location.search = encodeDimensionValues(dimensionValues)
    })

  /* Treemap related code */

    $(".dimension-treemap-mode").click(function() {

        var currentMode = $(this).attr('mode')

       var currentArea = $(this).closest(".treemap-section");
       // var currentID = $(this).attr('id')
       // console.log("currentArea", currentArea)
       // var currentContainer = $(".treemap-container", currentArea)////
       // var currentContainerID = $(currentContainer).attr("id")
       // var currentMetric = currentContainerID.substr(0, currentContainerID.length - 10)
       // console.log("current metric", currentMetric )

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



    /*  Heat Map Tabular view & Treemap related code*/

    //Display baseline and current date time next to the title of the table and the treemap
    var path = parsePath(window.location.pathname)
    var tz = getTimeZone();

    var baselineDateTime = moment(parseInt(path.baselineMillis)).tz(tz).format("YYYY-MM-DD HH:mm z")
    $(".baseline-date-time").html(" " + baselineDateTime)


    var currentDateTime = moment(parseInt(path.currentMillis)).tz(tz).format("YYYY-MM-DD HH:mm z")
    $(".current-date-time").html(" " + currentDateTime)


    //Indicate baseline total value increase/decrease with red/blue colors next to the title of the table and the treemap
    $(".dimension-heat-map-container-title .delta-ratio").each(function(index, currentDelta){

        var delta = Number( $(currentDelta).html().trim() )
        if (  delta != 0 && !isNaN(delta)){
            var color = delta > 0 ? "blue" : "red"
            $(currentDelta).addClass(color)
        }
    })

})


