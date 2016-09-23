function getTimeSeries(tab) {

    var url = "/dashboard/data/timeseries?" + window.location.hash.substring(1);

    getData(url, tab).done(function (data) {
        //Error handling when data is falsy (empty, undefined or null)
        if (!data) {
            $("#" + tab + "-chart-area-error").empty()
            var warning = $('<div></div>', { class: 'uk-alert uk-alert-warning' })
            warning.append($('<p></p>', { html: 'Something went wrong. Please try and reload the page. Error: data =' + data  }))
            $("#" + tab + "-chart-area-error").append(warning)
            $("#" + tab + "-chart-area-error").show()
            return
        } else {
            $("#" + tab + "-chart-area-error").hide()
        }

        var result_time_series = HandleBarsTemplates.template_time_series(data);
        $("#" + tab + "-display-chart-section").append(result_time_series);
        renderTimeSeriesUsingC3(data, tab)
    });
};

function renderTimeSeriesUsingC3(d, tab) {  //time-series-area

    var dateTimeFormat = "%I:%M %p";
    if (hash.hasOwnProperty("aggTimeGranularity") && hash.aggTimeGranularity == "DAYS") {
        dateTimeFormat = "%m-%d"
    }else if(d.summary.currentEnd - d.summary.currentStart > 86400000 ){
        dateTimeFormat = "%m-%d %I %p";
    }

    var colors = {};
    var numIds = d.keys.length;
    var colorArray;
    if (numIds < 10) {
        colorArray = d3.scale.category10().range();
        for (var idIndex = 0; idIndex < numIds; idIndex++) {
            colors[ d.keys[idIndex] ] = colorArray[idIndex] //colorArray[idIndex];
        }
    } else if (numIds < 20) {
        colorArray = d3.scale.category20().range();
        for (var idIndex = 0; idIndex < numIds; idIndex++) {
            colors[ d.keys[idIndex] ] = colorArray[idIndex] //colorArray[idIndex];
        }
    } else {
        colorArray = colorScale(numIds)
        for (var idIndex = 0; idIndex < numIds; idIndex++) {
            colors[ d.keys[idIndex] ] = colorArray[idIndex];
        }
    }

    var chart = c3.generate({
        bindto: '#time-series-area',
        data: {
            x: 'time',
            json: d["timeSeriesData"],
            type: 'area-spline',
            colors: colors
        },
        zoom: {
            enabled: true,
            rescale:true
        },
        axis: {
            x: {
                type: 'timeseries',
                tick: {
                    format: dateTimeFormat
                }
            },
            y: {
                tick: {
                    //format integers with comma-grouping for thousands
                    format: d3.format(",.1 ")
                }
            }
        },
        legend: {
            show: false
        }
    });
    //chart.transform('spline','data1')

    chart.hide();


    /** Timeseries eventlisteners **/

    $("#timeseries-time-series-legend").on("click", '.time-series-checkbox', function () {

        var checkbox = this;
        var checkboxObj = $(checkbox);
        var line = checkboxObj.val();
        if (checkboxObj.is(':checked')) {
            chart.show(line)

        } else {
            chart.hide(line)

        }
    });


    $("#main-view").on("click", ".time-series-select-all-checkbox", function () {

        //if select all is checked
        if ($(this).is(':checked')) {
            //trigger click on each unchecked checkbox
            $(".time-series-checkbox").each(function (index, checkbox) {
                if (!$(checkbox).is(':checked')) {
                    $(checkbox).click();
                }
            })
        } else {
            //trigger click on each checked checkbox
            $(".time-series-checkbox").each(function (index, checkbox) {
                if ($(checkbox).is(':checked')) {
                    $(checkbox).click();
                }
            })
        }
    });

    //Preselect first item
    var currentView = $("#" + tab + "-display-chart-section");
    var index = $($(".time-series-checkbox", currentView)[0]).val() == "time" ? 1 : 0;
    $($(".time-series-checkbox", currentView)[index]).click();
}