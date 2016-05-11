function getTimeSeries() {

	var url = "/dashboard/data/timeseries?" + window.location.hash.substring(1);

//	c3.generate({
//		bindto: '#display-chart-section',
//        data: {
//            url: "/dashboard/data/timeseries?" + window.location.hash.substring(1),
//            mimeType: 'json'
//        }
//    })
	getData(url).done(function(data) {

		var result_time_series = HandleBarsTemplates.template_time_series(data);
        $("#"+ hash.view +"-display-chart-section").append(result_time_series);
		renderTimeSeriesUsingC3(data)
	});
};

function renderTimeSeriesUsingC3(d){  //time-series-area
	//var data = JSON.parse(d);

    var colors = {};
    var IdArray = Object.keys(d.timeSeriesData)
    var numIds = IdArray.length;
    //Only color by name works since it's a hash.map
    // Todo: we can change the data type in be so the d3.scale.category10().range() is working
    for(key in d.timeSeriesData){
        colors[key] = colorByName(key);
    }

    var chart = c3.generate({
	    bindto: '#time-series-area',
	    data: {
	      x : 'time',	
	      json : d["timeSeriesData"],
	      type: 'area-spline',
          colors : colors
	    },
	    axis : {
	    	x: {
	    		type: 'timeseries'
	    	}
	    },
        legend : {
            show : false
        }
	});
	//chart.transform('spline','data1')

    chart.hide();
    $("#timeseries-time-series-legend").on("click",'.time-series-checkbox', function() {

        var checkbox = this;
        var checkboxObj = $(checkbox);
        var line = checkboxObj.val();
        if (checkboxObj.is(':checked')) {
            chart.show(line)

        } else {
            chart.hide(line)

        }
    });


    $("#main-view").on("click",".time-series-select-all-checkbox", function(){

        //if select all is checked
        if($(this).is(':checked')){
            //trigger click on each unchecked checkbox
            $(".time-series-checkbox").each(function(index, checkbox) {
                if (!$(checkbox).is(':checked')) {
                    $(checkbox).click();
                }
            })
        }else{
            //trigger click on each checked checkbox
            $(".time-series-checkbox").each(function(index, checkbox) {
                if ($(checkbox).is(':checked')) {
                    $(checkbox).click();
                }
            })
        }
    });

    //Preselect first item
    var currentView = $("#" + hash.view + "-display-chart-section");
    var index = $($(".time-series-checkbox",currentView)[0]).val() == "time" ? 1 : 0;
    $($(".time-series-checkbox",currentView)[index]).click();
}