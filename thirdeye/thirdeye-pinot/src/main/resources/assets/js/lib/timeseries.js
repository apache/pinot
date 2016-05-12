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
        //Error handling when data is falsy (empty, undefined or null)
        if(!data){
            $("#"+  hash.view  +"-chart-area-error").empty()
            var warning = $('<div></div>', { class: 'uk-alert uk-alert-warning' })
            warning.append($('<p></p>', { html: 'Something went wrong. Please try and reload the page. Error: data =' + data  }))
            $("#"+  hash.view  +"-chart-area-error").append(warning)
            $("#"+  hash.view  +"-chart-area-error").show()
            return
        }else{
            $("#"+  hash.view  +"-chart-area-error").hide()
        }

		var result_time_series = HandleBarsTemplates.template_time_series(data);
        $("#"+ hash.view +"-display-chart-section").append(result_time_series);
		renderTimeSeriesUsingC3(data)
	});
};

function renderTimeSeriesUsingC3(d){  //time-series-area

    var colors = {};
    var numIds = d.keys.length;
    var colorArray;
    if (numIds < 10) {
        colorArray = d3.scale.category10().range();
        for(var idIndex = 0; idIndex < numIds; idIndex++){
            colors[ d.keys[idIndex] ] = colorArray[idIndex] //colorArray[idIndex];
        }
    }else if(numIds < 20) {
        colorArray = d3.scale.category20().range();
        for(var idIndex = 0; idIndex < numIds; idIndex++){
            colors[ d.keys[idIndex] ] = colorArray[idIndex] //colorArray[idIndex];
        }
    }else {
        colorArray = colorScale(numIds)
        for(var idIndex = 0; idIndex < numIds; idIndex++){
            colors[ d.keys[idIndex] ] = colorArray[idIndex];
        }
    }


    console.log("colors")
    console.log(colors)
    console.log('d["timeSeriesData"]')
    console.log(d["timeSeriesData"])


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