function getTabular() {

	var url = "/dashboard/data/tabular?" + window.location.hash.substring(1);
	getData(url).done(function(data) {
		renderTabular(data);
	});
};

function renderTabular(data) {

	/* Handelbars template for time series legend */
	var result_metric_time_series_section =
	 HandleBarsTemplates.template_metric_time_series_section(data);
	 $("#"+ hash.view +"-display-chart-section").append(result_metric_time_series_section);

    drawTimeSeries(data)

    /* Handelbars template for funnel table */
    var result_funnels_template = HandleBarsTemplates
        .template_funnels_table(data);
    $("#"+ hash.view +"-display-chart-section").append(result_funnels_template);
    calcHeatMapBG("tabular");
    formatMillisToTZ();
}
var lineChart;
function drawTimeSeries(ajaxData) {


    var currentView = $("#" + hash.view + "-display-chart-section")
    var lineChartPlaceholder = $("#linechart-placeholder", currentView)[0];


	// Metric(s)
	var metrics = ajaxData["metrics"]
	var lineChartData = {};
	var barChartData = {};
	var dateTimeformat = (hash.hasOwnProperty("aggTimeGranularity") && hash.aggTimeGranularity
			.toLowerCase().indexOf("days") > -1) ? "MM-DD" : "h a";
	var xTickFormat = dateTimeformat;
	var xTicksBaseline = [];
	var xTicksCurrent = [];
	var colors = {};
	var chartTypes = {};
	var axes = {};
	for (var t = 0, len = ajaxData["timeBuckets"].length; t < len; t++) {
		var timeBucket = ajaxData["timeBuckets"][t]["currentStart"]
		xTicksBaseline.push(timeBucket)
		xTicksCurrent.push(timeBucket)
	}
	lineChartData["time"] = xTicksCurrent;
	barChartData["time"] = xTicksCurrent;
    var colorArray;
    if (metrics.length < 10) {
        colorArray = d3.scale.category10().range();
    }else if(metrics.length < 20) {
         colorArray = d3.scale.category20().range();
    }else {
      colorArray = [];
      for(i=0,len=metrics.length; i<len;i++){
        colorArray.push( colorByName(metrics[i]) );
       }
    }

	for (var i = 0, mlen = metrics.length; i < mlen; i++) {
		var metricBaselineData = [];
		var metricCurrentData = [];
		var deltaPercentageData = [];
		for (var t = 0, len = ajaxData["timeBuckets"].length; t < len; t++) {
			var baselineValue = ajaxData["data"][metrics[i]]["responseData"][t][0];
			var currentValue = ajaxData["data"][metrics[i]]["responseData"][t][1];
			var deltaPercentage = parseInt(ajaxData["data"][metrics[i]]["responseData"][t][2] * 100);
			metricBaselineData.push(baselineValue);
			metricCurrentData.push(currentValue);
			deltaPercentageData.push(deltaPercentage);
		}
		lineChartData[metrics[i] + "-baseline"] = metricBaselineData;
		lineChartData[metrics[i] + "-current"] = metricCurrentData;
		barChartData[metrics[i] + "-delta"] = deltaPercentageData;
		colors[metrics[i] + "-baseline"] = colorArray[i];
		colors[metrics[i] + "-current"] = colorArray[i];
		colors[metrics[i] + "-delta"] = colorArray[i];
		// chartTypes[metrics[i] + "-current"] = 'spline';
		// chartTypes[metrics[i] + "-baseline"] = 'spline';
		// chartTypes[metrics[i] + "-delta"] = 'bar';
		// axes[metrics[i] + "-current"] = 'y';
		// axes[metrics[i] + "-baseline"] = 'y';
		// axes[metrics[i] + "-delta"] = 'y2';
	}

	lineChart = c3.generate({
		bindto : lineChartPlaceholder,
		padding : {
			top : 0,
			right : 100,
			bottom : 0,
			left : 100
		},
		data : {
			x : 'time',
			json : lineChartData,
			type : 'spline',
			colors : colors
		},
		axis : {
			x : {
				type : 'timeseries'
			}
		},
		legend : {
			show : false
		},
		grid : {
			x : {
				show : false
			},
			y : {
				show : false
			}
		}
	});
	var barChart = c3.generate({

		bindto : '#barchart-placeholder',
		padding : {
			top : 0,
			right : 100,
			bottom : 0,
			left : 100
		},
		data : {
			x : 'time',
			json : barChartData,
			type : 'spline',
			colors : colors
		},
		axis : {
			x : {
				label : {
					text : "Time"
				},
				type : 'timeseries'
			},
			y : {
				label : {
					text : "% change",
					position: 'outer-middle'
				}
			}
		},
		legend : {
			show : false
		},
		grid : {
			x : {
				
			},
			y : {
				lines : [ {
					value : 0
				} ]
			}
		},
		bar : {
			width : {
				ratio : .5
			}
		}
	});

	ids = [];
	for (var i = 0; i < metrics.length; i++) {
		ids.push(i);
	}


	lineChart.hide();
	barChart.hide();

    //EventListeners
    var currentView = $("#" + hash.view + "-display-chart-section")

	// Clicking the checkbox of the timeseries legend will redraw the timeseries
	// with the selected elements
	$("#metric-time-series-legend", currentView).on("click",'.time-series-metric-checkbox', function() {
       var checkbox = this;
       var checkboxObj = $(checkbox);
            metricName = checkboxObj.val();
            if (checkboxObj.is(':checked')) {

                lineChart.show(metricName + "-current");
                lineChart.show(metricName + "-baseline");
                barChart.show(metricName + "-delta");
            } else {
                lineChart.hide(metricName + "-current");
                lineChart.hide(metricName + "-baseline");
                barChart.hide(metricName + "-delta");
            }
    });

    //Select all / deselect all metrics option
    currentView.on("click",".time-series-metric-select-all-checkbox", function(){

        //if select all is checked
        if($(this).is(':checked')){
            //trigger click on each unchecked checkbox
            $(".time-series-metric-checkbox", currentView).each(function(index, checkbox) {
                if (!$(checkbox).is(':checked')) {
                    $(checkbox).click();
                }
            })
        }else{
            //trigger click on each checked checkbox
            $(".time-series-metric-checkbox", currentView).each(function(index, checkbox) {
                if ($(checkbox).is(':checked')) {
                    $(checkbox).click();
                }
            })
        }
    });

    //Preselect first metric
	$($(".time-series-metric-checkbox", currentView)[0]).click();

}
