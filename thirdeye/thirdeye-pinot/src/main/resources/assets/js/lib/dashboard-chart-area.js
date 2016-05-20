//In this js file we keep chart related event listeners that are present in  multiple views

//Cumulative checkbox
$("#main-view").on("click", ".cumulative", function() {

	$(".cumulative").toggleClass("uk-active");
	$(".discrete-values").toggleClass("hidden");
	$(".cumulative-values").toggleClass("hidden");


	// Todo: feature:redraw metric timeseries using cumulative data
	if ($("#metric-time-series-placeholder").length > 0) {
	}
});

//On the first click on cumulative btn trigger the cumulative column total calculation
$("#main-view").one("click", ".cumulative", function() {
    $(".contributors-table .select_all_checkbox[rel='cumulative']").each(function(){
        $(this).trigger("click");
    });

    // Trigger contributors total calculation

//    var tableBodies = $(".cumulative").hasClass("uk-active")  ? $(".contributors-table.cumulative-values tbody") : $(".contributors-table.discrete-values tbody");
//    for (var i = 0, len = tableBodies.length; i < len; i++) {
//        sumColumn($("input[type = 'checkbox']", tableBodies[i])[0]);
//    }
});

// Summary and details tab toggle
$("#main-view").on("click","#sum-detail button",function() {

					if (!$(this).hasClass("uk-active")) {

						$(this).addClass("uk-active");
						$(this).siblings().removeClass("uk-active");

						var timeCells = document
								.getElementsByClassName("table-time-cell");
						var nextState = timeCells[0].getAttribute('colspan') == 1 ? 3
								: 1;
						for (var index = 0, len = timeCells.length; index < len; index++) {
							timeCells[index].setAttribute('colspan', nextState)
						}

						var detailsCells = document
								.getElementsByClassName("details-cell");
						//Alert
						if (detailsCells.length > 1000) {
							console.log('details cells to paint:')
							console.log(detailsCells.length)
						}

						for (var dIndex = 0, detailsLen = detailsCells.length; dIndex < detailsLen; dIndex++) {
							detailsCells[dIndex].classList.toggle("hidden");
						}

						var subheaderCells = document
								.getElementsByClassName("subheader");
						for (var sIndex = 0, subheaderLen = subheaderCells.length; sIndex < subheaderLen; sIndex++) {
							subheaderCells[sIndex].classList.toggle("hidden");
						}

					}
				});

/** Dashboard and tabular related listeners * */

// Click metric name in the table
$("#main-view").on("click", ".metric-label", function() {

	// Change the view to contributors

    //either dashboard or metrics param is present in hash
    delete hash.dashboard;

    //switch to time ver time tab
    hash.view = "compare";

    //set start and end date to the starte and end date of the table
    var timeBuckets = $("#timebuckets>span")
    var numTimeBuckets = timeBuckets.length;

    var firstTimeBucketInRow = $("#timebuckets>span")[0];
    var lastTimeBucketInRow = $("#timebuckets>span")[numTimeBuckets - 1];

    var currentStartUTC = $($("span", firstTimeBucketInRow)[0]).text().trim();
    var baselineStartUTC = $($("span", firstTimeBucketInRow)[2]).text().trim();

    var currentEndUTC = $($("span", lastTimeBucketInRow)[1]).text().trim();
    var baselineEndUTC = $($("span", lastTimeBucketInRow)[3]).text().trim();

    hash.baselineStart = baselineStartUTC;
    hash.baselineEnd = baselineEndUTC;
    hash.currentStart = currentStartUTC;
    hash.currentEnd = currentEndUTC;


    //check the current granularity of the data on the table
    var endOfFirstTimeBucket =  $($("span", firstTimeBucketInRow)[1]).text().trim();
    var diff = parseInt(endOfFirstTimeBucket) - parseInt(currentStartUTC)
    var diffProperties = describeMillis(diff)
    var aggTimeGranularity = diffProperties ? diffProperties.unit : "HOURS"

    hash.aggTimeGranularity = aggTimeGranularity

    //set the metrics
    metrics = [];
    // Todo: if metric label it's a derived metric so title contains
    metrics.push($(this).attr("title"))
    hash.metrics = metrics.toString();

    //select only the first dimension to retrieve less data
    hash.dimensions = $($(".dimension-option")[0]).attr("value");

    //update hash will trigger window.onhashchange event:
    // update the form area and trigger the ajax call
    window.location.hash = encodeHashParameters(hash);

});

// Clicking heat-map-cell should choose the related metrics and the derived
// metric the current time to the related hour then loading heatmap view
$("#main-view").on("click", "#funnels-table .heat-map-cell", function() {

	hash.view = "compare"
	hash.aggTimeGranularity = "aggregateAll"
	cellObj = $(this)
	var timeIndex = cellObj.attr("timeIndex");
	var timeBucketForColumnIndex = $("#timebuckets>span")[timeIndex];

    var currentStartUTC;
    var baselineStartUTC;
    var currentEndUTC;
    var baselineEndUTC;

    var currentSection = $(this).closest(".display-chart-section")
    if( $(".cumulative", currentSection).is(':checked') ){

        var firstTimeBucketInRow = $("#timebuckets>span")[0]
        currentStartUTC = $($("span", firstTimeBucketInRow)[0]).text().trim();
        baselineStartUTC = $($("span", firstTimeBucketInRow)[2]).text().trim();

    }else{
       currentStartUTC = $($("span", timeBucketForColumnIndex)[0]).text().trim();
       baselineStartUTC = $($("span", timeBucketForColumnIndex)[2]).text().trim();
    }

    currentEndUTC = $($("span", timeBucketForColumnIndex)[1]).text().trim();
	baselineEndUTC = $($("span", timeBucketForColumnIndex)[3]).text().trim();

	hash.baselineStart = baselineStartUTC;
	hash.baselineEnd = baselineEndUTC;
	hash.currentStart = currentStartUTC;
	hash.currentEnd = currentEndUTC;
	delete hash.dashboard;
	metrics = [];
	var metricName = cellObj.attr("metricIndex")
	metrics.push(metricName)
	hash.metrics = metrics.toString();

    //update hash will trigger window.onhashchange event:
    // update the form area and trigger the ajax call
    window.location.hash = encodeHashParameters(hash);

})

$("#main-view").on("click", ".close-btn", function() {
    $(this).hide();
})

$("#main-view").on("click", ".close-parent", function() {
    $(this).parent().hide();
})

