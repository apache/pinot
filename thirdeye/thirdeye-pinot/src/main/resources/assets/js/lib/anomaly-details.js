function showAnomalyDetails(id) {
    /* Handelbars template for table*/

    var mergeAnomalyData;
    for (var i = 0, len = anomaliesDisplayData.length; i < len; i++) {

        if (anomaliesDisplayData[i]['id'] == id) {
            mergeAnomalyData = anomaliesDisplayData[i];
        }
    }

    /* Handelbars template for anomaly details */
    var result_anomaly_details_template = HandleBarsTemplates.template_anomaly_details(mergeAnomalyData);
    $("#anomaly-details-box").remove();
    $("#anomalies-section").append(result_anomaly_details_template);
    $("#raw-anomalies").DataTable();

    $("#anomalies-form-area").hide();
    $("#anomalies-chart-area").hide();

    //Create dataTable instance out of raw anomalies table


    lineChartForSingleAnomaly(mergeAnomalyData);

    //Event listeners on anomaly details
    $("#anomaly-details-box").on("click", ".to-anomalies-view", function () {
        $("#anomaly-details-box").hide();
        $("#anomalies-form-area").show();
        $("#anomalies-chart-area").show();
    });
}

function lineChartForSingleAnomaly(mergeAnomalyData) {
    var dataset = hash.dataset;
    var mergedAnomalyId = mergeAnomalyData.id;

    // TODO: remove console logs
    console.log(window.datasetConfig);
    console.log(mergeAnomalyData);

    var aggTimeGranularity = calcAggregateGranularity(mergeAnomalyData.startTime, mergeAnomalyData.endTime);
    console.log(aggTimeGranularity);

    var extensionWindowMillis = (datasetConfig.dataGranularity == "DAYS") ? 86400000 : 3600000;
    var fnProperties = parseProperties(mergeAnomalyData.function.properties);

    var compare = 1;
    var compareMode = "WOW";
    //for
    switch(fnProperties.baseline){
        case "w/w":
            compare = 1;
            compareMode = "WOW";
            break;
        case "w/2w":
            compare = 2;
            compareMode = "WO2W";
            break;
        case "w/3w":
            compare= 3;
            compareMode= "WO3W";
            break;
        default:
            compare = 1;
            compareMode = "WOW";
    }

    var currentStart = mergeAnomalyData.startTime - extensionWindowMillis;
    var currentEnd = mergeAnomalyData.endTime + extensionWindowMillis;

    var baselineStart = moment(parseInt(currentStart)).add((-1* compare), 'weeks');
    var baselineEnd = moment(parseInt(currentEnd)).add( (-1* compare), 'weeks');

    var exploreDimension = mergeAnomalyData.function.exploreDimensions;
    var effectedValue = mergeAnomalyData.dimensions;
    var fnFiltersString = mergeAnomalyData.function.filters;

    var metrics = hash.metrics;
    var filters = createAnomalyFilters(effectedValue,exploreDimension,fnFiltersString);
    filters = encodeURIComponent(JSON.stringify(filters));

    var timeSeriesUrl = "/dashboard/data/tabular?dataset=" + dataset + "&compareMode=" + compareMode //
        + "&currentStart=" + currentStart + "&currentEnd=" + currentEnd  //
        + "&baselineStart=" + baselineStart + "&baselineEnd=" + baselineEnd   //
        + "&aggTimeGranularity=" + aggTimeGranularity + "&metrics=" + metrics+ "&filters=" + filters;
    var tab = hash.view;

    getDataCustomCallback(timeSeriesUrl,tab ).done(function (timeSeriesData) {
        //Error handling when data is falsy (empty, undefined or null)
        if (!timeSeriesData) {
            // do nothing
            return
        } else {
            $("#" + tab + "-chart-area-error").hide();
        }
        var anomalyRegionData = [];
        anomalyRegionData.push({startTime: parseInt( mergeAnomalyData.startTime), endTime: parseInt(mergeAnomalyData.endTime), id: mergedAnomalyId, regionColor: "#eedddd"});
        var placeholder = "#anomaly-details-timeseries-placeholder";
        var linechartSettings = {};
        linechartSettings.data = {};
        linechartSettings.data.subchart = {};
        linechartSettings.data.subchart.show = true;

        drawAnomalyTimeSeries(timeSeriesData, anomalyRegionData, tab, placeholder,linechartSettings );
    });
}
