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
    var mergedAnomalyId = mergeAnomalyData.id;

    // TODO: remove console logs
    console.log(window.datasetConfig);
    console.log(mergeAnomalyData);

    var timeSeriesUrl = "/dashboard/anomaly-merged-result/timeseries/" + mergeAnomalyData.id;
    var tab = hash.view;

    getDataCustomCallback(timeSeriesUrl, tab).done(function (timeSeriesData) {
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

        drawAnomalyTimeSeries(timeSeriesData, anomalyRegionData, tab, placeholder, linechartSettings);
    });
}
