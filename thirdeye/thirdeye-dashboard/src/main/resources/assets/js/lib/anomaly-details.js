function showAnomalyDetails(id) {
    /* Handelbars template for table*/

    var mergeAnomalyData;
    var anomalyIndex;
    for (var i = 0, len = anomaliesDisplayData.length; i < len; i++) {
        if (anomaliesDisplayData[i]['id'] == id) {
            mergeAnomalyData = anomaliesDisplayData[i];
            anomalyIndex = i;
        }
    }

    // render page with incomplete data, another ajax call would populate the details
    renderAnomalyDetails(mergeAnomalyData);
    lineChartForSingleAnomaly(mergeAnomalyData);

    if (mergeAnomalyData.anomalyResults.length == 0) {
        $("#raw-anomalies").hide();
        getData("/dashboard/anomalies/view/" + id, "anomalies").done(function (anomalyData) {
            $("#raw-anomalies").show();
            // cache the result so that we can avoid same call next time.
            anomaliesDisplayData[anomalyIndex] = anomalyData;
            mergeAnomalyData = anomalyData;
            renderAnomalyDetails(mergeAnomalyData);
            lineChartForSingleAnomaly(mergeAnomalyData);
        });
    }
}

function renderAnomalyDetails(mergeAnomalyData) {
    // Format float variables
    mergeAnomalyData.score = getFormattedNumber(mergeAnomalyData.score, 4);
    mergeAnomalyData.weight = getFormattedNumber(mergeAnomalyData.weight, 4);
    /* Handelbars template for anomaly details */
    var result_anomaly_details_template = HandleBarsTemplates.template_anomaly_details(mergeAnomalyData);
    $("#anomaly-details-box").remove();
    $("#anomalies-section").append(result_anomaly_details_template);

    //Create dataTable instance out of raw anomalies table
    $("#raw-anomalies").DataTable();
    $("#anomalies-form-area").hide();
    $("#anomalies-chart-area").hide();

    //Event listeners on anomaly details
    $("#anomaly-details-box").on("click", ".to-anomalies-view", function () {
        $("#anomaly-details-box").hide();
        $("#anomalies-form-area").show();
        $("#anomalies-chart-area").show();
    });
}

function lineChartForSingleAnomaly(mergeAnomalyData) {
    var mergedAnomalyId = mergeAnomalyData.id;

    // Line chart settings
    var anomalyRegionData = [];
    anomalyRegionData.push({startTime: parseInt(mergeAnomalyData.startTime), endTime: parseInt(mergeAnomalyData.endTime), id: mergedAnomalyId, regionColor: "#eedddd"});
    var placeholder = "#anomaly-details-timeseries-placeholder";
    var lineChartSettings = {};
    lineChartSettings.data = {};
    lineChartSettings.data.subchart = {};
    lineChartSettings.data.subchart.show = true;
    var tab = hash.view;

    if (!timeSeriesDataForAllAnomalies[mergedAnomalyId]) {
        var aggTimeGranularity = calcAggregateGranularity(mergeAnomalyData.startTime, mergeAnomalyData.endTime);

        var viewWindowStart = mergeAnomalyData.startTime;
        var viewWindowEnd = mergeAnomalyData.endTime;

        var extensionWindowMillis;
        switch (aggTimeGranularity) {
            case "DAYS":
                extensionWindowMillis = 86400000;
                break;
            case "HOURS":
            default:
                var viewWindowSize = viewWindowEnd - viewWindowStart;
                var multiplier = Math.max(2, parseInt(viewWindowSize / (2 * 3600000)));
                extensionWindowMillis = 3600000 * multiplier;
        }
        viewWindowStart -= extensionWindowMillis;
        viewWindowEnd += extensionWindowMillis;

        var timeSeriesUrl = "/dashboard/anomaly-merged-result/timeseries/" + mergeAnomalyData.id
            + "?aggTimeGranularity=" + aggTimeGranularity + "&start=" + viewWindowStart + "&end=" + viewWindowEnd;

        getDataCustomCallback(timeSeriesUrl, tab).done(function (timeSeriesData) {
            //Error handling when data is falsy (empty, undefined or null)
            if (!timeSeriesData) {
                // do nothing
                return
            } else {
                $("#" + tab + "-chart-area-error").hide();
            }
            drawAnomalyTimeSeries(timeSeriesData, anomalyRegionData, tab, placeholder, lineChartSettings);
            timeSeriesDataForAllAnomalies[mergedAnomalyId] = timeSeriesData;
        });
    }  else {
        drawAnomalyTimeSeries(timeSeriesDataForAllAnomalies[mergedAnomalyId], anomalyRegionData, tab, placeholder, lineChartSettings);
    }
}
