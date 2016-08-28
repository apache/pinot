function getMergeConfig() {
  var anomalyGroup = $("#anomaly-group-select").children(':selected').attr("value");
  var mergeDuration = $("#merge-length").children(':selected').attr("value");
  var sequentialMergeGap = $("#sequential-merge-gap").children(':selected').attr("value");
  var startTimeVal = $("#time-range").children(':selected').attr("value");
  var startTime = 0;
  if (startTimeVal != 0) {
    startTime = Date.now() - startTimeVal * 60 * 60 * 1000;
  }
  var mergeConfig = '{"mergeStrategy":"' + anomalyGroup + '","sequentialAllowedGap":'
      + sequentialMergeGap + ',"mergeDuration":' + mergeDuration + ',"startTime":' + startTime
      + ',"endTime":' + Date.now() + '}';
  return mergeConfig;
}

function renderAnomalyGroups() {
  $("#anomaly-merged-summary").html("");
  var anomalyGroup = $("#anomaly-group-select").children(':selected').attr("value");
  renderGroupBy(anomalyGroup);
}

function renderGroupBy(anomalyGroup) {
  submitData("/thirdeye/anomaly/summary/groupBy", getMergeConfig(), "anomalies2").done(
      function (data) {
        var result_anomaly_grouping_template = "";
        if (anomalyGroup === 'FUNCTION_DIMENSIONS') {
          result_anomaly_grouping_template = HandleBarsTemplates.template_anomaly_grouping_by_fun_dim(data);
        } else {
          result_anomaly_grouping_template = HandleBarsTemplates.template_anomaly_grouping_by_fun(data);
        }
        $("#group-by").html(result_anomaly_grouping_template);
      });
}

function renderAnomalySummaryByDimensions(functionId, dimensions) {
  submitData("/thirdeye/anomaly/summary/function/" + functionId + "?dimensions=" + dimensions,
      getMergeConfig(), "Anomaly2").done(
      function (data) {
        var result_anomaly_summary_template = HandleBarsTemplates.template_anomaly_summary(data);
        $("#anomaly-merged-summary").html(result_anomaly_summary_template);
      });
}
function renderAnomalySummaryByFunction(functionId) {
  submitData("/thirdeye/anomaly/summary/function/" + functionId, getMergeConfig(), "Anomaly2").done(
      function (data) {
        var result_anomaly_summary_template = HandleBarsTemplates.template_anomaly_summary(data);
        $("#anomaly-merged-summary").html(result_anomaly_summary_template);
      });
}
