$(document).ready(function () {
  $("#anomalies2").append("<h2>Analyze anomalies</h2>" + "<div id='merge-strategy'>"
      + "<table class='anomaly2'><tr><td>Anomaly Group </td><td><select id='anomaly-group-select' onchange='renderAnomalyGroups()'>"
      + "<option value='FUNCTION'>Function</option>"
      + "<option value='COLLECTION_METRIC' selected='selected'>Metric</option>"
      + "<option value='COLLECTION'>Collection</option></select></td></tr>"
      + "<tr><td>Select Time Range </td><td><select id='time-range' onchange='renderAnomalyGroups()'>"
      + "<option value='6' >Last 6 hours</option>"
      + "<option value='24' >Last 1 day</option>"
      + "<option value='168' selected='selected'>Last 1 week</option>"
      + "<option value='336'>Last 2 weeks</option>"
      + "<option value='720'>Last 1 Month</option>"
      + "<option value='0' >All Time</option>" + "</select></td></tr>"
      + "<tr><td>Merge Length </td><td><select id='sequential-merge-gap' onchange='renderAnomalyGroups()'>"
      + "<option value='7200000' >2 hrs</option>" + "<option value='21600000'>6 hrs</option>"
      + "<option value='43200000' selected='selected'>12 hrs</option>"
      + "<option value='86400000' >24 hrs</option></select></td>" + "</tr>" + "</table>" + "</div>"
      + "<div id='mergeConfig'></div>" + "<div id='group-by'></div>"
      + "<div id='anomaly-merged-summary'></div>");
  renderAnomalyGroups(this);
  routeToTab();
});

function getMergeConfig() {
  var anomalyGroup = $("#anomaly-group-select").children(':selected').attr("value");
  var sequentialMergeGap = $("#sequential-merge-gap").children(':selected').attr("value");
  var startTimeVal = $("#time-range").children(':selected').attr("value");
  var startTime = 0;
  if (startTimeVal != 0) {
    startTime = Date.now() - startTimeVal * 60 * 60 * 1000;
  }
  var mergeConfig = '{"mergeStrategy":"' + anomalyGroup
      + '","sequentialAllowedGap":30000,"mergeDuration":' + sequentialMergeGap + ',"startTime":'
      + startTime + ',"endTime":' + Date.now() + '}';
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
        if (anomalyGroup === 'FUNCTION') {
          result_anomaly_grouping_template = HandleBarsTemplates.template_anomaly_grouping_by_fun(data);
        } else if (anomalyGroup === 'COLLECTION_METRIC') {
          result_anomaly_grouping_template = HandleBarsTemplates.template_anomaly_grouping_by_metric(data);
        } else {
          result_anomaly_grouping_template = HandleBarsTemplates.template_anomaly_grouping_by_collection(data);
        }
        $("#group-by").html(result_anomaly_grouping_template);
      });
}

function renderAnomalySummaryByFunction(functionId) {
  submitData("/thirdeye/anomaly/summary/function/" + functionId, getMergeConfig(), "Anomaly2").done(
      function (data) {
        var result_anomaly_summary_template = HandleBarsTemplates.template_anomaly_summary(data);
        $("#anomaly-merged-summary").html(result_anomaly_summary_template);
      });
}

function renderAnomalySummaryByCollection(collection) {
  submitData("/thirdeye/anomaly/summary/" + collection, getMergeConfig() + "/", "Anomaly2").done(
      function (data) {
        var result_anomaly_summary_template = HandleBarsTemplates.template_anomaly_summary(data);
        $("#anomaly-merged-summary").html(result_anomaly_summary_template);
      });
}

function renderAnomalySummaryByMetric(collection, metric) {
  submitData("/thirdeye/anomaly/summary/" + collection + "?metric=" + metric, getMergeConfig(),
      "Anomaly2").done(function (data) {
    var result_anomaly_summary_template = HandleBarsTemplates.template_anomaly_summary(data);
    $("#anomaly-merged-summary").html(result_anomaly_summary_template);
  });
}
