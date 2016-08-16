function renderMergedAnomalies() {
  var startTimeVal = $("#time-range").children(':selected').attr("value");
  var startTime = 0;
  var endTime = Date.now();
  if (startTimeVal != 0) {
    startTime = endTime - startTimeVal * 60 * 60 * 1000;
  }

  getData("/thirdeye/anomaly/merged?startTime="+startTime+"&endTime="+endTime, "anomalies3").done(function (data) {
    var result_anomaly_summary_template = HandleBarsTemplates.template_anomaly_summary(data);
    $("#anomaly-merged-summary").html(result_anomaly_summary_template);
  });
}
