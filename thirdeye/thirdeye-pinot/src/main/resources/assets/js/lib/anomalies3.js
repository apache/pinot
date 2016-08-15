function renderMergedAnomalies() {
  getData("/thirdeye/anomaly/merged", "anomalies3").done(function (data) {
    var result_anomaly_summary_template = HandleBarsTemplates.template_anomaly_summary(data);
    $("#anomaly-merged-summary").html(result_anomaly_summary_template);
  });
}
