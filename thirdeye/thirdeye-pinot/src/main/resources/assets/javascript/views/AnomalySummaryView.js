function AnomalySummaryView(anomalySummaryModel) {
  var template = $("#anomaly-summary-template").html();
  this.template_compiled = Handlebars.compile(template);
  this.placeHolderId = "#anomaly-summary-place-holder";
}

AnomalySummaryView.prototype = {

  render : function() {
    var result = this.template_compiled(this.anomalySummaryModel);
    $(this.placeHolderId).html(result);
  }
}