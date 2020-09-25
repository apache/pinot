function AnomalySummaryView(anomalySummaryModel) {
  var template = $("#anomaly-summary-template").html();
  this.template_compiled = Handlebars.compile(template);
  this.placeHolderId = "#anomaly-summary-place-holder";
  this.anomalySummaryModel = anomalySummaryModel;

  this.anomalySummaryModel.renderViewEvent.attach(this.renderViewEventHandler.bind(this));
}

AnomalySummaryView.prototype = {

  renderViewEventHandler : function() {
    this.render();
  },
  render : function() {
    var result = this.template_compiled(this.anomalySummaryModel);
    $(this.placeHolderId).html(result);
  }
};
