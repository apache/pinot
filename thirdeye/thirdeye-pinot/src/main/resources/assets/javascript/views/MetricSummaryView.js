function MetricSummaryView(metricSummaryModel) {
  var template = $("#metric-summary-template").html();
  this.template_compiled = Handlebars.compile(template);
  this.placeHolderId = "#metric-summary-place-holder";
  this.metricSummaryModel = metricSummaryModel;
}

MetricSummaryView.prototype = {

  render : function() {
    console.log("MetricSummaryView.render")
    var result = this.template_compiled(this.metricSummaryModel);
    $(this.placeHolderId).html(result);
    $('div[data-toggle="tooltip"]').tooltip()
  }
};
