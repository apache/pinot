function MetricSummaryView(metricSummaryModel) {
  var template = $("#metric-summary-template").html();
  this.template_compiled = Handlebars.compile(template);
  this.placeHolderId = "#metric-summary-place-holder";
  this.metricSummaryModel = metricSummaryModel;

  this.metricSummaryModel.renderViewEvent.attach(this.renderViewEventHandler.bind(this));
}

MetricSummaryView.prototype = {

  renderViewEventHandler : function() {
    this.render();
  },
  render : function() {
    console.log("MetricSummaryView.render")
    console.log(this.metricSummaryModel.metricSummaryList);
    var result = this.template_compiled(this.metricSummaryModel.metricSummaryList);
    $(this.placeHolderId).html(result);
    
    $('div[data-toggle="tooltip"]').tooltip()
  }
};
