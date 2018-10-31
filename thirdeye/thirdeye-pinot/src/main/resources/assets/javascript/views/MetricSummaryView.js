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
  // FIXME: the headings of the metric tiles look absurd if name is too long (spills out of the header area)
  render : function() {
    var result = this.template_compiled(this.metricSummaryModel.metricSummaryList);
    $(this.placeHolderId).html(result);

    $('div[data-toggle="tooltip"]').tooltip()
  }
};
