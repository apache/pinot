function InvestigateView(investigateModel) {
  // Compile template
  const investigate = $("#investigate-template").html();
  this.investigate_template_compiled = Handlebars.compile(investigate);
  this.investigateModel = investigateModel;
}

InvestigateView.prototype = {
  init(params = {}) {
    const { metricId } = params;
    this.metricId = metricId;
  },

  render: function () {
    $("#investigate-place-holder").html(this.investigate_template_compiled);
  },
};
