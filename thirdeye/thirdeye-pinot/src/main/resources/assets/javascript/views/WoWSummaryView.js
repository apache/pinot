function WoWSummaryView(wowSummaryModel) {
  var template = $("#wow-summary-template").html();
  this.template_compiled = Handlebars.compile(template);
  this.placeHolderId = "#wow-place-holder";
  this.wowSummaryModel = wowSummaryModel;
}

WoWSummaryView.prototype = {

  render : function() {
    var result = this.template_compiled(this.wowSummaryModel);
    $(this.placeHolderId).html(result);
  }
}
