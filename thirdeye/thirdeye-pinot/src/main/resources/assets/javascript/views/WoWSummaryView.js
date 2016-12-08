function WoWSummaryView(woWSummaryModel) {
  var template = $("#wow-summary-template").html();
  this.template_compiled = Handlebars.compile(template);
  this.placeHolderId = "#wow-place-holder";
  this.woWSummaryModel = woWSummaryModel;
}

WoWSummaryView.prototype = {

  render : function() {
    var result = this.template_compiled(this.woWSummaryModel);
    $(this.placeHolderId).html(result);
  }
}
