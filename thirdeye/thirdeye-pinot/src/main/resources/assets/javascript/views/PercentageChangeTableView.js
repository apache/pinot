function PercentageChangeTableView(contributorTableModel) {
  var percentage_change_table_template = $("#percentage-change-table-template").html();
  this.percentage_change_table_template_compiled = Handlebars.compile(percentage_change_table_template);
  this.percentage_change_table_placeHolderId = "#percentage-change-table-placeholder";

  var wow_metric_table_template = $("#wow-metric-table-template").html();
  this.wow_metric_table_template_compiled = Handlebars.compile(wow_metric_table_template);
  this.wow_metric_table_placeHolderId = "#wow-metric-table-placeholder";

  var wow_metric_dimension_table_template = $("#wow-metric-dimension-table-template").html();
  this.wow_metric_dimension_table_template_compiled = Handlebars.compile(wow_metric_dimension_table_template);
  this.wow_metric_dimension_table_placeHolderId = "#wow-metric-dimension-table-placeholder";

  this.contributorTableModel = contributorTableModel;
}

PercentageChangeTableView.prototype = {

  render : function() {
    var percentageChangeTableResult = this.percentage_change_table_template_compiled();
    $(this.percentage_change_table_placeHolderId).html(percentageChangeTableResult);

    var wowMetricTableResult = this.wow_metric_table_template_compiled(
        this.contributorTableModel.wowMetricTable);
    $(this.wow_metric_table_placeHolderId).html(wowMetricTableResult);

    var wowMetricDimensionTableResult = this.wow_metric_dimension_table_template_compiled(
        this.contributorTableModel.wowMetricDimensionTable);
    $(this.wow_metric_dimension_table_placeHolderId).html(wowMetricDimensionTableResult);
  }
}
