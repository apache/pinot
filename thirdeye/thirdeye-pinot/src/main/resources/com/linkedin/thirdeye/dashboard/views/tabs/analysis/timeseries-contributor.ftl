<div class="row bottom-buffer">
  <div class="col-xs-12">
      <label class="label-medium-semibold">Metric: </label>
      <div><label>{{this.metricName}}<label></div>
  </div>
</div>
<!-- Chart section -->
<div class="analysis-card analysis-chart bottom-buffer">
    <div class="analysis-chart__graph" id="analysis-chart" style=""></div>
    <div class="analysis-chart__legend padding-all" id="analysis-chart-legend"></div>
</div>

<!-- Percentage change table -->
<div class="analysis-title">Change Over Time</div>
<div class="row">
  <div class="col-xs-6">
    <span class="label-small-semibold">Click on a cell to drill down into its contribution breakdown.</span>
  </div>
  <div class="col-xs-3 pull-right">
    <input type="checkbox" id="show-details" {{#if this.showDetailsChecked}}checked{{/if}}>
    <label for="show-details" class="metric-label">See Contribution Details</label>
  </div>
  <div class="col-xs-3 pull-right">
    <input type="checkbox" id="show-cumulative" {{#if this.showCumulativeChecked}}checked{{/if}}>
    <label for="show-cumulative" class="metric-label">Show Cumulative</label>
  </div>
</div>

<div class="row">
  <div id="contributor-table-placeholder"></div>
</div>
