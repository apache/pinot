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
<div class="analysis-change bottom-buffer">
  <span class="analysis-change__description label-small-semibold">Click on a cell to drill down into its contribution breakdown.</span>
  <input class="analysis-change__checkbox" type="checkbox" id="show-details" {{#if this.showDetailsChecked}}checked{{/if}}>
  <label for="show-details" class="metric-label analysis-change__label">See Contribution Details</label>
  <input class="analysis-change__checkbox" type="checkbox" id="show-cumulative" {{#if this.showCumulativeChecked}}checked{{/if}}>
  <label for="show-cumulative" class="metric-label analysis-change__label">Show Cumulative</label>
</div>

<div class="row">
  <div id="contributor-table-placeholder"></div>
</div>
