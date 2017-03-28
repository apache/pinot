<div class="row bottom-buffer">
  <div class="col-xs-12">
    <div>
      <label class="label-medium-semibold">Metric: </label>
      <label>{{this.metricName}}</label>
    </div>
    <#--<div>-->
       <#--3 anomalies detected for selected parameters <a href="#anomalies">See Anomalies</a>-->
    <#--</div>-->
  </div>
</div>
<!-- Chart section -->
<div class="row">
  <div class="col-xs-10">
    <div class="panel">
      <div id="analysis-chart" style="height: 400px"></div>
    </div>
  </div>
  <div class="col-xs-2">
    <div id="analysis-chart-legend" style="height: 400px;overflow: scroll;">
    </div>
  </div>
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
