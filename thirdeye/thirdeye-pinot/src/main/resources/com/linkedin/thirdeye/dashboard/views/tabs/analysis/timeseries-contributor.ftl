<div class="row bottom-buffer">
  <div class="col-md-12">
    <div>
      <label>{{this.metricName}}</label>
    </div>
    <#--<div>-->
       <#--3 anomalies detected for selected parameters <a href="#anomalies">See Anomalies</a>-->
    <#--</div>-->
  </div>
</div>
<!-- Chart section -->
<div class="row">
  <div class="col-md-10">
    <div class="panel">
      <div id="analysis-chart" style="height: 400px"></div>
    </div>
  </div>
  <div class="col-md-2">
    <div id="analysis-chart-legend" style="height: 400px;overflow: scroll;">
    </div>
  </div>
</div>

<!-- Percentage change table -->
<div class="row">
  <div class="col-md-2 pull-left ">
    <span class="label-medium-semibold">Total % Changes</span>
  </div>
  <div class="col-md-3">
    <input type="checkbox" id="show-details" {{#if this.showDetailsChecked}}checked{{/if}}>
    <label for="show-details" class="metric-label">See Contribution Details</label>
  </div>
  <div class="col-md-3">
    <input type="checkbox" id="show-cumulative" {{#if this.showCumulativeChecked}}checked{{/if}}>
    <label for="show-cumulative" class="metric-label">Show Cumulative</label>
  </div>
  <div class="col-md-4"></div>
</div>

<div class="row bottom-buffer">
  <div class="col-md-12">
    <span class="label-small-semibold">Click on a cell to drill down into its contribution breakdown.</span>
  </div>
</div>

<div class="row">
  <div id="contributor-table-placeholder"></div>
</div>
