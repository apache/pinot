<div class="container-fluid">
  <div class="row row-bordered">
    <div class="container top-buffer bottom-buffer">
      <div>
        Showing <label style="font-size: 15px; font-weight: 500"> {{this.numAnomaliesOnPage}} </label> anomalies of <label style="font-size: 15px; font-weight: 500"> {{this.totalAnomalies}}</label>
      </div>
    </div>
    {{#each this.anomalyDetailsList as |anomalyData anomalyIndex|}}
    <div class="container">
      <div class="panel padding-all">
        <div class="row">
          <div id="show-details-{{anomalyIndex}}" class="col-md-6">
            <label>{{this.anomalyId}}</label>
          </div>
          <div id="root-cause-analysis-button-{{anomalyIndex}}" class="col-md-6">
            <button type="button" class="btn btn-primary btn-sm pull-right">Show Root Cause Analysis</button>
          </div>
        </div>
        <div class="row">
          <div class="col-md-3">
            <label>{{this.metric}}</label>
          </div>
        </div>
        <div class="row">
          <div class="col-xs-10">
            <div id="anomaly-chart-{{anomalyIndex}}" style="height: 200px"></div>
          </div>
          <div class="col-xs-2">
            <div class="row">
              <div class="col-xs-4">
                <span class="pull-right">______</span>
              </div>
              <div id="current-range-{{anomalyIndex}}" class="col-md-8">
                <span class="pull-left"><label></label></span>
              </div>
            </div>
            <div class="row">
              <div class="col-md-4">
                <span class="pull-right">------</span>
              </div>
              <div id="baseline-range-{{anomalyIndex}}" class="col-md-8">
                <span class="pull-left"><label></label></span>
              </div>
            </div>
          </div>
        </div>
        <div class="row">
          <div class="col-md-5">
            <div class="row">
              <div class="col-xs-3">
                <span class="pull-left">Dimension</span>
              </div>
              <div id="dimension-{{anomalyIndex}}" class="col-xs-9">
                <span class="pull-left"></span>
              </div>
            </div>
            <div class="row">
              <div class="col-xs-3">Current</div>
              <div id="current-value-{{anomalyIndex}}" class="col-xs-9"></div>
            </div>
            <div class="row">
              <div class="col-xs-3">Baseline</div>
              <div id="baseline-value-{{anomalyIndex}}" class="col-xs-9"></div>
            </div>
            <div class="row">
              <div class="col-xs-3">Start-End (PDT)</div>
              <div id="region-{{anomalyIndex}}" class="col-xs-9"></div>
            </div>
          </div>
          <div class="col-md-7">
            <label>Anomaly Function Details:</label>
            <section class="anomaly-function-detail">
              <div>{{this.anomalyFunctionName}}</div>
              <div>{{this.anomalyFunctionType}}</div>
            <section>
          </div>
        </div>
        <div class="row row-footer">
          <div id="anomaly-feedback-{{anomalyIndex}}" class="col-xs-12">
            <label>This anomaly is as</label>
            <select data-placeholder="Provide Anomaly Feedback" style="width: 250px; border: 0px" class="chosen-select">
              <option>False Alarm</option>
              <option>Confirmed Anomaly</option>
              <option>Confirmed - Not Actionable</option>
            </select>
          </div>
        </div>
      </div>
    </div>
    {{/each}}
    <div class="container text-center">
      <nav aria-label="Page navigation">
        <ul id="pagination" class="pagination-sm"></ul>
      </nav>
    </div>
  </div>
</div>
