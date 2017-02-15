<div class="container-fluid">
  <div class="row row-bordered">
    <div class="container top-buffer bottom-buffer">
      <div>
        Showing <strong>{{this.numAnomaliesOnPage}} </strong> anomalies of <strong> {{this.totalAnomalies}}</strong>
      </div>
    </div>
    {{#each this.anomalyDetailsList as |anomalyData anomalyIndex|}}
    <div class="container">
      <div class="panel padding-all">
        <div class="row">
          <div id="show-details-{{anomalyIndex}}" class="col-md-6">
            <span>&num;{{this.anomalyId}}</span>
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
          <div class="col-xs-12">
            <div id="anomaly-chart-{{anomalyIndex}}" style="height: 200px"></div>
          </div>
        </div>
        <div class="row">
          <div class="col-md-5">
            <div class="row">
              <div class="col-xs-3 text-right">Dimension:</div>
              <div id="dimension-{{anomalyIndex}}" class="col-xs-9">
                <span class="pull-left"></span>
              </div>
            </div>
            <div class="row">
              <div class="col-xs-3 text-right">Current:</div>
              <div id="current-value-{{anomalyIndex}}" class="col-xs-9"></div>
            </div>
            <div class="row">
              <div class="col-xs-3 text-right">Baseline:</div>
              <div id="baseline-value-{{anomalyIndex}}" class="col-xs-9"></div>
            </div>
            <div class="row">
              <div class="col-xs-3 text-right">Start (PDT):</div>
              <div id="region-start-{{anomalyIndex}}" class="col-xs-9"></div>
            </div>
            <div class="row">
              <div class="col-xs-3 text-right">End PDT):</div>
              <div id="region-end-{{anomalyIndex}}" class="col-xs-9"></div>
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
            <label>This anomaly is a</label>
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
