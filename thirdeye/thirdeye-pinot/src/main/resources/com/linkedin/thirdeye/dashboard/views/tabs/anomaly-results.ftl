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
          <div id="show-details-{{anomalyIndex}}" class="col-md-3">
            <label>{{this.anomalyId}}<a href="#"> show details</a></label>
          </div>
          <div class="col-md-6"></div>
          <div class="col-md-1">
            <span class="pull-right">______</span>
          </div>
          <div id="current-range-{{anomalyIndex}}" class="col-md-2">
            <span class="pull-left"><label></label></span>
          </div>
        </div>
        <div class="row">
          <div class="col-md-3">
            <label>{{this.metric}}</label>
          </div>
          <div class="col-md-6"></div>
          <div class="col-md-1">
            <span class="pull-right">--------</span>
          </div>
          <div id="baseline-range-{{anomalyIndex}}" class="col-md-2">
            <span class="pull-left"><label></label></span>
          </div>
        </div>
        <div class="row">
          <div class="col-md-12">
            <div id="anomaly-chart-{{anomalyIndex}}" style="height: 200px"></div>
          </div>
        </div>
        <div class="row">
          <div class="col-md-7">
            <div class="row">
              <div class="col-md-3">
                <span class="pull-left">Dimension</span>
              </div>
              <div id="dimension-{{anomalyIndex}}" class="col-md-9">
                <span class="pull-left"></span>
              </div>
            </div>
            <div class="row">
              <div class="col-md-3">Current</div>
              <div id="current-value-{{anomalyIndex}}" class="col-md-9"></div>
            </div>
            <div class="row">
              <div class="col-md-3">Baseline</div>
              <div id="baseline-value-{{anomalyIndex}}" class="col-md-9"></div>
            </div>
            <div class="row">
              <div class="col-md-3">Start-End (PDT)</div>
              <div id="region-{{anomalyIndex}}" class="col-md-9"></div>
            </div>
          </div>
          <div class="col-md-5">
            <label>Anomaly Function Details:</label><br /> <label>{{this.anomalyFunctionName}}</label> <br /> <label>{{this.anomalyFunctionType}}</label>
          </div>
        </div>
        <div class="row">
          <div id="anomaly-feedback-{{anomalyIndex}}" class="col-md-3">
            <select data-placeholder="Provide Anomaly Feedback" style="width: 250px; border: 0px" class="chosen-select">
              <option>False Alarm</option>
              <option>Confirmed Anomaly</option>
              <option>Confirmed - Not Actionable</option>
            </select>
          </div>
          <div class="col-md-6"></div>
          <div id="root-cause-analysis-button-{{anomalyIndex}}" class="col-md-3">
            <button type="button" class="btn btn-primary btn-sm">Root Cause Analysis</button>
          </div>
        </div>
      </div>
    </div>
    {{/each}}
  </div>
</div>