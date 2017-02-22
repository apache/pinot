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

        <div class="anomaly-result-header">
          <div id="show-details-{{anomalyIndex}}" class="anomaly-result-title">
            <span>&num;{{this.anomalyId}} {{this.metric}}</span>
          </div>
          <div id="root-cause-analysis-button-{{anomalyIndex}}">
            <button type="button" class="btn btn-primary btn-sm pull-right">Investigate</button>
          </div>
        </div>

        <div class="row">
          <div class="col-xs-12">
            <div id="anomaly-chart-{{anomalyIndex}}" style="height: 300px"></div>
          </div>
        </div>

        <div class="anomaly-details">
          <div class="anomaly-details-row">
            <div class="anomaly-details-items anomaly-details-items--small">
              <label class="label-medium-semibold">Change</label>
              <span id="anomaly-change-{{anomalyIndex}}">{{this.changeDelta}}</span> {{this.changeDelta}}
            </div>

            <div class="anomaly-details-items anomaly-details-items--small">
              <label class="label-medium-semibold">Current</label>
              <span>{{this.current}}</span>
            </div>
            <div class="anomaly-details-items anomaly-details-items--small">
              <label class="label-medium-semibold">Baseline</label>
              <span>{{this.baseline}}</span>
            </div>
          </div>
          <div class="anomaly-details-row">
            <div class="anomaly-details-items">
              <label class="label-medium-semibold">Dimension</label>
              <span>
              {{#if_eq this.anomalyFunctionDimension '{}'}}
                N/A
              {{else}}
                {{this.anomalyFunctionDimension}}
              {{/if_eq}}
              </span>
            </div>

            <div class="anomaly-details-items">
              <label class="label-medium-semibold">Function</label>
              <span>{{this.anomalyFunctionName}}</span>
            </div>
          </div>
          <div class="anomaly-details-row">
            <div class="anomaly-details-items">
              <label class="label-medium-semibold">Duration</label>
              <span id="anomaly-region-{{anomalyIndex}}">
              </span>
            </div>

            <div class="anomaly-details-items">
              <label class="label-medium-semibold">Status</label>
              <span>
                {{#if this.anomalyFeedback}}
                  Resolved ({this.anomalyStatus}})
                {{else}}
                  Not Resolved
                {{/if}}
              </span>
            </div>
          </div>
        </div>

          <!-- <div class="col-md-5">
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
          </div> -->
        </section>

<!--         <div class="row row-footer">
          <div id="anomaly-feedback-{{anomalyIndex}}" class="col-xs-12">
            <label>This anomaly is a</label>
            <select data-placeholder="Provide Anomaly Feedback" style="width: 250px; border: 0px" class="chosen-select">
              <option>False Alarm</option>
              <option>Confirmed Anomaly</option>
              <option>Confirmed - Not Actionable</option>
            </select>
          </div>
        </div> -->
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
