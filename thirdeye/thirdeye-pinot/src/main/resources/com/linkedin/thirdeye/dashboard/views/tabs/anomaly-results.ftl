<div class="anomalies-panel">
  <div class ="padding-all">
    <span class="panel-title">Anomalies</span>
    <span class="pull-right">
    Showing <strong>{{this.numAnomaliesOnPage}} </strong> anomalies of <strong> {{this.totalAnomalies}}</strong>
    <span>
  </div>
  <div class="anomaly-cards-wrapper padding-all">
    {{#each this.anomalyDetailsList as |anomalyData anomalyIndex|}}
      {{#with anomalyData}}
        <div class="anomaly-card">
          <div class="anomaly-result-header">
            <div class="anomaly-result-title">
              <span>&num;{{anomalyId}} {{metric}}</span>
            </div>
            <div id="root-cause-analysis-button-{{anomalyIndex}}">
              <a type="button" class="btn thirdeye-btn pull-right">Investigate</a>
            </div>
          </div>

          <div class ="anormaly-chart" id="anomaly-chart-{{anomalyIndex}}"></div>

          <div class="anomaly-details">
            <div class="anomaly-details-row">
              <div class="anomaly-details-items anomaly-details-items--small">
                <label class="label-medium-semibold">Change</label>
                <span class="anomaly-change-delta {{colorDelta changeDelta}}">{{changeDelta}}</span>
              </div>

              <div class="anomaly-details-items anomaly-details-items--small">
                <label class="label-medium-semibold">Current</label>
                <span>{{current}}</span>
              </div>
              <div class="anomaly-details-items anomaly-details-items--small">
                <label class="label-medium-semibold">Baseline</label>
                <span>{{baseline}}</span>
              </div>
            </div>
            <div class="anomaly-details-row">
              <div class="anomaly-details-items">
                <label class="label-medium-semibold">Dimension</label>
                <span>
                {{#if_eq anomalyFunctionDimension '{}'}}
                  N/A
                {{else}}
                  {{anomalyFunctionDimension}}
                {{/if_eq}}
                </span>
              </div>

              <div class="anomaly-details-items">
                <label class="label-medium-semibold">Function</label>
                <span>{{anomalyFunctionName}}</span>
              </div>
            </div>
            <div class="anomaly-details-row">
              <div class="anomaly-details-items">
                <label class="label-medium-semibold">Duration</label>
                <span class="anomaly-duration">{{duration}}</span>
              </div>

              <div class="anomaly-details-items">
                <label class="label-medium-semibold">Status</label>
                <span>
                  {{#if anomalyFeedback}}
                    Resolved ({anomalyStatus}})
                  {{else}}
                    Not Resolved
                  {{/if}}
                </span>
              </div>
            </div>
          </div>
        </div>
      {{/with}}
    {{/each}}
  </div>
  <div class="text-center padding-all">
    <nav aria-label="Page navigation">
      <ul id="pagination" class="pagination-sm thirdeye-pagination"></ul>
    </nav>
  </div>
</div>
