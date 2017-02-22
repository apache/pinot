<div class="container-fluid">
  <div class="row row-bordered">
    <div class="container top-buffer bottom-buffer">
      <div>
        Showing <strong>{{this.numAnomaliesOnPage}} </strong> anomalies of <strong> {{this.totalAnomalies}}</strong>
      </div>
    </div>
    {{#each this.anomalyDetailsList as |anomalyData anomalyIndex|}}
      {{#with anomalyData}}
        <div class="container">
          <div class="panel padding-all">

            <div class="anomaly-result-header">
              <div id="show-details-{{anomalyIndex}}" class="anomaly-result-title">
                <span>&num;{{anomalyId}} {{metric}}</span>
              </div>
              <div id="root-cause-analysis-button-{{anomalyIndex}}">
                <button type="button" class="btn thirdeye-btn btn-sm pull-right">Investigate</button>
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
                  <span class="anomaly-change-delta {{colorDelta changeDelta}}">{{changeDelta}} </span>
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
                  <span>{{duration}}</span>
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
        </div>
      {{/with}}
    {{/each}}
    <div class="container text-center">
      <nav aria-label="Page navigation">
        <ul id="pagination" class="pagination-sm"></ul>
      </nav>
    </div>
  </div>
</div>
