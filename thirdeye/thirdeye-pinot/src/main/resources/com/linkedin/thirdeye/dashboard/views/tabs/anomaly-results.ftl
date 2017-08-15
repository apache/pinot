<div class="anomalies-panel">
  <div class ="padding-all">
    <span class="panel-title">Anomalies</span>
    <span class="pull-right">
    Showing <strong>{{this.numAnomaliesOnPage}} </strong> anomalies of <strong> {{this.totalAnomalies}}</strong>
    </span>
    <div class="filter-list">
      <div>Selected Filters:</div>
      <span class="filter-list__item">
      {{#if appliedFilters}}
        {{appliedFilters}}
      {{else}}
        N/A
      {{/if}}
      </span>
    </div>
  </div>
  <div class="anomaly-cards-wrapper padding-all">
    {{#each this.anomalyDetailsList as |anomalyData anomalyIndex|}}
      {{#with anomalyData}}
        <div class="anomaly-card" id="anomaly-card-{{anomalyIndex}}">
          <div class="anomaly-result-header">
            <div class="anomaly-result-title">
              <span class="anomaly-result-metric">{{metric}}</span> from <span class="anomaly-result-dataset">{{dataset}}</span>
              <div>&num;{{anomalyId}}</div>
            </div>
            <div id="investigate-button-{{anomalyIndex}}">
              <a href="/thirdeye#investigate?anomalyId={{anomalyId}}" type="button" class="btn thirdeye-btn pull-right">Investigate</a>
            </div>
          </div>

          <div class ="anomaly-chart" id="anomaly-chart-{{anomalyIndex}}"></div>

          <div class="anomaly-details bg-white">
            <div class="anomaly-details-row">
              <div class="anomaly-details-items anomaly-details-items--small">
                <label class="label-medium-semibold">Change</label>
                <span class="anomaly-change-delta {{colorDelta changeDelta}}">{{changeDelta}}</span>
              </div>
              <div class="anomaly-details-items anomaly-details-items--small">
                <label class="label-medium-semibold">Duration</label>
                <span class="anomaly-duration">{{duration}}</span>
              </div>
              <div class="anomaly-details-items anomaly-details-items--small">
                <label class="label-medium-semibold">Status</label>
                <span>
                  <select class="card-feedback-select hidden">
                    <option value="ANOMALY">Yes (True Anomaly)</option>
                    <option value="ANOMALY_NEW_TREND">Yes (New Trend)</option>
                    <option value="NOT_ANOMALY">No (False Alarm)</option>
                  </select>
                  <span class="card-feedback-final"> 
                    {{#if anomalyFeedback}}
                      Resolved ({{anomalyFeedback}})
                    {{else}}
                      Not Resolved
                    {{/if}}
                  </span>
                  <button class="thirdeye-link thirdeye-link--secondary" id="feedback-edit-{{anomalyIndex}}" title="edit">
                    <span class="thirdeye-link__icon"><i class="glyphicon glyphicon-pencil"></i></span> 
                  </button>
                  <button class="thirdeye-link thirdeye-link--secondary hidden" id="feedback-submit-{{anomalyIndex}}" title="submit">
                    <span class="thirdeye-link__icon"><i class="glyphicon glyphicon-ok"></i></span> 
                  </button>
                  <button class="thirdeye-link thirdeye-link--secondary thirdeye-link--delimited hidden" id="feedback-cancel-{{anomalyIndex}}" title="cancel">
                    <span class="thirdeye-link__icon"><i class="glyphicon glyphicon-remove"></i></span> 
                  </button>
                </span>
              </div>
              {{#if issueType}}
                <div class="anomaly-details-items anomaly-details-items--small">
                  <label class="label-medium-semibold">Issue Type</label>
                  <span>
                      {{issueType}}
                    </span>
                </div>
              {{/if}}
            </div>
            <div class="anomaly-details-row">
              <div class="anomaly-details-items">
                <label class="label-medium-semibold">Dimension</label>
                <span>
                {{#if_eq anomalyFunctionDimension '{}'}}
                  N/A
                {{else}}
                  {{parseFilters anomalyFunctionDimension}}
                {{/if_eq}}
                </span>
              </div>

              <div class="anomaly-details-items">
                <label class="label-medium-semibold">Alert Name</label>
                <span>{{anomalyFunctionName}}</span>
              </div>
            </div>
            <div class="anomaly-details-row">
              <div class="anomaly-details-items">
                <label class="label-medium-semibold">Current Avg</label>
                <span>{{formatNumber current}}</span>
              </div>
              <div class="anomaly-details-items">
                <label class="label-medium-semibold">Baseline Avg</label>
                <span>
                  {{#if baseline}}
                    {{formatNumber baseline}}
                  {{else}}
                    N/A
                  {{/if}}
                </span>
              </div>
            </div>
          </div>
        </div>
      {{/with}}
    {{else}}
      <div class="anomaly-card">
        No anomalies found.
      </div>
    {{/each}}
  </div>
  <div class="text-center padding-all">
    <nav aria-label="Page navigation">
      <ul id="pagination" class="pagination-sm thirdeye-pagination"></ul>
    </nav>
  </div>
</div>
