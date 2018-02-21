{{#with anomaly}}
  <div class="anomaly-feedback padding-all">
    <div class="container">
      <div class="row">
        <h3 class="anomaly-feedback-title">
          <span>Is this an anomaly?</span>
          <a class="anomaly-feedback-title__help thirdeye-link" href="https://iwww.corp.linkedin.com/wiki/cf/display/ENGS/Anomaly+Labeling+on+ThirdEye#AnomalyLabelingonThirdEye-WhataretheLabels?" target="_blank">Learn more about each option</a>
        </h3>
      </div>
      <form class="anomaly-feedback-radio" id="anomaly-feedback-radio">
          <label class="radio-inline" for="feedback-radio-yes">
            <input type="radio" id="feedback-radio-yes" name="feedback-radio" value="ANOMALY">
            <span class="label-medium-semibold">Yes</span> (True Anomaly)
          </label>
          <label for=feedback-radio-na class="radio-inline">
            <input class="" type="radio" id="feedback-radio-na" name="feedback-radio" value="ANOMALY_NEW_TREND">
            <span class="label-medium-semibold">Yes </span> <span>(But New Trend)<span>
          </label>
          <label for=feedback-radio-no class="radio-inline">
            <input class="" type="radio" id="feedback-radio-no" name="feedback-radio" value="NOT_ANOMALY">
            <span class="label-medium-semibold">No</span> (False Alarm)
          </label><br>
      </form>
      <div id="anomaly-feedback-comment" class="anomaly-feedback__comment anomaly-feedback__comment--show form-group hidden">
        <label for="feedback-comment"><span class="label-medium-semibold">Comment</span> (optional)</label>
        <textarea placeholder="Enter a comment" class="form-control" rows="2" id="feedback-comment"></textarea>
      </div>
      <p>Your feedback will help us improve the accuracy of our anomaly detection techniques.</p>
    </div>
  </div>

  <div class="container">
    <div class="investigate-header">
      <span class="investigate-title">
        {{metric}} <span> from </span> {{dataset}}
        <div>&num;{{anomalyId}}</div>
      </span>
      <div class="investigate-button">
        {{#if ../externalUrls.INGRAPH}}
          <a href={{../externalUrls.INGRAPH}} target="_blank" class="thirdeye-link">InGraphs</a>
        {{/if}}
       {{#if ../externalUrls.HARRIER}}
          <a href={{../externalUrls.HARRIER}} target="_blank" class="thirdeye-link">Harrier</a>
        {{/if}}
      </div>
    </div>

    <div class="anomaly-details-wrapper padding-all bg-white">
      <div class ="anomaly-chart" id="anomaly-investigate-chart"></div>
      <div class="anomaly-details">
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
            <span id="anomaly-feedback">
              {{#if anomalyFeedback}}
                Resolved ({{anomalyFeedback}})
              {{else}}
                Not Resolved
              {{/if}}
            </span>
          </div>
          {{#if issueType}}
            <div class="anomaly-details-items anomaly-details-items--small">
              <label class="label-medium-semibold">Issue Type</label>
              <span>{{issueType}}</span>
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
    <div class="investigate-header">
      <span class="investigate-title">Change Over Time</span>
    </div>

    <ul class="investigate-wow">
      <li class="wow-card">
        <div class="wow-card-header">
          <label class="label-medium-semibold">Current Average</label>
        </div>
        <div class="wow-card-body">
          {{formatNumber (formatDouble currentValue)}}
        </div>
        <div class="wow-card-footer"></div>
      </li>

      {{#each wowResults as |wow|}}
        <li class="wow-card">
          <div class="wow-card-header">
            <label class="label-medium-semibold">{{wow.compareMode}}</label>
          </div>
          <div class="wow-card-body">
            {{formatNumber (formatDouble wow.baselineValue)}}
            <span class="anomaly-change-delta {{colorDelta wow.change}}">({{formatPercent wow.change}})</span>
          </div>
          <div class="wow-card-footer">
             {{#if wow.isLast}} 
               <a href="{{wow.betaUrl}}" target="_blank" class="thirdeye-link">Root Cause Analysis</a>
             {{/if}}
          </div>
        </li>
      {{/each}}
    </ul>
</div>



