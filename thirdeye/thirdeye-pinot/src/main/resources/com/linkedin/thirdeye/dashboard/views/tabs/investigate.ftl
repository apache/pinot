<div class="anomaly-feedback padding-all">
  <div class="container">
  <div class="row">
    <div class="col-xs-9">
      <h3 class="anomaly-feedback-title">Your feedback is valuable!</h3>
      <p>Your feedback will help us improve the accuracy of our anomaly detection techniques. Please provide us feedback based on your diagnostics!</p>
    </div>
    <div class="col-xs-3">
      <select data-placeholder="Provide Anomaly Feedback" class="anomaly-feedback-select">
        <option>False Alarm</option>
        <option>Confirmed Anomaly</option>
        <option>Confirmed - Not Actionable</option>
      </select>
    </div>
  </div>
  </div>
</div>

<div class="container">

  <div class="investigate-title">{{metric}}</div>

  <div class ="anormaly-chart bg-white" id="anomaly-investigate-chart"></div>
  <div class="anomaly-details bg-white">
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
        <span>
          {{#if baseline}}
            {{baseline}}
          {{else}}
            N/A
          {{/if}}
        </span>
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
            Resolved ({{anomalyFeedback}})
          {{else}}
            Not Resolved
          {{/if}}
        </span>
      </div>
    </div>
  </div>

  <div class="investigate-title">Root Cause Summary</div>

  <div class="investigate-cards">

    <div class="investigate-card">
      <div class="investigate-card-title">
        Current
      </div>
      <div class="investigate-card-body">
        <div class="investigate-number">100k</div>
<!--         <hr class="investigate-divider"/>
        <label class="label-medium-semibold">Events</label>
        <span>Diwali (ind)</span> -->
      </div>
    </div>

    <div class="investigate-card">
      <div class="investigate-card-title">
        WoW
      </div>
      <div class="investigate-card-body">
        <div class="investigate-number">125k (-20%)</div>
      </div>
    </div>

    <div class="investigate-card">
      <div class="investigate-card-title">
        WoW2
      </div>
      <div class="investigate-card-body">
        <div class="investigate-number">125k (-20%)</div>
      </div>
    </div>

    <div class="investigate-card">
      <div class="investigate-card-title">
        WoW3
      </div>
      <div class="investigate-card-body">
        <div class="investigate-number">125k (-20%)</div>
      </div>
    </div>

  </div>



  <!-- <div class="investigate-tips padding-all">
    <div class="investigate-icon">
      <img rel="lightbulb" src="assets/img/Lightbulb.png">
    </div>
    <div class="investigate-tips-body">
      <label class="label-medium-semibold">Investigating Tip</label>
      <p>Anomalies are Tricky! Sure an event such as <strong>Diwali</strong> may be the cause for the anomaly. but where did the drop take place? Did it actually happen in India? Always take a double look and watch out for these tricky anomalies!</p>
    </div>
  </div> -->
</div>



