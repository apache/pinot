<div class="anomaly-feedback padding-all">
  <div class="container">
      <h3 class="anomaly-feedback-title">Is this an anomaly?</h3>
        <form class="anomaly-feedback-radio" id="anomaly-feedback-radio">
            <label class="radio-inline label-medium-semibold"" for="feedback-radio-yes">
              <input type="radio" id="feedback-radio-yes" name="feedback-radio" value="yes">
              Yes
            </label>
            <label for=feedback-radio-no class="radio-inline label-medium-semibold">
              <input class="" type="radio" id="feedback-radio-no" name="feedback-radio" value="no">
              No
            </label>
        </form>
      <p>Your feedback will help us improve the accuracy of our anomaly detection techniques.</p>
  </div>
</div>

<div class="container">

  <div class="investigate-title">
    <span>{{metric}}</span>
    <div class="investigate-button">
      <a href="#" class="thirdeye-link">Share</a>
      <a href="#" class="thirdeye-link">InGraphs</a>
    </div>
  </div>

  <div class="anomaly-details-wrapper padding-all bg-white">
    <div class ="anormaly-chart" id="anomaly-investigate-chart"></div>
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
  </div>

  <div class="investigate-title">Change Over Time</div>

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



