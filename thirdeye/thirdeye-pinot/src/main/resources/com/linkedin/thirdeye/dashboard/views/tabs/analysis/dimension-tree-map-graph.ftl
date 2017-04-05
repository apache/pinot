<div class="analysis-card analysis-summary bottom-buffer padding-all">
    <div class="analysis-summary__item">
      <label class="analysis-summary__label">Current Total</label>
      <!-- {{displayDate currentStart}} - {{displayDate currentEnd}} -->
      <span class="analysis-summary__data">{{formatNumber currentTotal}}</span>
    </div>
    <div class="analysis-summary__item">
      <label class="analysis-summary__label">Baseline Total</label>
      <!-- {{displayDate baselineStart}} - {{displayDate baselineEnd}} -->
      <span class="analysis-summary__data">{{formatNumber baselineTotal}}</span>
    </div>
    <div class="analysis-summary__item">
      <label class="analysis-summary__label">Change Value</label>
      <span class="analysis-summary__data {{colorDelta absoluteChange}}">{{formatNumber absoluteChange}}</span>
    </div>
    <div class="analysis-summary__item">
      <label class="analysis-summary__label">% Change</label>
      <span class="analysis-summary__data {{colorDelta percentChange}}">{{percentChange}} %</span>
    </div>
  </div>

  <div class="row">
    <div class="col-xs-12">
      <table class="table table-borderless tree-map__table">
        <tbody>
          <tr>
            <td class="col-xs-1"></td>
            <td class="col-xs-11">
              <div id="axis-placeholder" style="height: 25px; width: 100%"></div>
            </td>
          </tr>
          {{#each dimensions}}
          <tr>
            <td class="col-xs-1" style="vertical-align: middle" class="label-medium-light">{{this}}</td>
            <td class="col-xs-11">
              <div id="{{this}}-heatmap-placeholder" style="height: 50px; width: 100%"></div>
            </td>
          </tr>
          {{/each}}
        </tbody>
      </table>
    </div>
  </div>
  <div id="tooltip" class="hidden">
    <p><strong id="heading"></strong></p>
    <p><span id="percentageChange"></span></p>
    <p><span id="currentValue"></span></p>
    <p><span id="baselineValue"></span></p>
  </div>
