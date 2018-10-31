<div class="analysis-card analysis-summary bottom-buffer padding-all">
    <div class="analysis-summary__item">
      <label class="analysis-summary__label">Current Total</label>
      <span class="analysis-summary__data">{{formatNumber currentTotal}}</span>
    </div>
    <div class="analysis-summary__item">
      <label class="analysis-summary__label">Baseline Total</label>
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
  <div id="tooltip" class="analysis-tooltip hidden">
    <ul class="analysis-tooltip__list">
      <li class="analysis-tooltip__list-item"><span>value</span><span id="heading"></span></li>
      <li class="analysis-tooltip__list-item"><span>baseline value</span><span id="baselineValue"></span></li>
      <li class="analysis-tooltip__list-item"><span>current value</span><span id="currentValue"></span></li>
      <li class="analysis-tooltip__list-item"><span>percentage change </span><span id="percentageChange"></span></li>
      <li class="analysis-tooltip__list-item"><span>baseline contribution</span><span id="baselineContribution"></span></li>
      <li class="analysis-tooltip__list-item"><span>current contribution</span><span id="currentContribution"></span></li>
      <li class="analysis-tooltip__list-item"><span>contribution change</span><span id="contributionChange"></span></li>
    </ul>
  </div>
