<ul class="analysis-chart__dimensions" id="chart-dimensions">
  {{#if subDimensions}}
  <label class="label-medium-semibold">{{dimension}}:</label>

  {{#each subDimensions as |subDimension subDimensionIndex|}}
    <li class="analysis-chart__dimension">
      <input class="analysis-chart__checkbox" type="checkbox" id="{{subDimensionIndex}}" checked={{true}}>
      <label for="{{subDimensionIndex}}" class="metric-label analysis-change__label">{{subDimension}}</label>
    </li>
  {{/each}}
  {{/if}}
</ul>
