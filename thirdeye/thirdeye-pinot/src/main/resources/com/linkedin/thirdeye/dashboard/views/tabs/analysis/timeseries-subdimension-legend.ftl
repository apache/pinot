<ul class="analysis-chart__dimensions">
  {{#if this.subDimensions}}
  <label class="label-medium-semibold">{{this.dimension}}</label>

  {{#each this.subDimensions as |subDimension subDimensionIndex|}}
<!--   <li id="a-sub-dimension-{{subDimensionIndex}}">
    <a id="{{subDimensionIndex}}">{{subDimension}}</a>
  </li> -->
    <li class="analysis-chart__dimension">
      <input class="analysis-chart__checkbox" type="checkbox" id={{subDimensionIndex}}>
      <label for={{subDimensionIndex}} class="metric-label analysis-change__label">{{subDimension}}</label>
    </li>
  {{/each}}
  {{/if}}
</ul>
