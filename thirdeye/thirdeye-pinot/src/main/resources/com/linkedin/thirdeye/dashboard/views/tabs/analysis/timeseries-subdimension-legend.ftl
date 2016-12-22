<ul class="nav nav-pills nav-stacked">
  {{#if this.subDimensions}}
  <div>{{this.dimension}}</div>
  {{#each this.subDimensions as |subDimension subDimensionIndex|}}
  <li id="a-sub-dimension-{{subDimensionIndex}}">
  <a id="{{subDimensionIndex}}">{{subDimension}}</a>
  </li>
  {{/each}}
  {{/if}}
</ul>
