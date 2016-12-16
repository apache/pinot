<ul class="nav nav-pills nav-stacked">
  {{#if this.subDimensions}}
  <div>{{this.dimension}}</div>
  {{#each this.subDimensions as |subDimension subDimensionIndex|}}
  <li>
  <a id="a-sub-dimension-{{subDimension}}" href="#">{{subDimension}}</a>
  </li>
  {{/each}}
  {{/if}}
</ul>
