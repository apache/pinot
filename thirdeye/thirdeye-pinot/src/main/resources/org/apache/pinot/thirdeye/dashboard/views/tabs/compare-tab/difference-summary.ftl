<section id="heatmap-difference-summary-section" class="hidden" style="margin: 0;">
  <script id="difference-summary" type="text/x-handlebars-template">

  <table>
    <tr>
      <th colspan="3">Dimension</th>
      <th colspan="3">Metrics</th>
    </tr>
    <tr>
       {{#with dimensions}}
       {{#each this as |dimensionName dimensionIndex|}}
        <th>dimensionName</th>
       {{/each}}
       {{/with}}
      <th>Baseline</th>
      <th>Current</th>
      <th>ratio</th>
    </tr>
    {{#with responseRows}}
    {{#each this as |row rowIndex|}}
      <tr>

        <!--{{!--{{#each row.names as |dimensionValue dimension|}}
          <td>{{dimensionValue}}</td>
        {{/each}}--}}-->
        <td>{{row.baselineValue}}</td>
        <td>{{row.currentValue}}</td>
        <td>{{row.ratio}}</td>
      </tr>
    {{/each}}
    {{/with}}
  </table>

  </script>
</section>

