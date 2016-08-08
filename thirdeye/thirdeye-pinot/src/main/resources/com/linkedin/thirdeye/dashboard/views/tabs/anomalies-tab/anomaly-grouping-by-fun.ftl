<section id="anomaly-grouping-by-fun-template-section">
  <script id="anomaly-grouping-by-fun-template" type="text/x-handlebars-template">
    <div id="anomaly-grouping-by-fun">
      <table id="anomaly-grouping-table" class="anomaly2">
        <thead>
        <tr>
          <td>function name</td>
          <td>collection</td>
          <td>metric</td>
          <td>#anomalies</td>
        </tr>
        </thead>
        <tbody>
        {{#each this as |groupbyrow rowIndex|}}
        <tr>
          <td>{{groupbyrow/groupBy/functionName}}</td>
          <td>{{groupbyrow/groupBy/dataset}}</td>
          <td>{{groupbyrow/groupBy/metric}}</td>
          <td>{{groupbyrow/value}}</td>
          <td><span class="show-anomaly-grouping uk-button"
                    onclick="renderAnomalySummaryByFunction({{groupbyrow/groupBy/functionId}})"
                    data-uk-tooltip
                    title="Show Anomalies"><i class="uk-icon-list"></i></span></td>
        </tr>
        {{/each}}
        </tbody>
      </table>
    </div>
  </script>
</section>
