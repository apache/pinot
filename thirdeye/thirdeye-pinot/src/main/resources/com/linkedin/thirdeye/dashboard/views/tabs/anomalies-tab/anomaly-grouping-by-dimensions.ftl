<section id="anomaly-grouping-by-fun-dim-template-section">
  <script id="anomaly-grouping-by-fun-dim-template" type="text/x-handlebars-template">
    <div id="anomaly-grouping-by-fun-dim">
      <table id="anomaly-grouping-table" class="anomaly2">
        <thead>
        <tr>
          <td>collection</td>
          <td>metric</td>
          <td>dimensions</td>
          <td>#anomalies</td>
        </tr>
        </thead>
        <tbody>
        {{#each this as |groupbyrow rowIndex|}}
        <tr>
          <td>{{groupbyrow/groupBy/collection}}</td>
          <td>{{groupbyrow/groupBy/metric}}</td>
          <td>{{groupbyrow/groupBy/dimensions}}</td>
          <td>{{groupbyrow/value}}</td>
          <td><span class="show-anomaly-grouping uk-button"
                    onclick="renderAnomalySummaryByDimensions('{{groupbyrow/groupBy/collection}}','{{groupbyrow/groupBy/metric}}','{{groupbyrow/groupBy/dimensions}}')"
                    data-uk-tooltip
                    title="Show Anomalies"><i class="uk-icon-list"></i></span></td>
        </tr>
        {{/each}}
        </tbody>
      </table>
    </div>
  </script>
</section>
