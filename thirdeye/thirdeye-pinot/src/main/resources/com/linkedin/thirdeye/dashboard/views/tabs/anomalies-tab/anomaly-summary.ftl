<section id="anomaly-summary-template-section">
  <script id="anomaly-summary-template" type="text/x-handlebars-template">
    <table id="anomaly-summary-table" class="anomaly2">
      <thead>
      <tr>
        <td>id</td>
        <td>startTime</td>
        <td>endTime</td>
        <td>function id</td>
        <td>collection</td>
        <td>metric</td>
        <td>score</td>
        <td>weight</td>
        <td>dimensions</td>
      </tr>
      </thead>
      <tbody>
      {{#each this as |summary summaryIndex|}}
      <tr class="anomaly-summary">
        <td>{{summary/id}}</td>
        <td>{{millisToDate summary/startTime}}</td>
        <td>{{millisToDate summary/endTime}}</td>
        <td>{{summary/function/id}}</td>
        <td>{{summary/collection}}</td>
        <td>{{summary/metric}}</td>
        <td>{{summary/score}}</td>
        <td>{{summary/weight}}</td>
        <td>{{summary/dimensions}}</td>
      </tr>
      {{#with summary/anomalyResults}}
      {{#each this as |anomalyResult resultIndex|}}
      <tr class="anomaly-detail">
        <td>{{anomalyResult/id}}</td>
        <td>{{millisToDate anomalyResult/startTimeUtc}}</td>
        <td>{{millisToDate anomalyResult/endTimeUtc}}</td>
        <td>{{anomalyResult/function/id}}</td>
        <td>{{anomalyResult/collection}}</td>
        <td>{{anomalyResult/metric}}</td>
        <td>{{anomalyResult/score}}</td>
        <td>{{anomalyResult/weight}}</td>
        <td>{{anomalyResult/dimensions}}</td>
      </tr>
      {{/each}}
      {{/with}}
      {{/each}}
      </tbody>
    </table>
  </script>
</section>
