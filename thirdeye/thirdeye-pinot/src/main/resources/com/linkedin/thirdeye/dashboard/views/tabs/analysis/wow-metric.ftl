<div class="col-md-12">
  <div class="table-responsive">
    <table class="table table-bordered analysis-table">
      <thead>
      <tr>
        {{#if this.timestamps.length}}
        {{#each this.timestamps as |timestamp timeIndex|}}
        <th>{{displayHour timestamp}}</th>
        {{/each}}
        {{/if}}
      </tr>
      </thead>
      <tbody>
      <tr>
        {{#each this.metricTable.[0].data as |data dataIndex|}}
        <td>{{data}}</td>
        {{/each}}
      </tr>
      </tbody>
    </table>
  </div>
</div>
