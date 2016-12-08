<div class="col-md-12">
  <div class="table-responsive">
    <table class="table table-bordered analysis-table">
      <thead>
      <tr>
        <th class="dim-label">By {{this.dimensionName}}</th>
        {{#if this.timestamps.length}}
        {{#each this.timestamps as |timestamp timeIndex|}}
        <th>{{displayHour timestamp}}</th>
        {{/each}}
        {{/if}}
      </tr>
      </thead>
      <tbody>
        {{#each this.metricDimansionTable as |row rowIndex|}}
          <tr>
            <td class="dim-label">{{row.metricName}}</td>
            {{#each row.data as |data dataIndex|}}
              <td>{{data}}</td>
            {{/each}}
          </tr>
        {{/each}}
      </tbody>
    </table>
  </div>
</div>
