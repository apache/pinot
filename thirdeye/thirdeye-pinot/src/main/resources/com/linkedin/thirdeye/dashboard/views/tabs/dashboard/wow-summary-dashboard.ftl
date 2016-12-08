<div class="row bottom-line row-bordered">
  <div class="col-md-12">
    <div class="container top-buffer bottom-buffer">
      <div class="table-responsive">
        <table class="table dashboard-table" style="border-collapse: separate; border-spacing: 0em 1em">
          <thead>
          <tr>
            {{#if this.timestamps.length}}
            <th><div class="row-label"> {{displayDate this.timestamps.[0]}} </div></th>
            {{#each this.timestamps as |timestamp timeIndex|}}
            <th>{{displayHour timestamp}}</th>
            {{/each}}
            {{/if}}
          </tr>
          </thead>
          <tbody>
          {{#each this.wowSummary as |row rowIndex|}}
          <tr class="bg-white">
            <td><div>
              <a href="#"><span class="metric-label">{{row.metricName}}</span></a>
            </div></td>
            {{#each row.data as |data dataIndex|}}
            <td><div class="box {{#if data}}box-anomaly{{/if}}">{{data}}</div></td>
            {{/each}}
          </tr>
          {{/each}}
        </table>
      </div>
    </div>
  </div>
</div>


