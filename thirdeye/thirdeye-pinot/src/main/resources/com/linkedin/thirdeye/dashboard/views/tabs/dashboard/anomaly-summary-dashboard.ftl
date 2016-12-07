<div class="row bottom-line row-bordered">
	<div class="col-md-12">
		<div class="container top-buffer bottom-buffer">
			<div class="table-responsive">
				<table class="table dashboard-table" style="border-collapse: separate; border-spacing: 0em 1em">
					<thead>
						<tr>
							<th><div class="row-label">Sep 2nd, 2016</div></th>
							<th>8p</th>
							<th>9p</th>
							<th>10p</th>
							<th>10p</th>
							<th>10p</th>
							<th>10p</th>
							<th>10p</th>
							<th>10p</th>
							<th>10p</th>
							<th>10p</th>
							<th>10p</th>
							<th>10p</th>
							<th>10p</th>
							<th>10p</th>
							<th>10p</th>
							<th>10p</th>
							<th>10p</th>
							<th>10p</th>
							<th>10p</th>
							<th>10p</th>
							<th>10p</th>
							<th>10p</th>
							<th>10p</th>
							<th>8a</th>
						</tr>
					</thead>
					<tbody>
            {{#each this.anomalySummary.rows as |row rowIndex|}}
						<tr class="bg-white">
                <td><div>
                  <a href="#"><span class="metric-label">{{row.metricName}}</span></a>
                </div></td>
                {{#each row.data as |data dataIndex|}}
                  <td><div class="box" box-anomaly>{{data}}</div></td>
                {{/each}}
						</tr>
            {{/each}}
				</table>
			</div>
		</div>
	</div>
</div>


