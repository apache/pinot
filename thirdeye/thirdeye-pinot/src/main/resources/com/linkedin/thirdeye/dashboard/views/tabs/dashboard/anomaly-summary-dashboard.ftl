<div class="row bottom-line row-bordered">
	<div class="col-md-12">
		<div class="container top-buffer bottom-buffer">
			<div class="table-responsive">
				<table class="table dashboard-table" style="border-collapse: separate; border-spacing: 0em 1em">
					<thead>
						<tr>
							<th></th> 
							{{#each timeRangeLabels as |label index|}}
							<th>{{label}}</th> 
							{{/each}}
						</tr>
					</thead>
					<tbody>
					   {{#each anomalySummaryList as |anomalySummary metricIndex|}}
						<tr class="bg-white">
							<td><a href="#"><span class="dashboard-metric-label">{{anomalySummary.metricName}}</span></a></td> 
							{{#each anomalySummary.data as |info index|}}
							{{#if_eq info.open '0'}}
							<td style="background-color:rgba(124, 184, 47, 0.1)">
							{{else}}
							<td>
							{{/if_eq}}
							{{#if_no_anomalies info}}
							   No Anomalies
							{{else}}
							<div class="td-box-left">
									<span class="glyphicon glyphicon-ok" style="color: #7CB82F" aria-hidden="true"></span> 
									<span aria-hidden="true">{{info.open}}</span>
								</div>
								<div class="td-box-right">
									<span class="glyphicon glyphicon-remove" style="color: #DD2E1F" aria-hidden="true"></span> 
									<span aria-hidden="true">{{info.resolved}}</span>
								</div>
							{{/if_no_anomalies}}
							</td> 
							{{/each}}
						</tr>
						{{/each}}
					</tbody>
				</table>
			</div>
		</div>
	</div>
</div>


