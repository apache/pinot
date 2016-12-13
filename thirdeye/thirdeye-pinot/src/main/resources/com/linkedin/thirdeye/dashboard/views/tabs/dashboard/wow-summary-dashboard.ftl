<div class="row bottom-line row-bordered">
	<div class="col-md-12">
		<div class="container top-buffer bottom-buffer">
			<div class="table-responsive">
				<table class="table dashboard-table" style="border-collapse: separate; border-spacing: 0em 1em">
					<thead>
						<tr>
							<th class="col-md-4"></th> {{#each timeRangeLabels as |label index|}}
							<th class="col-md-2">{{label}}</th> {{/each}}
						</tr>
					</thead>
					<tbody>
						{{#each wowSummaryList as |wowSummary metricIndex|}}
						<tr class="bg-white">
							<td class="col-md-4"><a href="#"><span class="dashboard-metric-label">{{wowSummary.metricName}}</span></a></td>
							{{#each wowSummary.data as |info index|}}
							<td class="col-md-2" style="background-color:{{computeColor info.percentChange}};">
							  <span class="label wow-summary-content" style="color:{{computeTextColor info.percentChange}}">{{info.current}}</span>
							  <span class="label wow-summary-content" style="color:{{computeTextColor info.percentChange}}">({{info.percentChange}}%)</span>
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


