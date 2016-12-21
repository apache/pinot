<div class="row bottom-line row-bordered">
	<div class="col-md-12">
		<div class="container top-buffer bottom-buffer">
			{{#each this as |metricSummary metricSummaryIndex|}}
			<div class="metric-card-container">
				<div class="metric-card">
					<div class="metric-card-header">
						<div>
							<span class="label-medium-light">{{this.metricName}}</span>
						</div>
					</div>
					<div class="metric-card-body">
						<div class="metric-card-body-row-1">
							<div style="height: 100%; display: table-cell; vertical-align: middle; text-align: center" data-toggle="tooltip" data-placement="bottom">
								<span class="label-large-light">{{metricSummary.wowPercentageChange}}</span>
							</div>
						</div>
						<div class="metric-card-body-row-2">
							<span class="label-medium-light" title="No anomaly configured">{{computeAnomaliesString metricSummary.anomaliesSummary.numAnomalies}}</span>
						</div>
						<div class="metric-card-body-row-3">
							<span class="label-medium-light">{{metricSummary.currentValue}}(-15%)</span>
						</div>
					</div>
				</div>
			</div>
			{{/each}}
		</div>
	</div>
</div>


