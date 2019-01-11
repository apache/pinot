<div class="row bottom-line row-bordered">
	<div class="col-md-12">
		<div class="container top-buffer bottom-buffer">
			{{#each this as |metricSummary metricSummaryIndex|}}
			<div class="metric-card-container">
				<div class="metric-card">
					<div class="metric-card-header" style="word-wrap: break-word;">
						<div>
							<span class="label-medium-light">{{this.metricName}}</span>
						</div>
					</div>
					<div class="metric-card-body" style="background-color:{{computeColor metricSummary.wowPercentageChange}}">
						<div class="metric-card-body-row-1">
							<div style="">
								<span class="label-large-light">{{formatPercent metricSummary.wowPercentageChange}}</span>
							</div>
						</div>
						<div class="metric-card-body-row-2">
							<span class="label-medium-light" title="No anomaly configured">{{computeAnomaliesString metricSummary.anomaliesSummary.numAnomalies}}</span>
						</div>
						<div class="metric-card-body-row-3">
							<span class="label-medium-light">{{abbreviateNumber metricSummary.currentValue}}({{abbreviateNumber (formatDelta metricSummary.currentValue metricSummary.baselineValue)}})</span>
						</div>
					</div>
				</div>
			</div>
			{{/each}}
		</div>
	</div>
</div>


