<div class="row bottom-line row-bordered">
	<div class="col-md-12">
		<div class="container top-buffer bottom-buffer">
			{{#each this.metrics}}
			<div class="metric-card-container">
				<div class="metric-card">
					<div class="metric-card-header">
						<div>
							<span class="label-medium-light">Metric A</span>
						</div>
					</div>
					<div class="metric-card-body">
						<div class="metric-card-body-row-1">
							<div style="height: 100%; display: table-cell; vertical-align: middle; text-align: center" data-toggle="tooltip" data-placement="bottom" title="Hooray!">
								<span class="label-large-light">+ 15%</span>
							</div>
						</div>
						<div class="metric-card-body-row-2">
							<span class="label-medium-light" title="No anomaly configured">1 anomalies</span>
						</div>
						<div class="metric-card-body-row-3">
							<span class="label-medium-light">15000(-15%)</span>
						</div>
					</div>
				</div>
			</div>
			{{/each}}
		</div>
	</div>
</div>


