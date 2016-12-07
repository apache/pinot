
<div class="container-fluid ">
	<div class="row bg-white row-bordered">
		<div class="container top-buffer bottom-buffer ">
			<div class=row>
				<div class="col-md-12">
					<label for="metric-button" class="label-large-light">Root Cause Analysis for:</label>
					<button type="button" class="btn btn-link label-medium-semibold" id="metric-button">+Metric</button>
				</div>
			</div>
		</div>
	</div>
</div>
<div class="container-fluid">
	<div class="row bg-white row-bordered">
		<div class="container top-buffer bottom-buffer">
			<div class=row>

				<div class="col-md-4">
					<div>
						<label class="label-medium-semibold">Select time ranges to compare: </label>
					</div>
					<div>
						<label class="label-small-semibold">DATE RANGE(CURRENT) </label>
					</div>

					<div id="current-range">
						<i class="glyphicon glyphicon-calendar fa fa-calendar"></i>&nbsp; <span></span> <b class="caret"></b>
					</div>
					<hr />
					<div>
						<label class="label-small-semibold">COMPARE TO(BASELINE)</label>
					</div>
					<div id="baseline-range">
						<i class="glyphicon glyphicon-calendar fa fa-calendar"></i>&nbsp; <span></span> <b class="caret"></b>
					</div>
					<hr>
				</div>
				<div class="col-md-2">
					<div class="form-group ">
						<label class="label-small-semibold" for="granularity">GRANULARITY </label> <select class="form-control selectAutoWidth" id="granularity">
							<option>DAYS</option>
							<option>HOURS</option>
							<option>MINUTES</option>
						</select>
					</div>
				</div>
				<div class="col-md-3">
					<div class="row">
						<label class="label-medium-semibold" for="add-dimension-button">Dimensions</label>
					</div>
					<div class="row">
						<span><a id="add-dimension-button">+ Add Dimension</a></span>
					</div>
				</div>
				<div class="col-md-3">
					<div class="row">
						<label class="label-medium-semibold" for="add-filter-button">Filters </label>
					</div>
					<div class="row">
						<a id="add-filter-button">+ Add Filter</a>
					</div>
				</div>
			</div>
		</div>
	</div>
</div>

<div class="container-fluid">
	<div class="row row-bordered">
		<div class="container top-buffer bottom-buffer">
			<div class="row bottom-buffer">
				<div class="col-md-12">
					<div>
						<label>num_login_attempt</label>
					</div>
					<div>
						3 anomalies detected for selected parameters <a href="">See Anomalies</a>
					</div>
				</div>
			</div>
			<!-- Chart section -->
			<div class="row">
				<div class="col-md-10">
					<div class="panel">
						<div id="analysis-chart" style="height: 400px"></div>
					</div>
				</div>
				<div class="col-md-2">
					<div id="analysis-chart-legend" style="height: 400px"></div>
				</div>
			</div>
			<!-- Percentage change table -->
			<div id="percentage-change-table-placeholder"></div>
		</div>
	</div>
</div>

<div class="container-fluid">
	<div class="row row-bordered bg-white">
		<div class="container top-buffer bottom-buffer">
			<div id="dimension-tree-map-placeholder"></div>
		</div>
	</div>
</div>
