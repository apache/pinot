
<div class="container-fluid ">
	<div class="row bg-white row-bordered">

		<div class="container top-buffer bottom-buffer ">
			<label for="metric-button" class="label-large-light">Root Cause Analysis for:</label>
			<button type="button" class="btn btn-link" id="metric-button">+Metric</button>
		</div>
	</div>
</div>
<div class="container-fluid">
	<div class="row bg-white row-bordered">
		<div class="container top-buffer bottom-buffer">
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
			<div class="row">
				<div class="col-md-2 pull-left ">
					<span class="label-medium-semibold">Total % Changes</span>
				</div>
				<div class="col-md-2">
					<input type="checkbox" id="show-details"> <label for="show-details" class="metric-label">See Contribution Details</label>
				</div>
				<div class="col-md-2">
					<input type="checkbox" id="show-cumulative"> <label for="show-cumulative" class="metric-label">Show Cumulative</label>
				</div>
				<div class="col-md-6"></div>
			</div>
			<div class="row bottom-buffer">
				<div class="col-md-12">
					<span class="label-small-semibold">Click on a cell to drill down into its contribution breakdown.</span>
				</div>
			</div>
			<div id="wow-metric-table" class="row">
				<div class="col-md-12">
					<#include "analysis/wow-metric.ftl"/>
				</div>
			</div>

			<div id="wow-metric-dimension-table" class="row">
				<div class="col-md-12">
					<#include "analysis/wow-metric-dimension.ftl"/>
				</div>
			</div>
		</div>
	</div>
</div>

<div class="container-fluid">
	<div class="row row-bordered bg-white">
		<div class="container top-buffer bottom-buffer">
			<div class="row bottom-buffer">
				<div class="col-md-12">
					<div>
						<label>num_login_attempt</label> heatmap
					</div>
				</div>
			</div>
			<div class="row bottom-buffer">
				<div class="col-md-2">
					<label class="label-small-semibold pull-right">COMPARING</label>
				</div>
				<div class="col-md-3">
					<div id="heatmap-current-range">
						<i class="glyphicon glyphicon-calendar fa fa-calendar"></i>&nbsp; <span></span> <b class="caret"></b>
					</div>
				</div>
				<div class="col-md-1">
					<label class="label-small-semibold">TO</label>
				</div>
				<div class="col-md-3">
					<div id="heatmap-baseline-range">
						<i class="glyphicon glyphicon-calendar fa fa-calendar"></i>&nbsp; <span></span> <b class="caret"></b>
					</div>
				</div>
			</div>
			<hr />

			<div class="row bottom-buffer">
				<div class="col-md-12">
					<nav class="navbar navbar-transparent" role="navigation">
						<div class="collapse navbar-collapse">
							<ul class="nav navbar-nav dashboard-tabs" id="dashboard-tabs">
								<li class=""><a href="#num_of_anomalies" data-toggle="tab">% Change</a></li>
								<li class=""><a href="#wow" data-toggle="tab">Change in contribution</a></li>
								<li class=""><a href="#wow" data-toggle="tab">Contribution to overall change</a></li>
							</ul>
						</div>
					</nav>
				</div>
			</div>
			<div class="row bottom-buffer">
				<div class="col-md-12">
					<table class="table info-table">
						<thead>
							<tr>
								<th>CURRENT TOTAL(9/02 8pm - 9/03 8pm)</th>
								<th>BASELINE TOTAL(8/02 8pm - 8/03 8pm)</th>
								<th>CHANGE VALUE</th>
								<th>% CHANGE</th>
							</tr>
						</thead>
						<tbody>
							<tr>
								<td>50,000</td>
								<td>100,000</td>
								<td><p class="text-danger">-50,000</p></td>
								<td><p class="text-danger">-50%</p></td>
							</tr>
						</tbody>
					</table>
				</div>
			</div>
			<div class="row bottom-buffer">
				<div class="col-md-12">
					<div class="row">
						<table class="table table-borderless">
							<tbody>
								<tr>
									<td class="col-md-1"></td>
									<td class="col-md-11">
										<div id="axis-placeholder" style="height: 25px; width: 100%">
											
										</div>
									</td>
								</tr>

								<tr>
									<td class="col-md-1" style="vertical-align: middle">Browser</td>
									<td class="col-md-11">
										<div id="browser-heatmap-placeholder" style="height: 50px; width: 100%"></div>
									</td>
								</tr>
								<tr>
									<td class="col-md-1" style="vertical-align: middle">Country</td>
									<td class="col-md-11">
										<div id="country-heatmap-placeholder" style="height: 50px; width: 100%"></div>
									</td>
								</tr>
								<tr>
									<td class="col-md-1" style="vertical-align: middle">Device</td>
									<td class="col-md-11">
										<div id="device-heatmap-placeholder" style="height: 50px; width: 100%"></div>
									</td>
								</tr>
							</tbody>
						</table>
					</div>
				</div>
			</div>
		</div>
	</div>
</div>