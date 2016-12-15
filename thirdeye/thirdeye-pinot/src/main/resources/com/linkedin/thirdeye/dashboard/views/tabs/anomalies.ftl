
<div class="container-fluid">
	<div class="row bg-white">
		<div class="container">
			<div class="col-md-12">
				<nav class="navbar navbar-transparent" role="navigation">
					<div class="navbar-header">
						<label class="label-medium-semibold" style="padding-top: 15px; padding-bottom: 15px">Search anomalies by:</label>
					</div>
					<div class="collapse navbar-collapse">
						<ul id="anomalies-search-tabs" class="nav navbar-nav">
							<li class=""><a href="#anomalies_search-by-metric" data-toggle="tab">Metrics</a></li>
							<li class=""><a href="#anomalies_search-by-dashboard" data-toggle="tab">Dashboard</a></li>
							<li class=""><a href="#anomalies_search-by-id" data-toggle="tab">ID</a></li>
						</ul>
					</div>
				</nav>
			</div>
		</div>
	</div>
</div>
<div class="container-fluid bg-white ">
	<div class="row bg-white row-bordered">
		<div class="container top-buffer bottom-buffer">
			<div class="tab-content">
				<div class="tab-pane" id="anomalies_search-by-metric">
					<div class=row>
						<div class="col-md-12">
							<div style="float: left; width: auto">
								<label for="anomalies-metric-input" class="label-large-light">Metric(s): </label>
							</div>
							<div style="overflow: hidden">
								<select style="width: 100%" id="anomalies-metric-input" class="label-large-light" multiple="multiple"></select>
							</div>
						</div>
					</div>
				</div>
				<div class="tab-pane" id="anomalies_search-by-dashboard">
					<div class=row>
						<div class="col-md-12">
							<div style="float: left; width: auto">
								<label for="anomalies-dashboard-input" class="label-large-light">Dashboard: </label>
							</div>
							<div style="overflow: hidden">
								<select style="width: 100%" id="anomalies-dashboard-input" class="label-large-light"></select>
							</div>
						</div>
					</div>
				</div>
				<div class="tab-pane" id="anomalies_search-by-id">
					<div class=row>
						<div class="col-md-12">
							<div style="float: left; width: auto">
								<label for="anomalies-id-input" class="label-large-light">ID(s): </label>
							</div>
							<div style="overflow: hidden">
								<select style="width: 100%" id="anomalies-id-input" class="label-large-light" multiple="multiple"></select>
							</div>
						</div>
					</div>
				</div>
			</div>
		</div>
	</div>
</div>
<div class="container-fluid bg-white ">
	<div class="row bg-white row-bordered">
		<div class="container top-buffer bottom-buffer">
			<div class="col-md-4">
				<div>
					<label style="font-size: 15px; font-weight: 500">Select time range: </label>
				</div>
				<div>
					<label style="font-size: 11px; font-weight: 500">DATE RANGE(CURRENT) </label>
				</div>
				<div id="anomalies-time-range">
					<i class="glyphicon glyphicon-calendar fa fa-calendar"></i>&nbsp; <span></span> <b class="caret"></b>
				</div>
			</div>
			<div class="col-md-4">
				<div>
					<label style="font-size: 15px; font-weight: 500">Filter by Function: </label>
				</div>
				<div>
					<select class="form-control" id="anomaly-function-dropdown">
					</select>
				</div>
			</div>
			<div class="col-md-2">
				<div>
					<label style="font-size: 15px; font-weight: 500">Anomaly Status: </label>
				</div>
				<div>
					<label class="checkbox-inline"><input type="checkbox" id="status-resolved-checkbox"><span class="label anomaly-status-label">Resolved</span></label>
				</div>
				<div>
					<label class="checkbox-inline"><input type="checkbox" id="status-unresolved-checkbox"><span class="label anomaly-status-label">Unresolved</span></label>
				</div>
			</div>
			<div class="col-md-2" id="apply-button">
				<input type="button" class="btn btn-info" value="Apply" />
			</div>
		</div>
	</div>
</div>


<div>
	<div id="anomaly-results-place-holder"></div>
</div>
