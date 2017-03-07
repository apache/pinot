
<div class="container-fluid">
	<div class="row bg-black row-bordered ">
		<div class="container top-buffer bottom-buffer">

			<div class="search-bar">
				<div class="search-select">
					<select id="anomalies-search-mode" style="width:100%">
						<option value="metric">Metric(s)</option>
						<option value="dashboard">Dashboard</option>
						<option value="id">Anomaly ID</option>
            <option value="time" selected>Time</option>
					</select>
				</div>

				<div class="search-input search-field">
					<div id="anomalies-search-metrics-container" class="" style="overflow:hidden; display: none;">
						<select id="anomalies-search-metrics-input" class="label-large-light" multiple="multiple"></select>
					</div>
					<div id="anomalies-search-dashboard-container" class=""  style="overflow:hidden; display: none;">
						<select id="anomalies-search-dashboard-input" class="label-large-light"></select>
					</div>
					<div id="anomalies-search-anomaly-container" class=""  style="overflow:hidden; display: none;">
						<select id="anomalies-search-anomaly-input" class="label-large-light" multiple="multiple"></select>
					</div>
          <div id="anomalies-search-time-container" class=""  style="overflow:hidden; display: none;">
            <select id="anomalies-search-time-input" class="label-large-light"></select>
          </div>
				</div>

        <a class="btn thirdeye-btn search-button" type="button" id="search-button"><span class="glyphicon glyphicon-search"></span></a>

			</div>
		</div>
	</div>
</div>
				<!-- Hidding this until fleshed out -->
				<!-- Jira: https://jira01.corp.linkedin.com:8443/browse/THIRDEYE-1043 -->
				<!-- <div class="col-md-4">
					<div>
						<label style="font-size: 15px; font-weight: 500">Filter by Function: </label>
					</div>
					<div>
						<select class="form-control" id="anomaly-function-dropdown">
						</select>
					</div>
				</div> -->

<div class="container top-buffer bottom-buffer">
  <div id='anomaly-spin-area'></div>
  <div class="page-content">
  	<div class="page-filter">
	  	<div class="anomalies-panel">
	  		<div class="filter-header">
	  			Filter
	  		</div>

	  		<div class="filter-body">
					<div id="#anomalies-time-range">
						<label class="label-medium-semibold">Start date</label>
						<div id="anomalies-time-range-start" class="datepicker-range">
							<span></span>
						</div>
						<label class="label-medium-semibold">End date</label>
						<div id="anomalies-time-range-end" class="datepicker-range">
							<span></span>
						</div>
					</div>
				</div>

				<div class="filter-footer">
					<a type="button" id="apply-button">Apply Filter</a>
				</div>
	  	</div>
  	</div>

		<div class="page-results" id="anomaly-results-place-holder"></div>
  </div>
</div>
