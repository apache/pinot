
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
						<select style="width: 100%" id="anomalies-search-metrics-input" class="label-large-light" multiple="multiple"></select>
					</div>
					<div id="anomalies-search-dashboard-container" class=""  style="overflow:hidden; display: none;">
						<select style="width: 100%;" id="anomalies-search-dashboard-input" class="label-large-light"></select>
					</div>
					<div id="anomalies-search-anomaly-container" class=""  style="overflow:hidden; display: none;">
						<select style="width: 100%;" id="anomalies-search-anomaly-input" class="label-large-light" multiple="multiple"></select>
					</div>
          <div id="anomalies-search-time-container" class=""  style="overflow:hidden; display: none;">
            <select style="width: 100%;" id="anomalies-search-time-input" class="label-large-light"></select>
          </div>
				</div>

				<!-- <div class="search-button"> -->
	        <a class="btn thirdeye-btn search-button" type="button" id="search-button"><span class="glyphicon glyphicon-search"></span></a>
	      <!-- </div> -->

			</div>
		</div>
	</div>
</div>
<div class="container-fluid bg-white ">
	<div class="row bg-white row-bordered">
		<div class="container top-buffer bottom-buffer">
			<div class="row filter-bar">
				<div class="col-md-5">
					<div>
						<label>Select time range: </label>
					</div>
					<div>
						<label class="label-description">Date Range (current)</label>
					</div>
					<div id="anomalies-time-range" class="datepicker-range">
						<span></span> <b class="caret"></b>
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
				<!-- <div class="col-md-2">
					<div>
						<label style="font-size: 15px; font-weight: 500">Anomaly Status: </label>
					</div>
					<div>
						<label class="checkbox-inline"><input type="checkbox" id="status-resolved-checkbox"><span class="label anomaly-status-label">Resolved</span></label>
					</div>
					<div>
						<label class="checkbox-inline"><input type="checkbox" id="status-unresolved-checkbox"><span class="label anomaly-status-label">Unresolved</span></label>
					</div>
				</div> -->
				<div class="col-md-2 filter-apply-button" id="apply-button">
					<input type="button" class="btn thirdeye-btn" value="Apply" />
				</div>
			</div>
		</div>
	</div>
</div>


<div>
  <div id='anomaly-spin-area'></div>
	<div id="anomaly-results-place-holder"></div>
</div>
