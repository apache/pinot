
<div class="container-fluid">
	<div class="row bg-white row-bordered">
		<div class="container top-buffer bottom-buffer ">
			<div class=row>
				<div class="col-md-12">
					<label for="metric-button" class="label-large-light">View Anomalies for:</label>
					<button type="button" class="btn btn-link label-medium-semibold" id="metric-button">+Metric</button>
				</div>
			</div>
		</div>
	</div>
</div>
<div class="container-fluid bg-white ">
	<div class="row bg-white row-bordered">
		<div class="container top-buffer bottom-buffer">
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
	</div>
</div>
<div class="container-fluid">
	<div class="row row-bordered">
		<div class="container top-buffer bottom-buffer">
			<div>
				Showing <label style="font-size: 15px; font-weight: 500"> 10 </label> anomalies of <label style="font-size: 15px; font-weight: 500"> 25</label>
			</div>
		</div>
		<div class="container">
			<div class="panel padding-all">
				<div class="row">
					<div class="col-md-3">
						<label>#123213 <a href="#">show details</a></label>
					</div>
					<div class="col-md-6"></div>
					<div class="col-md-1">
						<span class="pull-right">______</span>
					</div>
					<div class="col-md-2">
						<span class="pull-left"><label> Sep 2 - Sep 9</label></span>
					</div>
				</div>
				<div class="row">
					<div class="col-md-3">
						<label>num_login_attempt</label>
					</div>
					<div class="col-md-6"></div>
					<div class="col-md-1">
						<span class="pull-right">--------</span>
					</div>
					<div class="col-md-2">
						<span class="pull-left"><label> Aug 2 - Aug 9</label></span>
					</div>
				</div>
				<div class="row">
					<div class="col-md-12">
						<div id="anomaly-chart" style="height: 200px"></div>
					</div>
				</div>
				<div class="row">
					<div class="col-md-7">
						<div class="row">
							<div class="col-md-3">
								<span class="pull-left">Dimension</span>
							</div>
							<div class="col-md-9">
								<span class="pull-left">country_code:United States</span>
							</div>
						</div>
						<div class="row">
							<div class="col-md-3">Current</div>
							<div class="col-md-9">123</div>
						</div>
						<div class="row">
							<div class="col-md-3">Baseline</div>
							<div class="col-md-9">100</div>
						</div>
						<div class="row">
							<div class="col-md-3">Start-End (PDT)</div>
							<div class="col-md-9">Jan 03 - Jan 05</div>
						</div>
					</div>
					<div class="col-md-5">
						<label>Anomaly Function Details:</label><br /> <label>efs_country_code</label> <br /> <label>week_over_week_rule</label>
					</div>
				</div>
				<div class="row">
					<div class="col-md-3">
						<select data-placeholder="Provide Anomaly Feedback" style="width: 250px; border: 0px" class="chosen-select">
							<option>False Alarm</option>
							<option>Confirmed Anomaly</option>
							<option>Confirmed - Not Actionable</option>
						</select>
					</div>
					<div class="col-md-6"></div>
					<div class="col-md-3">
						<button type="button" class="btn btn-primary btn-sm">Root Cause Analysis</button>
					</div>
				</div>
			</div>
		</div>
	</div>
</div>