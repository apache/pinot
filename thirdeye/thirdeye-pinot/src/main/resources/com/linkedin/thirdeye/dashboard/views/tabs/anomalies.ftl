
<div class="container-fluid">
	<div class="row bg-white row-bordered">
		<div class="container top-buffer bottom-buffer ">
			<div class=row>
				<div class="col-md-12">
					<div style="float: left; width: auto">
						<label for="anomalies-metric-input" class="label-large-light">View Anomalies for: </label>
					</div>
					<div style="overflow: hidden">
						<select style="width: 100%" id="anomalies-metric-input" class="label-large-light underlined" multiple="multiple"></select>
					</div>
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
		{{#each this as |anomalyData anomalyIndex|}}
		<div class="container">
			<div class="panel padding-all">
				<div class="row">
					<div id="show-details-{{anomalyIndex}}" class="col-md-3">
						<label>{{this.anomalyId}}<a href="#"> show details</a></label>
					</div>
					<div class="col-md-6"></div>
					<div class="col-md-1">
						<span class="pull-right">______</span>
					</div>
					<div id="current-range-{{anomalyIndex}}" class="col-md-2">
						<span class="pull-left"><label></label></span>
					</div>
				</div>
				<div class="row">
					<div class="col-md-3">
						<label>{{this.metric}}</label>
					</div>
					<div class="col-md-6"></div>
					<div class="col-md-1">
						<span class="pull-right">--------</span>
					</div>
					<div id="baseline-range-{{anomalyIndex}}" class="col-md-2">
						<span class="pull-left"><label></label></span>
					</div>
				</div>
				<div class="row">
					<div class="col-md-12">
						<div id="anomaly-chart-{{anomalyIndex}}" style="height: 200px"></div>
					</div>
				</div>
				<div class="row">
					<div class="col-md-7">
						<div class="row">
							<div class="col-md-3">
								<span class="pull-left">Dimension</span>
							</div>
							<div id="dimension-{{anomalyIndex}}" class="col-md-9">
								<span class="pull-left"></span>
							</div>
						</div>
						<div class="row">
							<div class="col-md-3">Current</div>
							<div id="current-value-{{anomalyIndex}}" class="col-md-9"></div>
						</div>
						<div class="row">
							<div class="col-md-3">Baseline</div>
							<div id="baseline-value-{{anomalyIndex}}" class="col-md-9"></div>
						</div>
						<div class="row">
							<div class="col-md-3">Start-End (PDT)</div>
							<div id="region-{{anomalyIndex}}" class="col-md-9"></div>
						</div>
					</div>
					<div class="col-md-5">
						<label>Anomaly Function Details:</label><br /> <label>{{this.anomalyFunctionName}}</label> <br /> <label>{{this.anomalyFunctionType}}</label>
					</div>
				</div>
				<div class="row">
					<div id="anomaly-feedback-{{anomalyIndex}}" class="col-md-3">
						<select data-placeholder="Provide Anomaly Feedback" style="width: 250px; border: 0px" class="chosen-select">
							<option>False Alarm</option>
							<option>Confirmed Anomaly</option>
							<option>Confirmed - Not Actionable</option>
						</select>
					</div>
					<div class="col-md-6"></div>
					<div id="root-cause-analysis-button-{{anomalyIndex}}" class="col-md-3">
						<button type="button" class="btn btn-primary btn-sm">Root Cause Analysis</button>
					</div>
				</div>
			</div>
		</div>
		{{/each}}
	</div>
</div>