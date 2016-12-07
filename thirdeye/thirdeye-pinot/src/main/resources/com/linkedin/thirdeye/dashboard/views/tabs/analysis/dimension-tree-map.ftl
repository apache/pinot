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
							<div id="axis-placeholder" style="height: 25px; width: 100%"></div>
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