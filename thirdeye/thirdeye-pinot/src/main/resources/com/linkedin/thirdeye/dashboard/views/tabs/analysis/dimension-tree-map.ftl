<div class="analysis-title">Contribution Analysis:</div>
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
				<ul class="nav navbar-nav tree-map-tabs" id="dashboard-tabs">
					<li id="percent_change"><a href="#percent_change" data-toggle="tab">% Change</a></li>
					<li id="change_in_contribution"><a href="#change_in_contribution" data-toggle="tab">Change in contribution</a></li>
					<li id="contribution_to_overall_change"><a href="#contribution_to_overall_change" data-toggle="tab">Contribution to overall change</a></li>
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
					<th>CURRENT TOTAL({{this.currentStartTime}} - {{this.currentEndTime}})</th>
					<th>BASELINE TOTAL({{this.baselineStartTime}} - {{this.baselineEndTime}})</th>
					<th>CHANGE VALUE</th>
					<th>% CHANGE</th>
				</tr>
			</thead>
			<tbody>
				<tr>
					<td>{{this.currentTotal}}</td>
					<td>{{this.baselineTotal}}</td>
					<td><p class="text-danger">{{this.absoluteChange}}</p></td>
					<td><p class="text-danger">{{this.percentChange}} %</p></td>
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
					{{#each this.dimensions}}
					<tr>
						<td class="col-md-1" style="vertical-align: middle" class="label-medium-light">{{this}}</td>
						<td class="col-md-11">
							<div id="{{this}}-heatmap-placeholder" style="height: 50px; width: 100%"></div>
						</td>
					</tr>
					{{/each}}
				</tbody>
			</table>
		</div>
	</div>
</div>
<div id="tooltip" class="hidden">
	<p><strong id="heading"></strong></p>
	<p><span id="percentageChange"></span></p>
	<p><span id="currentValue"></span></p>
	<p><span id="baselineValue"></span></p>
</div>
