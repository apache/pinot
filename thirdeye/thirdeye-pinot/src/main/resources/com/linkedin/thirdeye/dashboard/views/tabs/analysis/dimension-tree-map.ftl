<div class="analysis-card padding-all top-buffer">
	<div class="analysis-title bottom-buffer">Contribution Analysis</div>
<!-- 	<div class="row bottom-buffer">
		<div class="col-xs-12">
			<div>
				<label>{{metricName}}</label> heatmap
			</div>
		</div>
	</div> -->

	<div class="contribution-analysis">
		<div class="contribution-analysis__daterangepicker">
			<label class="label-medium-semibold">Comparing:</label>
			<div class="datepicker-range" id="heatmap-current-range">
				<span></span>
				<b class="caret"></b>
			</div>
		</div>
		<div class="contribution-analysis__daterangepicker">
			<label class="label-medium-semibold">To:</label>
			<div class="datepicker-range" id="heatmap-baseline-range">
				<span></span>
				<b class="caret"></b>
			</div>
		</div>
	</div>

	<div class="row top-buffer bottom-buffer">
		<div class="col-xs-12">
			<nav class="navbar navbar-transparent" role="navigation">
				<div class="collapse navbar-collapse tree-map__nav">
					<ul class="nav navbar-nav tree-map-tabs" id="dashboard-tabs">
						<li class="tree-map__tab" id="percent_change">
							<a class="tree-map__link" href="#percent_change" data-toggle="tab">% Change</a>
						</li>
						<li class="tree-map__tab" id="change_in_contribution">
							<a class="tree-map__link" href="#change_in_contribution" data-toggle="tab">Change in contribution</a>
						</li>
						<li class="tree-map__tab" id="contribution_to_overall_change">
							<a class="tree-map__link" href="#contribution_to_overall_change" data-toggle="tab">Contribution to overall change</a>
						</li>
					</ul>
				</div>
			</nav>
		</div>
	</div>

	<div class="analysis-card analysis-summary bottom-buffer padding-all">
		<div class="analysis-summary__item">
			<label class="analysis-summary__label">Current Total</label>
			<span class="analysis-summary__data">{{formatNumber currentTotal}}</span>
		</div>
		<div class="analysis-summary__item">
			<label class="analysis-summary__label">Baseline Total</label>
			<span class="analysis-summary__data">{{formatNumber baselineTotal}}</span>
		</div>
		<div class="analysis-summary__item">
			<label class="analysis-summary__label">Change Value</label>
			<span class="analysis-summary__data {{colorDelta absoluteChange}}">{{formatNumber absoluteChange}}</span>
		</div>
		<div class="analysis-summary__item">
			<label class="analysis-summary__label">% Change</label>
			<span class="analysis-summary__data {{colorDelta percentChange}}">{{percentChange}} %</span>
		</div>
	</div>

	<!-- <div class="row bottom-buffer">
		<div class="col-xs-12">
			<table class="table info-table">
				<thead>
					<tr>
						<th>CURRENT TOTAL({{currentStartTime}} - {{currentEndTime}})</th>
						<th>BASELINE TOTAL({{baselineStartTime}} - {{baselineEndTime}})</th>
						<th>CHANGE VALUE</th>
						<th>% CHANGE</th>
					</tr>
				</thead>
				<tbody>
					<tr>
						<td>{{currentTotal}}</td>
						<td>{{baselineTotal}}</td>
						<td><p class="text-danger">{{absoluteChange}}</p></td>
						<td><p class="text-danger">{{percentChange}} %</p></td>
					</tr>
				</tbody>
			</table>
		</div>
	</div> -->

	<div class="row">
		<div class="col-xs-12">
			<table class="table table-borderless tree-map__table">
				<tbody>
					<tr>
						<td class="col-xs-1"></td>
						<td class="col-xs-11">
							<div id="axis-placeholder" style="height: 25px; width: 100%"></div>
						</td>
					</tr>
					{{#each dimensions}}
					<tr>
						<td class="col-xs-1" style="vertical-align: middle" class="label-medium-light">{{this}}</td>
						<td class="col-xs-11">
							<div id="{{this}}-heatmap-placeholder" style="height: 50px; width: 100%"></div>
						</td>
					</tr>
					{{/each}}
				</tbody>
			</table>
		</div>
	</div>
	<div id="tooltip" class="hidden">
		<p><strong id="heading"></strong></p>
		<p><span id="percentageChange"></span></p>
		<p><span id="currentValue"></span></p>
		<p><span id="baselineValue"></span></p>
	</div>
</div>
