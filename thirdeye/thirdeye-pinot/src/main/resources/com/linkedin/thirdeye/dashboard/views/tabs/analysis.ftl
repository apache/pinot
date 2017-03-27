<div class="container-fluid ">
	<div class="row bg-white row-bordered">
		<div class="container top-buffer bottom-buffer ">
			<div class="search-bar">

				<div class="search-title">
					<label for="metric-input" class="label-large-light">Metric Name: </label>
				</div>

				<div class="search-input search-field">
					<select style="width:100%" id="analysis-metric-input" class="label-large-light underlined"></select>
				</div>

        <a class="btn thirdeye-btn search-button" type="button" id="analysis-apply-button"><span class="glyphicon glyphicon-search"></span></a>
      </div>
	</div>
</div>

<div class="container top-buffer bottom-buffer">
	<div class="analysis-card">
		<div class="analysis-title">Trend Analysis</div>
		<div class="analysis-options">
			<div class="analysis-options__datepicker">
			<div class="datepicker-field">
				<label class="label-medium-semibold">Date Range (Current) </label>
				<div id="current-range" class="datepicker-range">
					<span></span><b class="caret"></b>
				</div>
			</div>

			<div class="datepicker-field">
				<label class="label-medium-semibold">Compare To (Baseline)</label>
				<div id="baseline-range" class="datepicker-range">
					<span></span> <b class="caret"></b>
				</div>
			</div>

			</div>
			<div class="analysis-options__dropdown">
				<div class="analysis-options__granularity">
					<label for="granularity">Granularity </label>
		      <select id="analysis-granularity-input" style="width: 100%;" class="label-large-light"></select>
		    </div>
				<div class="analysis-options__dimensions">
					<label for="add-dimension-button">Dimensions</label>
		      <select id="analysis-metric-dimension-input" style="width: 100%;" class="label-large-light filter-select-field"></select>
				</div>
				<div class="analysis-options__filters">
					<label for="add-filter-button">Filters </label>
		      <select id="analysis-metric-filter-input" style="width: 100%;" class="label-large-light"></select>
				</div>
			</div>

			<div class="analysis-options__apply">
				<a class="btn thirdeye-btn" type="button"><span>Apply</span></a>
			</div>

		</div>
		<div id="timeseries-contributor-placeholder"></div>
	</div>
	<div class="analysis-card" id="dimension-tree-map-placeholder"></div>
</div>
