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
		<div class="row">
			<div class="col-md-5">
			<label>Select time ranges to compare:</label>

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
		<div class="col-md-2">
			<label for="granularity">Granularity </label>
      <select id="analysis-granularity-input" style="width: 100%" class="label-large-light underlined"></select>
    </div>
		<div class="col-md-2">
			<label for="add-dimension-button">Dimensions</label>
      <select id="analysis-metric-dimension-input" style="width: 100%;" class="label-large-light underlined filter-select-field"></select>
		</div>
		<div class="col-md-3">
			<label for="add-filter-button">Filters </label>
      <select id="analysis-metric-filter-input" style="width: 100%;" class="label-large-light underlined"></select>
		</div>
		</div>
		<div id="timeseries-contributor-placeholder"></div>
	</div>
	<div class="analysis-card" id="dimension-tree-map-placeholder"></div>
</div>
