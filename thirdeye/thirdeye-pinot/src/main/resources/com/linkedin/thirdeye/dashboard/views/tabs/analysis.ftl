<div class="container-fluid ">
	<div class="row bg-white row-bordered">
		<div class="container top-buffer bottom-buffer ">
			<div class=row>
				<div class="col-md-8">
					<div style="float: left;">
						<label for="metric-input" class="label-large-light">Metric Name: </label>
					</div>
					<div style="width: 370px; float: left">
						<select style="width: 100%" id="analysis-metric-input" class="label-large-light underlined"></select>
					</div>
				</div>
        <div class="col-md-2">
          <input id="analysis-apply-button" type="button" class="btn btn-info" value="Apply">
        </div>
			</div>
		</div>
	</div>
</div>
<div class="container-fluid">
	<div class="row bg-white row-bordered">
		<div class="container top-buffer bottom-buffer">
			<div class=row>

				<div class="col-md-5">
					<label>Select time ranges to compare:</label>

					<div class="datepicker-field">
						<label class="label-description">DATE RANGE(CURRENT) </label>
						<div id="current-range" class="datepicker-range">
							<span></span><b class="caret"></b>
						</div>
					</div>

					<div class="datepicker-field">
						<label class="label-description">COMPARE TO(BASELINE)</label>
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
		</div>
	</div>
</div>

<div class="container-fluid">
	<div class="row row-bordered">
		<div class="container top-buffer bottom-buffer">
			<div id = "timeseries-contributor-placeholder"></div>
		</div>
	</div>
</div>

<div class="container-fluid">
	<div class="row row-bordered bg-white">
		<div class="container top-buffer bottom-buffer">
			<div id="dimension-tree-map-placeholder"></div>
		</div>
	</div>
</div>
