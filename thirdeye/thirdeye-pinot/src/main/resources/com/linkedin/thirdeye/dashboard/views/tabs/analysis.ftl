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
					<div>
						<label class="label-medium-semibold">Select time ranges to compare: </label>
					</div>
					<div>
						<label class="label-small-semibold">DATE RANGE(CURRENT) </label>
					</div>

					<div id="current-range">
						<span></span><b class="caret"></b>
					</div>
					<div>
						<label class="label-small-semibold">COMPARE TO(BASELINE)</label>
					</div>
					<div id="baseline-range">
						<span></span> <b class="caret"></b>
					</div>
				</div>
				<div class="col-md-2">
					<div class="row">
						<label class="label-medium-semibold" for="granularity">Granularity </label>
					</div>
          <div class="row">
            <div style="width: 370px; float: left">
              <select id="analysis-granularity-input" class="label-large-light underlined"></select>
            </div>
          </div>
				</div>
				<div class="col-md-2">
					<div class="row">
						<label class="label-medium-semibold" for="add-dimension-button">Dimensions</label>
					</div>
					<div class="row">
            <div style="width: 370px; float: left">
              <select id="analysis-metric-dimension-input" style="width: 50%;" class="label-large-light underlined"></select>
            </div>
					</div>
				</div>
				<div class="col-md-3">
					<div>
						<label class="label-medium-semibold" for="add-filter-button">Filters </label>
					</div>
					<div>
                <select id="analysis-metric-filter-input" style="width: 110%;" class="label-large-light underlined"></select>
					</div>
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
