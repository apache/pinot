<section id="contributors-section" width="100%">
	<script id="contributors-table-template" type="text/x-handlebars-template">

        {{#each metrics as |metricName metricIndex|}}
        <div class="metric-section-wrapper" rel="{{metricName}}">
            {{#each ../dimensions as |dimensionNames dimensionIndex|}}
            <div class="dimension-section-wrapper" rel="{{dimensionNames}}" metric="{{metricName}}" style="vertical-align: top; ">
                <p class="dimension-title hidden"> {{dimensionNames}}</p>
                <p class="dimension-title hidden"> {{dimensionNames}}</p>

                <h2 class="uk-margin-top"> Break down of metric: <b>{{metricName}}</b> by dimension: <b>{{dimensionNames}}</b><span class="filters-title"></span></h2>
                <div class="dimension-timeseries-section clear-fix uk-margin-large-bottom" dimension="{{dimensionNames}}" style="width: 100%; position: relative;">
                    {{#with @root/timeBuckets}}
                    <div  class="title-box full-width">
                        <table>
                            <tbody>
                            <tr>
                                <th>BASELINE</th>
                                <th><b>Start:</b></th>
                                {{#each this as |timeBucket timeBucketIndex|}}
                                {{#if @first}}
                                <td class="baseline-date-time">{{displayDate  timeBucket.baselineStart}}</td>
                                {{/if}}
                                {{/each}}

                                <th><b>End:</b></th>
                                {{#each this as |timeBucket timeBucketIndex|}}
                                {{#if @last}}
                                <td class="baseline-date-time">{{displayDate  timeBucket.baselineEnd}}</td>
                                {{/if}}
                                {{/each}}
                            </tr>
                            <tr>
                                <th>CURRENT</th>
                                <th><b>Start:</b></th>
                                {{#each this as |timeBucket timeBucketIndex|}}
                                {{#if @first}}
                                <td class="current-date-time">{{displayDate  timeBucket.currentStart}}</td>
                                {{/if}}
                                {{/each}}
                                <th><b>End:</b></th>
                                {{#each this as |timeBucket timeBucketIndex|}}
                                {{#if @last}}
                                <td class="current-date-time">{{displayDate timeBucket.currentEnd}}</td>
                                {{/if}}
                                {{/each}}
                            </tr>
                            </tbody>
                        </table>

                    </div>
                    {{/with}}
                    <div class="dimension-timeseries" id="contributor-timeseries-{{metricName}}-{{dimensionNames}}" class="uk-display-inline-block" style="display: inline-block; width:83%; height: 300px;"></div>
                    <div class="dimension-timeseries" id="contributor-percentChange-{{metricName}}-{{dimensionNames}}" class="uk-display-inline-block" style="display: inline-block; width:83%; height: 150px;"></div>
                    <div id="contributor-timeseries-legend-{{metricName}}-{{dimensionNames}}" class="timeseries-legend-box dimension-timeseries-legend" style="display: inline-block">
                        <svg class="line-legend" width="200" height="55">
                            <line x1="0" y1="15" x2="55" y2="15" stroke="gray" stroke-dasharray="5,5"/>
                            <text x="60" y="15" dy=".3em" style="text-anchor: start; font-family:Arial, sans-serif; font-size:0.75em">BASELINE</text>
                            <line x1="0" y1="30" x2="55" y2="30" stroke="gray"/>
                            <text x="60" y="30" dy=".3em" style="text-anchor: start; font-family:Arial, sans-serif; font-size:0.75em">CURRENT</text>
                        </svg>
                        <label style="display: block;"><input class="time-series-dimension-select-all-checkbox filter-select-all-checkbox" type="checkbox">Select All</label>
                        <div class="dimension-time-series-legend timeseries-legend-sub-box uk-display-inline-block" style="width:250px;">
                            {{#lookupDimValues @root/dimensionValuesMap dimName=dimensionNames}}
                            {{#each this as |dimValue dimensionValueIndex|}}
                            <label rel="{{metricName}}" dimension="{{dimensionNames}}" value="{{dimValue}}">
                                <input class="time-series-dimension-checkbox" type="checkbox" dimension="{{dimensionNames}}" metric="{{metricName}}" value="{{dimValue}}" color="{{colorByIdContributors dimensionValueIndex @root/dimensionValuesMap name=dimValue dimName= dimensionNames}}">
                                <div class="legend-color uk-display-inline-block" style="width: 10px; height: 10px; background:{{colorByIdContributors dimensionValueIndex @root/dimensionValuesMap name=dimValue dimName= dimensionNames}}" color="{{colorByIdContributors dimensionValueIndex @root/dimensionValuesMap name=dimValue dimName= dimensionNames}}">
                                </div>
                                {{displayDimensionValue dimValue}}
                            </label>
                            {{/each}}
                            {{/lookupDimValues}}
                        </div>
                    </div>
                </div>

	            <h2 class="uk-margin-top"> Break down of metric: <b>{{metricName}}</b> by dimension: <b>{{dimensionNames}}</b><span class="filters-title"></span></h2>

                <!-- Contributors table -->
                <!--Summary and details buttons -->
                <ul class="uk-display-inline-block uk-float-left">
                    <li id="sum-detail" class="uk-display-inline-block uk-button-group uk-margin" data-uk-button-radio>
                        <button class="radio-type-button uk-active">
                            Summary
                        </button>
                        <button class="radio-type-button">
                            Details
                        </button>
                    </li>
                    <li class="uk-display-inline-block uk-margin">
                        <br>
                        <input type="checkbox" class="cumulative">Cumulative</input>
                    </li>
                </ul>
				<div class="flipped-scroll-container">
                    <table id="contributors-view-{{metricName}}" class="uk-table contributors-table fixed-table-layout uk-margin-top discrete-values" cell-spacing="0" width="100%">
                        <thead>
                        <!-- Time row-->
                        <tr>
                            <!-- Hiding total feature till ratio metrics are handled: table-date cell should be colspan="2" when readding -->
                            <th class="contributors-table-date border-left" currentUTC="{{@root/timeBuckets.[0]/currentStart}}" baselineUTC="{{@root/timeBuckets.[0]/currentStart}}"  title="Baseline Start: {{millisToDate  @root/timeBuckets.[0]/baselineStart}}">Current Start: {{millisToDate @root/timeBuckets.[0]/currentStart}}
                            </th>
                            {{#each @root/timeBuckets as |timeBucket timeBucketIndex|}}
                            <th class="table-time-cell border-left" currentUTC="{{timeBucket/currentStart}}" baselineUTC="{{timeBucket/baselineStart}}" title="baseline: {{millisToDate timeBucket/baselineStart}}" colspan="1">{{timeBucket/currentStart}}
                            </th>
                            {{/each}}
                        </tr>
                        <!--Subheader-->
                        <tr class="subheader hidden">
                            <th class="divider-cell">{{dimensionNames}} values</th>
                            {{#each @root/timeBuckets}}
                            <th class="details-cell border-left hidden">Baseline</th>
                            <th class="details-cell hidden">Current</th>
                            <th class="">Ratio</th>
                            {{/each}}
                        </tr>

                        <!-- Dimension total row discrete values-->
                        <!-- Hiding total feature till ratio metrics are handled -->
                        <!--<tr class="discrete-values sum-row">
                            <th class="select_all_cell"><input class="select_all_checkbox" value="1" type="checkbox" rel="discrete"></th>
                            <th class="hidden"></th>
                            <th class="row-title"> Total:</th>
                            {{#each @root/timeBuckets}}
                            <th class="details-cell hidden"></th>
                            <th class="details-cell hidden"></th>
                            <th class="heat-map-cell"></th>
                            {{/each}}
                        </tr>-->

                        <!-- Divider row -->
                        <tr class="divider-row hidden">
                            <td colspan="5"><h3>{{dimensionNames}} values:</h3>
                            </td>
                        </tr>
                        </thead>
                       <tbody class="contributor-tabular-section">
                        <!-- Table row dimension  discrete values-->
                        {{#lookupDimValues @root/dimensionValuesMap dimName=dimensionNames}}
                            {{#each this as |dimensionValues dimensionValueIndex|}}
                            <tr class="data-row discrete-values">
                                <!-- Hiding total feature till ratio metrics are handled -->
                                <!--<td class="checkbox-cell"><input value="1" type="checkbox"></td>-->
                                <td class="dimension dimension-cell hidden">{{dimensionNames}}</td>
                                <td class="dimension-value-cell">{{dimensionValues}}</td>
                                {{#lookupRowIdList @root/responseData/keyToRowIdMapping metricName=metricName dimName=dimensionNames dimValue=dimensionValues}}
                                    {{#each this as |rowId rowIdIndex|}}
                                    <td class="{{classify 0}}" value="{{returnValue @root/responseData  key=rowId schemaItem='baselineValue'}}">{{returnValue @root/responseData  key=rowId schemaItem='baselineValue'}}</td>
                                    <td class="{{classify 1}}" value="{{returnValue @root/responseData  key=rowId schemaItem='currentValue'}}">{{returnValue @root/responseData  key=rowId schemaItem='currentValue'}}</td>
                                    <td class="{{classify 2}}" value="{{returnValue @root/responseData  key=rowId schemaItem='percentageChange'}}" data-metric-name="{{metricName}}" data-metric-index="{{metricIndex}}">{{returnValue @root/responseData  key=rowId schemaItem='percentageChange'}}</td>
                                    {{/each}}
                                {{/lookupRowIdList}}
                            </tr>
                            {{/each}}
                        {{/lookupDimValues}}

                        </tbody>
                    </table>
                    <!-- cumulative table-->
                    <table id="contributors-view-{{metricName}}" class="uk-table contributors-table fixed-table-layout uk-margin-top cumulative-values hidden" cell-spacing="0" width="100%">
                        <thead>
                        <!-- Time row-->
                        <tr>
                            <!-- Hiding total feature till ratio metrics are handled: table-date cell should be colspan="2" when readding -->
                            <th class="contributors-table-date border-left" currentUTC="{{@root/timeBuckets.[0]/currentStart}}" baselineUTC="{{@root/timeBuckets.[0]/currentStart}}" title="Baseline Start: {{millisToDate  @root/timeBuckets.[0]/baselineStart}}">Current Start: {{millisToDate @root/timeBuckets.[0]/currentStart}}
                            </th>
                            {{#each @root/timeBuckets as |timeBucket timeBucketIndex|}}
                            <th class="table-time-cell border-left" currentUTC="{{timeBucket/currentStart}}" title="{{timeBucket/baselineStart}}" colspan="1">{{timeBucket/currentStart}}
                            </th>
                            {{/each}}
                        </tr>
                        <!--Subheader-->
                        <tr class="subheader hidden">
                            <th class="divider-cell">{{dimensionNames}} values</th>
                            {{#each @root/timeBuckets}}
                            <th class="details-cell border-left hidden">Baseline</th>
                            <th class="details-cell hidden">Current</th>
                            <th class="">Ratio</th>
                            {{/each}}
                        </tr>

                        <!-- Dimension total row cumulative values-->
                        <!-- Hiding total feature till ratio metrics are handled -->
                        <!--<tr class="cumulative-values sum-row hidden">
                            <th class="select_all_cell"><input class="select_all_checkbox" value="1" type="checkbox" rel="cumulative"></th>
                            <th class="hidden"></th>
                            <th class="row-title"> Total:</th>
                            {{#each @root/timeBuckets}}
                            <th class="details-cell hidden">total cumulative value will come from JS</th>
                            <th class="details-cell hidden"></th>
                            <th class="heat-map-cell"></th>
                            {{/each}}
                        </tr>-->

                        <!-- Divider row -->
                        <tr class="divider-row hidden">
                            <td colspan="5"><h3>{{dimensionNames}} values:</h3>
                            </td>
                        </tr>
                        </thead>
                       <tbody class="contributor-tabular-section">

                        <!-- Table row dimension  cumulative values-->
                        {{#lookupDimValues @root/dimensionValuesMap dimName=dimensionNames}}
                            {{#each this as |dimensionValues dimensionValueIndex|}}
                            <tr class="data-row">
                                <!-- Hiding total feature till ratio metrics are handled -->
                                <!--<td class="checkbox-cell"><input value="1" type="checkbox"></td>-->
                                <td class="dimension dimension-cell hidden">{{dimensionNames}}</td>
                                <td class="dimension-value-cell">{{dimensionValues}}</td>
                                {{#lookupRowIdList @root/responseData/keyToRowIdMapping metricName=metricName dimName=dimensionNames dimValue=dimensionValues}}
                                    {{#each this as |rowId rowIdIndex|}}
                                    <td class="{{classify 0}}" value="{{returnValue @root/responseData  key=rowId schemaItem='cumulativeBaselineValue'}}">{{returnValue @root/responseData  key=rowId schemaItem = 'cumulativeBaselineValue'}}</td>
                                    <td class="{{classify 1}}" value="{{returnValue @root/responseData  key=rowId schemaItem='cumulativeCurrentValue'}}">{{returnValue @root/responseData  key= rowId schemaItem = "cumulativeCurrentValue"}}</td>
                                    <td class="{{classify 2}}" value="{{returnValue @root/responseData  key=rowId schemaItem='cumulativePercentageChange'}}" data-metric-name="{{metricName}}" data-metric-index="{{metricIndex}}">{{returnValue @root/responseData  key= rowId schemaItem = "cumulativePercentageChange"}}</td>
                                    {{/each}}
                                {{/lookupRowIdList}}
                            </tr>
                            {{/each}}
                        {{/lookupDimValues}}
                        </tbody>
                    </table>
                </div>
        </div>  <!-- end of dimension wrapper -->
            {{/each}}
    </div>  <!-- end of metric wrapper -->
        {{/each}}
    </script>
</section>
