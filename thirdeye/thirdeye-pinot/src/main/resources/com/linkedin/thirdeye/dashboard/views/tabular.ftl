<section id="custom-funnel-section" class="hidden" style="margin: 0;">
	<script id="funnels-table-template" type="text/x-handlebars-template">
    <br>
        <ul class="uk-display-inline-block uk-float-left">
        <li id="sum-detail" class="uk-display-inline-block uk-button-group uk-margin" data-uk-button-radio="">
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
	<div id="timebuckets"  class="hidden">
		{{#each timeBuckets as |timeBucket timeBucketIndex|}}
		<span>
			<span>{{timeBucket.currentStart}}</span>
			<span>{{timeBucket.currentEnd}}</span>
			<span>{{timeBucket.baselineStart}}</span>
			<span>{{timeBucket.baselineEnd}}</span>
		</span>
		{{/each}}
	</div>
	<div id="metrics"  class="hidden">
		{{#each metrics as |metricName metricIndex|}}
		  <span>{{metricName }}</span>	
		{{/each}}
	</div>

    <div class="flipped-scroll-container">
        <table id="funnels-table" class="uk-table dimension-view-overview-rendered">
            <thead>
                <tr>
                    <th class="border-left">Time</th>
                    {{#each timeBuckets as |timeBucket timeBucketIndex|}}
                    <th class="table-time-cell border-left" currentStartUTC="{{timeBucket.currentStart}}" colspan="1">this</th>
                    {{/each}}
                </tr>
                <!-- Subheader -->
                <tr  class="subheader hidden">
                    <th class="border-left"></th>
                    {{#each timeBuckets}}
                    <th class="border-left">Baseline</th>
                    <th>Current</th>
                    <th>Ratio</th>
                    {{/each}}
                </tr>

            </thead>

            <!-- Table of discrete values -->
            <tbody class="discrete-values">
                {{#each data as |metricData metricIndex|}}
                <tr class="data-row">
                    <td class="metric-label border-left" title="{{@key}}" style="width:145px;">{{@key}}</td>
                    {{#each responseData as |metricDataRows metricDataRowIndex|}}
                    <td class="{{classify 0}}" timeIndex={{metricDataRowIndex}} metricIndex={{metricIndex}} value="{{metricDataRows.[0]}}">{{displayRatioUnformattedData metricDataRows.[0] 0}}</td>
                    <td class="{{classify 1}}" timeIndex={{metricDataRowIndex}} metricIndex={{metricIndex}} value="{{metricDataRows.[1]}}">{{displayRatioUnformattedData metricDataRows.[1] 1}}</td>
                    <td class="{{classify 2}}" timeIndex={{metricDataRowIndex}} metricIndex={{metricIndex}} value="{{metricDataRows.[2]}}" title="Baseline Value: {{displayRatioUnformattedData metricDataRows.[0] 0}} Current Value: {{displayRatioUnformattedData metricDataRows.[1] 1}}">
                        {{displayRatioUnformattedData metricDataRows.[2] 2}}
                    </td>
                    {{/each}}
                </tr>
                {{/each}}
            </tbody>


            <!-- Table of cumulative values -->
            <tbody class="cumulative-values hidden">
                {{#each data as |metricData metricIndex|}}
                <tr class="data-row">
                    <td class="metric-label border-left" title="{{@key}}" style="width:145px;">{{@key}}</td>
                    {{#each responseData as |metricDataRows metricDataRowIndex|}}
                    <td class="{{classify 0}}" timeIndex={{metricDataRowIndex}} metricIndex={{metricIndex}} value="{{metricDataRows.[0]}}">{{displayRatioUnformattedData metricDataRows.[3] 0}}</td>
                    <td class="{{classify 1}}" timeIndex={{metricDataRowIndex}} metricIndex={{metricIndex}} value="{{metricDataRows.[1]}}">{{displayRatioUnformattedData metricDataRows.[4] 1}}</td>
                    <td class="{{classify 2}}" timeIndex={{metricDataRowIndex}} metricIndex={{metricIndex}} value="{{metricDataRows.[2]}}" title="Baseline Value: {{displayRatioUnformattedData metricDataRows.[0] 0}} Current Value: {{displayRatioUnformattedData metricDataRows.[1] 1}}">
                        {{displayRatioUnformattedData metricDataRows.[5] 2}}
                    </td>
                    {{/each}}
                </tr>
                {{/each}}
            </tbody>
        </table>
    </div>
    </script>
</section>