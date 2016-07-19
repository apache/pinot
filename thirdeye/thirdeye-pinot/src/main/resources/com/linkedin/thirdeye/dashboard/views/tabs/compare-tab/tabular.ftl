<section id="metrics-section" class="hidden" style="margin: 0;">
	<script id="funnels-table-template" type="text/x-handlebars-template">
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

    <div id="funnel-table-msg" class="tip-to-user uk-alert">
        <i class="close-parent uk-icon-close"></i>
        <span>In the table below click on the cell to explore how various dimensions contributed to the change in the metric value for a specific time period.<br/>
        Click on the metric name to view the w-o-w difference trend on various dimension values.</span>
    </div>

    <div class="flipped-scroll-container">
        <table id="funnels-table" class="uk-table dimension-view-overview-rendered">
            <thead>
                <tr>
                    <th class="border-left">Time</th>
                    <th class="dropdown-column"></th>
                    {{#each timeBuckets as |timeBucket timeBucketIndex|}}
                    <th class="table-time-cell border-left" currentStartUTC="{{timeBucket.currentStart}}" colspan="1" data-uk-tooltip title="{{millisToDate  timeBucket.currentStart}}">{{millisToDateTimeInAggregate  timeBucket.currentStart}}</th>
                    {{/each}}
                </tr>
                <!-- Subheader -->
                <tr  class="subheader hidden">
                    <th class="border-left"></th>
                    <th class="dropdown-column"></th>
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
                    <td class="dropdown-column" style="position: relative"><button class="funnels-dimension-selector-btn uk-button" style="width:100%"  data-metric="{{@key}}"><i class="uk-icon-caret-down"></i></button></td>

                    {{#each responseData as |metricDataRows metricDataRowIndex|}}
                    <td class="{{classify 0}}" timeIndex={{metricDataRowIndex}} data-metric-name={{metricIndex}} metricIndex={{metricIndex}} value="{{metricDataRows.[0]}}">{{displayRatio metricDataRows.[0] 0}}</td>
                    <td class="{{classify 1}}" timeIndex={{metricDataRowIndex}} data-metric-name={{metricIndex}} metricIndex={{metricIndex}} value="{{metricDataRows.[1]}}">{{displayRatio metricDataRows.[1] 1}}</td>
                    <td class="{{classify 2}}" timeIndex={{metricDataRowIndex}} data-metric-name={{metricIndex}} metricIndex={{metricIndex}} value="{{metricDataRows.[2]}}" title="Baseline Value: {{displayRatio metricDataRows.[0] 0}} Current Value: {{displayRatio metricDataRows.[1] 1}}">
                        {{displayRatio metricDataRows.[2] 2}}
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
                    <td class="dropdown-column" style="position: relative"><button class="funnels-dimension-selector-btn uk-button" style="width:100%"  data-metric="{{@key}}"><i class="uk-icon-caret-down"></i></button></td>

                    {{#each responseData as |metricDataRows metricDataRowIndex|}}
                    <td class="{{classify 0}}" timeIndex={{metricDataRowIndex}} data-metric-name={{metricIndex}} metricIndex={{metricIndex}} value="{{metricDataRows.[0]}}">{{displayRatio metricDataRows.[3] 0}}</td>
                    <td class="{{classify 1}}" timeIndex={{metricDataRowIndex}} data-metric-name={{metricIndex}} metricIndex={{metricIndex}} value="{{metricDataRows.[1]}}">{{displayRatio metricDataRows.[4] 1}}</td>
                    <td class="{{classify 2}}" timeIndex={{metricDataRowIndex}} data-metric-name={{metricIndex}} metricIndex={{metricIndex}} value="{{metricDataRows.[2]}}" title="Baseline Value: {{displayRatio metricDataRows.[0] 0}} Current Value: {{displayRatio metricDataRows.[1] 1}}">
                        {{displayRatio metricDataRows.[5] 2}}
                    </td>
                    {{/each}}
                </tr>
                {{/each}}
            </tbody>
        </table>
    </div>
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
        <span>{{metricName}}</span>
        {{/each}}
    </div>

    <div class="funnels-table-dimension-box hidden">
        <i class="close-parent uk-icon-close"></i>
        <ul  class="funnels-table-dimension-list">
        </ul>
    </div>
    </script>
</section>