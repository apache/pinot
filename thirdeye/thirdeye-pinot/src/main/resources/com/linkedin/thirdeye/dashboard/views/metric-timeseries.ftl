<section id="metric-timeseries-section" class="">
	<script id="metric-time-series-section-template" type="text/x-handlebars-template">
        <div class="clear-fix" style="clear: both">
            {{#with summary}}
            <div  class="title-box full-width">
                <table>
                    <tbody>
                    <tr>
                        <th>BASELINE</th>
                        <th><b>Start:</b></th>
                        <td class="baseline-date-time">{{displayDate  baselineStart}}</td>
                        <th><b>End:</b></th>
                        <td class="baseline-date-time">{{displayDate  baselineEnd}}</td>
                    </tr>
                    <tr>
                        <th>CURRENT</th>
                        <th><b>Start:</b></th>
                        <td class="current-date-time">{{displayDate  currentStart}}</td>
                        <th><b>End:</b></th>
                        <td class="current-date-time">{{displayDate currentEnd}}</td>
                    </tr>
                    </tbody>
                </table>
            </div>
            {{/with}}

        <div id='linechart-placeholder'></div>
        <div id='barchart-placeholder'></div>
        <div class="timeseries-legend-box" style="display: inline-block">
            <svg class="line-legend" width="200" height="55">
                <line x1="0" y1="15" x2="55" y2="15" stroke="gray" stroke-dasharray="5,5"/>
                <text x="60" y="15" dy=".3em" style="text-anchor: start; font-family:Arial, sans-serif; font-size:0.75em">BASELINE</text>
                <line x1="0" y1="30" x2="55" y2="30" stroke="gray"/>
                <text x="60" y="30" dy=".3em" style="text-anchor: start; font-family:Arial, sans-serif; font-size:0.75em">CURRENT</text>
            </svg>
            <label style="display: block;"><input class="time-series-metric-select-all-checkbox" type="checkbox">Select All
            </label>
            <div id="metric-time-series-legend" class="timeseries-legend-sub-box uk-display-inline-block" style="width:250px;">
                {{#each metrics as |metricName metricIndex|}}
                <label class="legend-item" value="{{metricName}}">
                    <table  data-uk-tooltip title="{{metricName}}">
                        <tr>
                            <td>
                                <input class="time-series-metric-checkbox" type="checkbox" value="{{metricName}}" color="{{colorById metricIndex ../metrics.length  name= metricName}}">
                            </td>
                            <td>
                                <div class="legend-color uk-display-inline-block" style="width: 10px; height: 10px; background:{{colorById metricIndex ../metrics.length  name= metricName}}" color="{{colorById metricIndex ../metrics.length  name= metricName}}" ></div>
                            </td>
                            <td class="legend-label-value-td">
                                {{metricName}}
                            </td>
                        </tr>
                    </table>
                </label>
                {{/each}}
            </div>
        </div>

        </div>

        </script>
</section>
