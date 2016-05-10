<section id="metric-timeseries-section" class="">
	<script id="metric-time-series-section-template" type="text/x-handlebars-template">
        <div class="clear-fix" style="clear: both">
            {{#with summary}}
            <div  class="title-box full-width">
                <table title="{{displayDate this.baselineUTC}}">
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

        <div id='linechart-placeholder' style='display: inline-block; width:83%; height: 300px; '></div>
        <div id='barchart-placeholder' style='display: inline-block; width:83%; height: 150px; '></div>
        <div class="timeseries-legend-box" style="display: inline-block">
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
	<script id="metric-time-series-section-anomaly-template" type="text/x-handlebars-template">

        {{#with summary}}
        <div  class="title-box full-width">
            <table title="{{displayDate this.baselineUTC}}">
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


        <div class="clear-fix" style="clear: both">
        <div id='anomaly-linechart-placeholder' style='display: inline-block; width:83%; height: 300px; '></div>
        <div class="timeseries-legend-box" style="display: inline-block">
            <label style="display: block;"><input class="anomalies-time-series-select-all-checkbox" type="checkbox">Select All
            </label>
            <div id="anomalies-time-series-legend" class="timeseries-legend-sub-box uk-display-inline-block" style="width:250px;">
                {{#each metrics as |metricName metricIndex|}}
                <label class="legend-item" value="{{metricName}}">
                    <table  data-uk-tooltip title="{{metricName}}">
                        <tr>
                            <td>
                                <input class="anomalies-time-series-checkbox" type="checkbox" value="{{metricName}}" color="{{colorById metricIndex ../metrics.length  name= metricName}}">
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
