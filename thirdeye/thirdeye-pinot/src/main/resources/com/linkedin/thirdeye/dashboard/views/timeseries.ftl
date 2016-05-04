<section id="custom-funnel-section" class="hidden" style="margin: 0;">
	<script id="time-series-template" type="text/x-handlebars-template">
{{#each metrics as |metricName metricIndex|}}
      <br>Select Metric: <select>
	    <option value="{{metricName}}">{{metricName}}</option>
	</select>
{{/each}}

{{#with timeSeriesData/time}}
<div  class="title-box full-width">
    <table title="{{displayDate this.baselineUTC}}">
        <tbody>
        <tr>
            <th><b>Start:</b></th>
            <td class="baseline-date-time">{{displayDate  @first}}</td>
            <th><b>End:</b></th>
            <td class="baseline-date-time">{{displayDate  @last}}</td>
        </tr>
        </tbody>
    </table>
</div>
{{/with}}

<div id="time-series-area" class="uk-display-inline-block" style="display: inline-block; width:83%; height: 400px;">
</div>

<div class="timeseries-legend-box" style="display: inline-block">
    <label style="display: block;"><input class="time-series-select-all-checkbox" type="checkbox">Select All
    </label>
    <div id="timeseries-time-series-legend" class="timeseries-legend-sub-box uk-display-inline-block" style="width:250px;">
        {{#with timeSeriesData}}
        {{#each this as |Index label|}}

        {{#if_eq label "time"}}
        {{else}}
        <label class="legend-item" value="{{label}}">
            <table>
                <tr>
                    <td>
                        <input class="time-series-checkbox" type="checkbox" value="{{label}}" color="{{colorById @index @root/timeSeriesData}}">
                    </td>
                    <td>
                        <div class="legend-color uk-display-inline-block" style="width: 10px; height: 10px; background:{{colorById @index @root/timeSeriesData}}" color="{{colorById @index @root/timeSeriesData}}" ></div>
                    </td>
                    <td class="legend-label-value-td">
                        {{label}}
                    </td>
                </tr>
            </table>
        </label>
        {{/if_eq}}

        {{/each}}
        {{/with}}
    </div>
</div>




    </script>
</section>