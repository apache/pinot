<section id="time-series-section" class="hidden" style="margin: 0;">
	<script id="time-series-template" type="text/x-handlebars-template">
{{#each metrics as |metricName metricIndex|}}
      <br>Select Metric: <select>
	    <option value="{{metricName}}">{{metricName}}</option>
	</select>
{{/each}}

{{#with summary}}
<div  class="title-box full-width">
    <table title="{{displayDate this.baselineUTC}}">
        <tbody>
        <tr>
            <th><b>Start:</b></th>
            <td class="baseline-date-time">{{millisToDate currentStart}}</td>
            <th><b>End:</b></th>
            <td class="baseline-date-time">{{millisToDate currentEnd}}</td>
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
        {{#with keys}}
        {{#each this as |label Index|}}
        <label class="legend-item  {{hide_if_eq label 'time'}}" value="{{label}}">
            <table  data-uk-tooltip title="{{label}}">
                <tr>
                    <td>
                        <input class="time-series-checkbox" type="checkbox" value="{{label}}" color="{{colorById Index @root/keys.length  name= label}}">
                    </td>
                    <td>
                        <div class="legend-color uk-display-inline-block" style="width: 10px; height: 10px; background:{{colorById Index @root/keys.length  name= label}}" color="{{colorById Index @root/keys.length  name= label}}" ></div>
                    </td>
                    <td class="legend-label-value-td">
                        {{label}}
                    </td>
                </tr>
            </table>
        </label>

        {{/each}}
        {{/with}}
    </div>
</div>




    </script>
</section>