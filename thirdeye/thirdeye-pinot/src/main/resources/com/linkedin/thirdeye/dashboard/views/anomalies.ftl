<section id="anomalies-section" class="hidden" style="margin: 0;">
    <script id="anomalies-template" type="text/x-handlebars-template">

        <div class="title-box full-width">
        {{#each this as |anomalyData anomalyIndex|}}
        {{#if @first}}
            <p class="uk-margin-top">Anomalies in <b>{{anomalyData/collection}}</b> dataset <b>{{anomalyData/metric}}</b> metric</p>
        {{/if}}
        {{/each}}
        </div>
        <table id="anomalies-table" class="uk-table">
            <thead>
            <tr>
                <th class="select_all_cell"><input class="select-all-checkbox" value="1" type="checkbox" rel="anomalies" checked>ID</th>
                <!--{{!--<th class="border-left">Metric</th>--}}-->
                <th class="border-left">Start / End ({{returnUserTimeZone}})</th>
                <th class="border-left">Alert reason</th>
				<!--<th class="border-left">Function ID</th>-->
                <!--{{!--<th class="border-left">Function type</th>--}}-->
                <th class="border-left">Dimension</th>
                <th class="border-left"><p>See heatmap</p><p>of timerange</p></th>
                <th class="border-left">Is this an anomaly?</th>
            </tr>
            </thead>

            <!-- Table of values -->
            <tbody class="">
            {{#each this as |anomalyData anomalyIndex|}}
            <tr>
                <td class="checkbox-cell"><label class="anomaly-table-checkbox"><input type="checkbox" data-value="{{anomalyData/metric}}" id="{{anomalyData/id}}" checked><div class="color-box uk-display-inline-block" style="background:{{colorById anomalyIndex @root.length}}">
                </div></label>
                {{anomalyData/id}}</td>
                <!--{{!--<td class="border-left">{{anomalyData/metric}}</td>--}}-->
                <td class="border-left">
                    <p>{{millisToDate anomalyData/startTimeUtc showTimeZone=false}} </p>
                    <p> {{millisToDate anomalyData/endTimeUtc showTimeZone=false}}</p>
                </td>
                <td class="border-left">{{anomalyData/message}}</td>
				<!--{{!--<td class="border-left">{{anomalyData/functionId}}</td>--}}-->
                <!--{{!--<td class="border-left">{{anomalyData/functionType}}</td>--}}-->
                <td class="border-left">{{anomalyData/dimensions}}</td>
                <td class="border-left">
                    <a class="heatmap-link" href="#" data-start-utc-millis="{{anomalyData/startTimeUtc}}" data-end-utc-millis="{{anomalyData/endTimeUtc}}"  data-metric="{{anomalyData/metric}}"><i class="uk-icon-eye"></i>
                    </a>
                </td>
                <td class="border-left">
                    <div class="uk-button-group" data-uk-button-radio>
                        <button class="uk-button radio-type-button" type="button" value="true">Yes</button>
                        <button class="uk-button radio-type-button" type="button" value="false">No</button>
                    </div>
                </td>
            </tr>

            {{/each}}
            </tbody>
        </table>

        <div id="anomaly-table-tooltip" class="hidden">
            <table>
            </table>
        </div>

    </script>
</section>