<section id="anomalies-section" class="hidden" style="margin: 0;">
    <script id="anomalies-template" type="text/x-handlebars-template">
        <table id="anomalies-table" class="uk-table">
            <thead>
            <tr>
                <th class="select_all_cell"><input class="select-all-checkbox" value="1" type="checkbox" rel="anomalies" checked>Select All</th>
                <th class="border-left">Anomaly ID</th>
                <th class="border-left">Metric</th>
                <th class="border-left">Start time</th>
                <th class="border-left">End time</th>
                <th class="border-left">Dataset</th>
                <th class="border-left">Alert reason</th>
				<!--<th class="border-left">Function ID</th>-->
                <th class="border-left">Function type</th>
                <th class="border-left">Dimension</th>
            </tr>
            </thead>

            <!-- Table of values -->
            <tbody class="">
            {{#each this as |anomalyData anomalyIndex|}}
            <tr>
                <td class="checkbox-cell"><label class="anomaly-table-checkbox"><input type="checkbox" data-value="{{anomalyData/metric}}" id="{{anomalyData/id}}" checked><div class="uk-display-inline-block" style="width: 10px; height: 10px; background:{{colorById anomalyIndex @root.length}}"></div></label></td>
                <td class="border-left">{{anomalyData/id}}</td>
                <td class="border-left">{{anomalyData/metric}}</td>
                <td class="border-left">{{millisToDate anomalyData/startTimeUtc}}</td>
                <td class="border-left">{{millisToDate anomalyData/endTimeUtc}}</td>
                <td class="border-left">{{anomalyData/collection}}</td>
                <td class="border-left">{{anomalyData/message}}</td>
				<!--{{!--<td class="border-left">{{anomalyData/functionId}}</td>--}}-->
                <td class="border-left">{{anomalyData/functionType}}</td>
                <td class="border-left">{{anomalyData/dimensions}}</td>
            </tr>

            {{/each}}
            </tbody>
        </table>

    </script>
</section>