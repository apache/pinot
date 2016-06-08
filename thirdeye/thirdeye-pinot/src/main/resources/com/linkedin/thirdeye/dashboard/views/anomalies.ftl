<section id="anomalies-section" class="hidden" style="margin: 0;">
    <script id="anomalies-template" type="text/x-handlebars-template">
        <table id="anomalies-table" class="uk-table">
            <thead>
            <tr>
                <!--Todo: checkbox
                <th></th> -->
                <th class="border-left">Anomaly ID</th>
                <th class="border-left">Metric</th>
                <th class="border-left">Start time</th>
                <th class="border-left">End time</th>
                <th class="border-left">Dataset</th>
				<th class="border-left">Function ID</th>
                <th class="border-left">Function type</th>
                <th class="border-left">Dimension</th>
            </tr>
            </thead>

            <!-- Table of values -->
            <tbody class="">
            {{#each this as |anomalyData anomalyIndex|}}
            <tr>
                <!--Todo:
                <td class="checkbox-cell"><input type="checkbox" value="{{anomalyData/metric}}"></td>-->
                <td class="border-left">{{anomalyData/id}}</td>
                <td class="metric-label border-left">{{anomalyData/metric}}</td>
                <td class="border-left">{{millisToDate anomalyData/startTimeUtc}}</td>
                <td class="border-left">{{millisToDate anomalyData/endTimeUtc}}</td>
                <td class="border-left">{{anomalyData/collection}}</td>
				<td class="border-left">{{anomalyData/functionId}}</td>
                <td class="border-left">{{anomalyData/functionType}}</td>
                <td class="border-left">{{anomalyData/dimensions}}</td>
                

            </tr>

            {{/each}}
            </tbody>
        </table>

    </script>
</section>