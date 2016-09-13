<section id="dimension-heat-map-section">
    <script id="heatmap-summary-template" type="text/x-handlebars-template">
        {{#with @root/summaryData}}

            <table id="heat-map-{{metricName}}-difference-summary-table" style="width:100%;">


                <thead>
                <tr>
                    <th colspan="{{dimensions.length}}">Dimension</th>
                    <th></th>
                    <th></th>
                    <th></th>
                    <th></th>
                    <th></th>
                </tr>
                <tr>
                    {{#with @root/summaryData/dimensions}}
                    {{#each this as |dimensionName dimensionIndex|}}
                    <th>{{dimensionName}}</th>
                    {{/each}}
                    {{/with}}
                    <th class="summary-header">Baseline</th>
                    <th class="summary-header">Current</th>
                    <th class="summary-header thin-column">Percentage Change</th>
                    <th class="summary-header thin-column">Contribution Change</th>
                    <th class="summary-header thin-column">Contribution To Overall Change</th>
                </tr>
                </thead>
                {{#with @root/summaryData/responseRows}}
                <tbody>
                {{#each this as |row rowIndex|}}
                <tr>
                    {{#each row.names as |dimensionValue dimension|}}
                    <td style="background-color: rgba(222, 222, 222, 0.5);">{{dimensionValue}}</td>
                    {{/each}}
                    <td align="right">{{row.baselineValue}}</td>
                    <td align="right">{{row.currentValue}}</td>
                    <td align="right">{{row.percentageChange}}</td>
                    <td align="right">{{row.contributionChange}}</td>
                    <td align="right">{{row.contributionToOverallChange}}</td>
                </tr>
                {{/each}}
                {{/with}}<!--end of summaryData -->
                </tbody>
            </table>
        {{/with}}<!--end of summaryData scope-->
    </script>
</section>