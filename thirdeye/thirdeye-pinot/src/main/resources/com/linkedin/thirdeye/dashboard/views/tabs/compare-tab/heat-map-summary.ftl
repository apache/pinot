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
            <table id="heat-map-{{metricName}}-gainer-summary-table" style="width:100%;">
              <thead>
              <tr>
                <th colspan="{{dimensions.length}}">Top {{gainer.length}} Gainer</th>
                <th></th>
                <th></th>
                <th></th>
                <th></th>
                <th></th>
              </tr>
              <tr>
                <th class="summary-header">Dimension</th>
                <th class="summary-header"> </th>
                <th class="summary-header">Baseline</th>
                <th class="summary-header">Current</th>
                <th class="summary-header thin-column">Percentage Change</th>
                <th class="summary-header thin-column">Contribution Change</th>
                <th class="summary-header thin-column">Contribution To Overall Change</th>
                <th class="summary-header thin-column">Score</th>
              </tr>
              </thead>
              {{#with @root/summaryData/gainer}}
              <tbody>
              {{#each this as |row rowIndex|}}
              <tr>
                <td style="background-color: rgba(222, 222, 222, 0.5);">{{row.dimensionName}}</td>
                <td style="background-color: rgba(222, 222, 222, 0.5);">{{row.dimensionValue}}</td>
                <td align="right">{{row.baselineValue}}</td>
                <td align="right">{{row.currentValue}}</td>
                <td align="right">{{row.percentageChange}}</td>
                <td align="right">{{row.contributionChange}}</td>
                <td align="right">{{row.contributionToOverallChange}}</td>
                <td align="right">{{row.cost}}</td>
              </tr>
              {{/each}}
              {{/with}}<!--end of summaryData -->
              </tbody>
            </table>
            <table id="heat-map-{{metricName}}-loser-summary-table" style="width:100%;">
              <thead>
              <tr>
                <th colspan="{{dimensions.length}}">Top {{loser.length}} Loser</th>
                <th></th>
                <th></th>
                <th></th>
                <th></th>
                <th></th>
              </tr>
              <tr>
                <th class="summary-header">Dimension</th>
                <th class="summary-header"> </th>
                <th class="summary-header">Baseline</th>
                <th class="summary-header">Current</th>
                <th class="summary-header thin-column">Percentage Change</th>
                <th class="summary-header thin-column">Contribution Change</th>
                <th class="summary-header thin-column">Contribution To Overall Change</th>
                <th class="summary-header thin-column">Score</th>
              </tr>
              </thead>
              {{#with @root/summaryData/loser}}
              <tbody>
              {{#each this as |row rowIndex|}}
              <tr>
                <td style="background-color: rgba(222, 222, 222, 0.5);">{{row.dimensionName}}</td>
                <td style="background-color: rgba(222, 222, 222, 0.5);">{{row.dimensionValue}}</td>
                <td align="right">{{row.baselineValue}}</td>
                <td align="right">{{row.currentValue}}</td>
                <td align="right">{{row.percentageChange}}</td>
                <td align="right">{{row.contributionChange}}</td>
                <td align="right">{{row.contributionToOverallChange}}</td>
                <td align="right">{{row.cost}}</td>
              </tr>
              {{/each}}
              {{/with}}<!--end of summaryData -->
              </tbody>
            </table>
        {{/with}}<!--end of summaryData scope-->
    </script>
</section>
