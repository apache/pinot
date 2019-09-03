<section id="anomaly-details">
    <script id="anomaly-details-template"  type="text/x-handlebars-template">
    <div id="anomaly-details-box" class="uk-width-1-1">
        <div id="" class="title-box">
            <a class="to-anomalies-view blue"><span>Anomalies</span></a><span>/ #</span>{{id}}

        </div>
        <div>
            <h3>{{metric}}</h3>
                <div class="timestamp uk-clearfix">
                  <span><div class="small-label">Start - End ({{returnUserTimeZone}}):</div>
                    {{displayDateRange startTime endTime}}
                  </span>
                </div>
                <span>score: {{score}}, </span>
                <span>weight: {{weight}}</span>
                <div><span>message: {{message}}</span></div>
        </div>
        <div id="anomaly-details-timeseries-placeholder">
            <span style="float:right;margin-right: 18%;">
                        <svg class="line-legend" width="80" height="25">
                            <line x1="0" y1="15" x2="20" y2="15" stroke="#1f77b4" stroke-width="3px"  stroke-dasharray="5,5"></line>
                            <text x="25" y="15" dy=".3em"
                                  style="text-anchor: start; font-family:Arial, sans-serif; font-size:0.75em; color:#1f77b4;">
                                baseline
                            </text>
                        </svg>
                        <svg class="line-legend" width="80" height="25">
                            <line x1="0" y1="15" x2="20" y2="15" stroke-width="3px" stroke="#ff5f0e"></line>
                            <text x="25" y="15" dy=".3em"
                                  style="text-anchor: start; font-family:Arial, sans-serif; font-size:0.75em; color:#ff5f0e;">
                                current
                            </text>
                        </svg>
                    </span>
            <div class="anom-linecharts-container" id="d3charts-{{anomalyIndex}}">
                <i class="uk-icon-spinner uk-icon-spin uk-icon-large"
                   style="z-index:15; position: absolute; right: 50%"></i> <span
                    style="height:148px; min-width:300px;"></span>
            </div>

        </div>
        <#--<div id="anomaly-details-heatmap-placeholder">-->
        <#--</div>-->
        <h4>Anomaly Function:</h4>
        <div>
            <table>
              <tr><td>Id </td><td> {{function/id}}</td></tr>
              <tr><td>Name </td><td> {{function/functionName}}</td></tr>
              <tr><td>Type </td><td> {{function/type}}</td></tr>
              <tr><td>Monitor Size </td><td> {{function/windowSize}} {{function/windowUnit}}</td></tr>
              <tr><td>Dimension </td><td> {{function/exploreDimensions}}</td></tr>
              <tr><td>Filters </td><td> {{function/filters}}</td></tr>
              <tr><td>Properties </td><td> {{function/properties}}</td></tr>
          </table>
        </div>

        <h4>Raw anomalies:</h4>
        <table id="raw-anomalies">
            <thead>
            <th>ID</th>
            <th>Start - End ({{returnUserTimeZone}}):</th>
            <th>Dimension</th>
            <th>Message</th>
            </thead>

            <tbody>
            {{#each anomalyResults as |rawAnomaly rawAnomalyIndex|}}
            <tr>
                <td>
                    {{id}}
                </td>
                <td>
                    {{displayDateRange rawAnomaly/startTime rawAnomaly/endTime}}
                </td>
                <td>
                    {{displayAnomalyResultExploreDimensions rawAnomaly/dimensions}}
                </td>
                <td>
                    {{message}}
                </td>
            </tr>
            {{/each}}
            </tbody>
        </table>

    </div>
    </script>
</section>
