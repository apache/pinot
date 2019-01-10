<section id="anomalies-section" class="hidden" style="margin: 0;">
    <script id="anomalies-template" type="text/x-handlebars-template">

        <div class="title-box full-width">
            {{#each this as |anomalyData anomalyIndex|}}
            {{#if @first}}
            <h2 class="uk-margin-top">Anomalies in <b>{{anomalyData/collection}}</b> dataset
                <b>{{anomalyData/metric}}</b> metric</h2>
            {{/if}}
            {{/each}}
        </div>
        <div id="anomaly-result-thumbnails">
            {{#each this as |anomalyData anomalyIndex|}}
            <div class="large-anomaly-box">
                <div class="anomalies-box-background">
                    <div class="box-inner">
                        <div class="chart-info">
                        </div>
                        <div class="anomaly-info">
                        </div>


                        <div>
                        <table><tr>
                            <td><a class="anomaly-details-link blue" data-id="{{anomalyData/id}}"># {{anomalyData/id}}</a></td>
                            <td><div class="blue" id="external-props-{{anomalyIndex}}"></div></td>
                        </tr></table>
                        </div>


                        <div>
                            <h3 style="margin-top:0px;">
                                {{anomalyData/metric}}
                            </h3>
                        </div>

                        <div>
                            <div class="small-label">dimension:</div>
                            <div class="dimension">
                            <span style="font-weight: 800;font-size:14px; ">
                                {{displayAnomalyResultExploreDimensions anomalyData/dimensions}}
                            </span>

                            <div class="change" style="margin-top:6px;">
                               {{parseProperties anomalyData/message 'change'}}
                            </div>
                        </div>
                        <div class="current-baseline">
                            <br/>
                            <table>
                                <tr><td class="small-label" style="padding: 3px 0;">current: </td><td style="color:#ff5f0e">{{parseProperties anomalyData/message 'currentVal'}}</td>
                                    </tr>
                                <tr><td class="small-label" style="padding: 3px 0;">baseline: </td><td style="color:#1f77b4">{{parseProperties anomalyData/message 'baseLineVal'}}</td></tr>
                            </table>


                            </div>

                            <div class="timestamp uk-clearfix">
                                <br/>
                                <span><div class="small-label">Start - End ({{returnUserTimeZone}}):</div>
                                {{displayDateRange anomalyData/startTime anomalyData/endTime}}
                                </span>
                            </div>

                        </div>
                    </div>
                </div>
                <div class="box-anom-linechart" data-anom-linecharts-chart="{{anomalyIndex}}">

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
                <div class="action-buttons" style="width: 18%; float: right; text-align: right; position: absolute;top: 40px; right: 10px; z-index:4;">

                    <div class="feedback-selector" style=" position: absolute; top:0px; right:0px;">
                        <div data-uk-dropdown="{mode:'click'}" aria-haspopup="true" aria-expanded="false"
                             class="feedback-dropdown uk-button-group" style="text-align: left;">
                            <div class="selected-feedback uk-button" data-anomaly-id="{{anomalyData/id}}"
                                 value="{{#if anomalyData/feedback/feedbackType}}{{anomalyData/feedback/feedbackType}}{{/if}}">
                                {{#if
                                anomalyData/feedback/feedbackType}}{{anomalyData/feedback/feedbackType}}{{else}}Provide
                                Feedback{{/if}}
                            </div>
                            <button class="uk-button uk-button-primary" type="button"><i
                                    class="uk-icon-caret-down"></i>
                            </button>
                            <div class="uk-dropdown uk-dropdown-small">
                              <ul class="feedback-list uk-nav uk-nav-dropdown single-select">
                                <li class="anomaly-feedback-option" value="NOT_ANOMALY"><a>False Alarm</a></li>
                                <li class="anomaly-feedback-option" value="ANOMALY"><a>Confirmed Anomaly</a></li>
                                <li class="anomaly-feedback-option" value="ANOMALY_NEW_TREND"><a>Confirmed - New Trend</a></li>
                              </ul>
                                <textarea
                                        class="feedback-comment {{#if anomalyData/feedback/comment}}{{else}}hidden{{/if}}">{{#if
                                    anomalyData/feedback/comment}}{{anomalyData/feedback/comment}}{{/if}}
                                </textarea>
                            </div>
                        </div>
                    </div>

                    <a class="heatmap-link" href="#" data-start-utc-millis="{{anomalyData/startTime}}"
                       data-end-utc-millis="{{anomalyData/endTime}}"
                       data-metric="{{anomalyData/metric}}" style="padding: 0px;  position: absolute; top:50px; right:0px;">
                               <span class="uk-button" data-uk-tooltip title="See heatmap of this timerange" style="display: inline-block; width:175px;">
                                   <i class="uk-icon-th-list heatmap-icon"></i>
                                   <text style="margin-left:5px;">Heatmap</text>
                               </span>
                    </a>
                </div>

                <div id="function-details" style="position: absolute; right:15px; top: 140px; width: 170px; text-overflow: ellipsis;">
                    <label>Anomaly Function details:</label>
                    <div style="font-weight: 800;font-size:14px; ">{{anomalyData/function/functionName}}
                    </div>
                    <div style="font-weight: 800;font-size:14px; ">{{anomalyData/function/type}}
                    </div>
                </div>

            </div>
            {{/each}}
        </div>
	<div id="anomaly-table-tooltip" class="hidden">
                <table>
                </table>
        </div>
    </script>
</section>
