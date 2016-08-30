<section id="anomalies-section" class="hidden" style="margin: 0;">
    <script id="anomalies-template" type="text/x-handlebars-template">

        <div class="title-box full-width">
        {{#each this as |anomalyData anomalyIndex|}}
        {{#if @first}}
            <h2 class="uk-margin-top">Anomalies in <b>{{anomalyData/collection}}</b> dataset <b>{{anomalyData/metric}}</b> metric</h2>
        {{/if}}
        {{/each}}
        </div>
        <table id="anomalies-table" class="uk-table display" data-page-length='100'>
            <thead>
            <tr>
                <th class="select_all_cell"><input class="select-all-checkbox hidden" value="1" type="checkbox" rel="anomalies" checked>ID</th>
                <th>Start / End ({{returnUserTimeZone}})</th>
                <th>Alert reason</th>
                <th>Dimension</th>
                <th>Heatmap <br>of timerange</th>
                <th>Is this an anomaly?</th>
            </tr>
            </thead>

            <!-- Table of values -->
            <tbody class="">
            {{#each this as |anomalyData anomalyIndex|}}
            <tr>
                <td class="checkbox-cell"><label class="anomaly-table-checkbox">
                    <input type="checkbox" data-value="{{anomalyData/metric}}" id="{{anomalyData/id}}" checked><div class="color-box uk-display-inline-block" style="background:{{colorById anomalyIndex @root.length}}">
                </div> {{anomalyData/id}}</label>
                </td>
                <td>
                    <p>{{millisToDate anomalyData/startTime showTimeZone=false}} </p>
                    <p> {{millisToDate anomalyData/endTime showTimeZone=false}}</p>
                </td>
                <td>{{anomalyData/message}}</td>
                <td>{{#if anomalyData/function/exploreDimensions}}{{anomalyData/function/exploreDimensions}}:{{/if}} {{displayAnomalyResultDimensionValue anomalyData/dimensions}}</td>
                <td>
                    <a class="heatmap-link" href="#" data-start-utc-millis="{{anomalyData/startTime}}" data-end-utc-millis="{{anomalyData/endTime}}"  data-metric="{{anomalyData/metric}}">
                        <span class="uk-button" data-uk-tooltip title="See heatmap of this timerange"><i class="uk-icon-eye"></i></span>
                    </a>
                </td>
                <td>
                   <div class="feedback-selector">
                        <div data-uk-dropdown="{mode:'click'}" aria-haspopup="true" aria-expanded="false" class="feedback-dropdown uk-button-group">
                            <div class="selected-feedback uk-button"  data-anomaly-id="{{anomalyData/id}}" value="{{#if anomalyData/feedback/feedbackType}}{{anomalyData/feedback/feedbackType}}{{/if}}">{{#if anomalyData/feedback/feedbackType}}{{anomalyData/feedback/feedbackType}}{{else}}Provide Feedback{{/if}}</div>
                            <button class="uk-button uk-button-primary" type="button"><i class="uk-icon-caret-down"></i></button>
                            <div class="uk-dropdown uk-dropdown-small">
                                <ul class="feedback-list uk-nav uk-nav-dropdown single-select">
                                    <li class="anomaly-feedback-option" value="NOT_ANOMALY"><a>NOT_ANOMALY</a></li>
                                    <li class="anomaly-feedback-option" value="ANOMALY"><a>ANOMALY</a></li>
                                    <li class="anomaly-feedback-option" value="ANOMALY_NO_ACTION"><a>ANOMALY_NO_ACTION</a></li>
                                </ul>
                                <textarea class="feedback-comment {{#if anomalyData/feedback/comment}}{{else}}hidden{{/if}}">{{#if anomalyData/feedback/comment}}{{anomalyData/feedback/comment}}{{/if}}</textarea>
                            </div>
                        </div>
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
