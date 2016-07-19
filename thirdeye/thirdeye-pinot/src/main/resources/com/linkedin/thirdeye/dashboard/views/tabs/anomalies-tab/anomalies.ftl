<section id="anomalies-section" class="hidden" style="margin: 0;">
    <script id="anomalies-template" type="text/x-handlebars-template">

        <div class="title-box full-width">
        {{#each this as |anomalyData anomalyIndex|}}
        {{#if @first}}
            <h2 class="uk-margin-top">Anomalies in <b>{{anomalyData/collection}}</b> dataset <b>{{anomalyData/metric}}</b> metric</h2>
        {{/if}}
        {{/each}}
        </div>
        <table id="anomalies-table" class="uk-table display">
            <thead>
            <tr>
                <th class="select_all_cell"><input class="select-all-checkbox" value="1" type="checkbox" rel="anomalies" checked>ID</th>
                <!--{{!--<th >Metric</th>--}}-->
                <th>Start / End ({{returnUserTimeZone}})</th>
                <th>Alert reason</th>
				<!--<th >Function ID</th>-->
                <!--{{!--<th >Function type</th>--}}-->
                <th>Dimension</th>
                <th>Heatmap <br>of timerange</th>
                <th>Is this an anomaly?</th>
            </tr>
            </thead>

            <!-- Table of values -->
            <tbody class="">
            {{#each this as |anomalyData anomalyIndex|}}
            <tr>
                <td class="checkbox-cell"><label class="anomaly-table-checkbox"><input type="checkbox" data-value="{{anomalyData/metric}}" id="{{anomalyData/id}}" checked><div class="color-box uk-display-inline-block" style="background:{{colorById anomalyIndex @root.length}}">
                </div> {{anomalyData/id}}</label>
                </td>
                <!--{{!--<td>{{anomalyData/metric}}</td>--}}-->
                <td>
                    <p>{{millisToDate anomalyData/startTimeUtc showTimeZone=false}} </p>
                    <p> {{millisToDate anomalyData/endTimeUtc showTimeZone=false}}</p>
                </td>
                <td>{{anomalyData/message}}</td>
				<!--{{!--<td>{{anomalyData/functionId}}</td>--}}-->
                <!--{{!--<td>{{anomalyData/functionType}}</td>--}}-->
                <td>{{anomalyData/dimensions}}</td>
                <td>
                    <a class="heatmap-link" href="#" data-start-utc-millis="{{anomalyData/startTimeUtc}}" data-end-utc-millis="{{anomalyData/endTimeUtc}}"  data-metric="{{anomalyData/metric}}">
                        <span class="uk-button" data-uk-tooltip title="See heatmap of this timerange"><i class="uk-icon-eye"></i></span>
                    </a>
                </td>
                <td>
                   <div class="feedback-selector" data-anomaly-id="{{anomalyData/id}}" data-dataset="{{anomalyData/collection}}">
                        <div data-uk-dropdown="{mode:'click'}" aria-haspopup="true" aria-expanded="false" class="uk-button-group"
                            <!--<div class="add-feedback add-btn uk-display-inline-block" rel="{{tabName}}" data-uk-dropdown="{mode:'click'}">-->
                            <div id="selected-feedback" class="uk-button"  value="{{#if anomalyData/feedback/feedbackType}}{{anomalyData/feedback/feedbackType}}{{/if}}">{{#if anomalyData/feedback/feedbackType}}{{anomalyData/feedback/feedbackType}}{{else}}Provide Feedback{{/if}}</div>
                            <button class="add-single-metric-btn uk-button uk-button-primary" type="button"><i class="uk-icon-caret-down"></i></button>
                            <div class="uk-dropdown uk-dropdown-small">
                                <ul class="feedback-list uk-nav uk-nav-dropdown single-select">
                                    <li class="anomaly-feedback-option" value="NOT_ANOMALY"><a class="uk-dropdown-close">NOT_ANOMALY</a></li>
                                    <li class="anomaly-feedback-option" value="ANOMALY"><a class="uk-dropdown-close">ANOMALY</a></li>
                                    <li class="anomaly-feedback-option" value="ANOMALY_NO_ACTION"><a class="uk-dropdown-close">ANOMALY_NO_ACTION</a></li>
                                </ul>
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
