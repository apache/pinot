<section id="anomaly-function-form-section">
<script id="anomaly-function-form-template" type="text/x-handlebars-template">
    <form id="configure-anomaly-function-form" class="uk-form">
           {{#if data/id}}
           {{else}}
               <div  class="title-box full-width" style="margin-top:15px;">
                   <h2>Create anomaly function</h2>
               </div>
           {{/if}}
    <table id="configure-functions-form-table">
            {{#if data/id}}
            <tr>
               <td>
                   <label class="uk-form-label bold-label">Function id: </label>
               </td>
                <td>
                    #<span id="function-id">{{data/id}}</span>
                </td>
            {{/if}}
            <tr>
                <td>
                    <label class="uk-form-label bold-label required">Name</label>
                </td>
                <td>
                    <input id="name" type="text" maxlength="160" {{#if data/functionName}}value="{{data/functionName}}"{{/if}} placeholder="anomaly_function_name">
                </td>
            </tr>
            <tr>
                <td>
                    <label class="uk-form-label bold-label required">Dataset</label>
                </td>
                <td>
                    <div data-uk-dropdown="{mode:'click'}" aria-haspopup="true" aria-expanded="false" class="uk-button-group">
                        <div class="selected-dataset uk-button"
                             value="{{#if data/collection}}{{data/collection}}{{/if}}">
                            {{#if data/collection}}
                                {{data/collection}}
                            {{else}}
                                Select dataset
                            {{/if}}
                        </div>
                        <div class="uk-button {{#if data/collection}}" disabled{{else}}uk-button-primary"{{/if}} type="button" >
                            <i class="uk-icon-caret-down"></i>
                        </div>
                        <div class="landing-dataset uk-dropdown uk-dropdown-small uk-dropdown-bottom" style="top: 30px; left: 0px;">
                        </div>
                    </div>
                </td>
            </tr>

            <tr>
                <td>
                    <label class="uk-form-label bold-label required">Function Type</label>
                </td>
                <td>
                    <div data-uk-dropdown="{mode:'click'}" aria-haspopup="true" aria-expanded="false" class="uk-button-group uk-display-inline-block">
                        <div id="selected-function-type" class="uk-button" value="{{#if data/id}}{{data/type}}{{else}}WEEK_OVER_WEEK_RULE{{/if}}">{{#if data/id}}{{data/type}}{{else}}WEEK_OVER_WEEK_RULE{{/if}}</div>
                        <div class="uk-button uk-button-primary" type="button"><i class="uk-icon-caret-down"></i></div>
                        <div class="uk-dropdown uk-dropdown-small uk-dropdown-bottom" style="top: 30px; left: 0px;">
                            <ul class="uk-nav uk-nav-dropdown single-select">
                                {{#if @root/fnTypeMetaData}}
                                {{#with @root/fnTypeMetaData}}
                                {{#each this as |functionProperties functionType|}}
                                <li id="{{functionType}}" class="function-type-option" value="{{functionType}}">
                                    <a class="uk-dropdown-close">{{functionType}}</a>
                                </li>
                                {{/each}}
                                {{/with}}
                                {{/if}}
                            </ul>
                        </div>
                    </div>
                </td>
            </tr>
        </table>

        <div class="uk-form-row uk-margin-top">
            <div class="uk-display-inline-block">Alert me when </div>
            <div class="uk-form-row uk-display-inline-block">
                <div data-uk-dropdown="{mode:'click'}" aria-haspopup="true" aria-expanded="false" class="uk-button-group uk-display-inline-block">
                    <div id="selected-metric-manage-anomaly-fn" class="uk-button"
                    value="{{#if data/metric}}{{data/metric}}{{/if}}">
                        {{#if data/metric}}
                           {{data/metric}}
                        {{else}}
                            Metric
                        {{/if}}
                    </div>
                    <div class="uk-button {{#if data/metric}}" disabled{{else}}uk-button-primary"{{/if}} type="button"><i class="uk-icon-caret-down"></i>
                    </div>
                    <div class="uk-dropdown uk-dropdown-small uk-dropdown-bottom" style="top: 30px; left: 0px;">
                        <ul class="metric-list single-metric-list uk-nav uk-nav-dropdown single-select">
                        </ul>
                    </div>
                </div>
            </div>

            <!-- ** WEEK_OVER_WEEK_RULE PROPERTIES ** -->
            <div class="WEEK_OVER_WEEK_RULE-fields function-type-fields uk-display-inline-block {{#if data/functionype}}uk-hidden{{/if}}">
                <div id="anomaly-condition-selector" class="uk-form-row uk-form-row uk-display-inline-block" rel="self-service">
                    <div data-uk-dropdown="{mode:'click'}" aria-haspopup="true" aria-expanded="false" class="uk-button-group uk-display-inline-block">
                        <div id="selected-anomaly-condition" class="uk-button" value={{#if fnProperties}}"{{describeDelta fnProperties/changeThreshold 'description'}}"{{/if}}>
                            {{#if fnProperties}}
                                {{describeDelta fnProperties/changeThreshold 'description'}}
                            {{else}}
                                 Condition
                            {{/if}}
                        </div>
                        <div class="uk-button uk-button-primary" type="button"><i class="uk-icon-caret-down"></i>
                        </div>
                        <div class="uk-dropdown uk-dropdown-small uk-dropdown-bottom" style="top: 30px; left: 0px;">
                            <ul class="uk-nav uk-nav-dropdown single-select">
                                <li class="anomaly-condition-option" value="DROPS"><a href="#" class="uk-dropdown-close">DROPS</a></li>
                                <li class="anomaly-condition-option" value="INCREASES"><a href="#" class="uk-dropdown-close">INCREASES</a></li>
                            </ul>
                        </div>
                    </div>
                </div>
                <span>by</span>
                <div class="uk-display-inline-block">
                    <input id="anomaly-threshold" type="text" placeholder="threshold" value="{{#if fnProperties}}{{displayAnomalyFunctionProp 'changeThreshold' fnProperties/changeThreshold}}{{/if}}"><span>%</span>
                </div>
                <div id="anomaly-compare-mode-selector" class="uk-form-row uk-display-inline-block" rel="self-service">
                    <div data-uk-dropdown="{mode:'click'}" aria-haspopup="true" aria-expanded="false" class="uk-button-group uk-display-inline-block">
                        <div id="selected-anomaly-compare-mode" class="uk-button" value="{{#if fnProperties}}{{#if properties/baseline}}{{properties/baseline}}{{else}}w/w{{/if}}{{else}}w/w{{/if}}">{{#if fnProperties}}{{#if properties/baseline}}{{properties/baseline}}{{else}}w/w{{/if}}{{else}}w/w{{/if}}</div>
                        <div class="uk-button uk-button-primary" type="button"><i class="uk-icon-caret-down"></i>
                        </div>
                        <div class="uk-dropdown uk-dropdown-small uk-dropdown-bottom" style="top: 30px; left: 0px;">
                            <ul class="uk-nav uk-nav-dropdown single-select">
                                <li class="anomaly-compare-mode-option" value="w/w"><a href="#" class="uk-dropdown-close">w/w</a></li>
                                <li class="anomaly-compare-mode-option" value="w/2w"><a href="#" class="uk-dropdown-close">w/2w</a></li>
                                <li class="anomaly-compare-mode-option" value="w/3w"><a href="#" class="uk-dropdown-close">w/3w</a></li>
                            </ul>
                        </div>
                    </div>
                </div>
            </div>

            <!-- ** END OF WEEK_OVER_WEEK_RULE PROPERTIES 2/1 ** -->
            <!-- ** MIN_MAX_THRESHOLD PROPERTIES ** -->
            <div class="MIN_MAX_THRESHOLD-fields function-type-fields uk-display-inline-block uk-hidden">
                <div id="anomaly-condition-selector-min-max" class="uk-display-inline-block">
                    <div data-uk-dropdown="{mode:'click'}" aria-haspopup="true" aria-expanded="false" class="uk-button-group uk-display-inline-block">
                        <div id="selected-anomaly-condition-min-max" class="uk-button" value="{{#if fnProperties}}{{else}}MIN{{/if}}">
                            {{#if fnProperties}}
                            {{else}}
                            IS LESS THAN
                            {{/if}}
                        </div>
                        <div class="uk-button uk-button-primary" type="button"><i class="uk-icon-caret-down"></i>
                        </div>
                        <div class="uk-dropdown uk-dropdown-small uk-dropdown-bottom" style="top: 30px; left: 0px;">
                            <ul class="uk-nav uk-nav-dropdown single-select">
                                <li class="anomaly-condition-min-max-option" value="MIN"><a href="#" class="uk-dropdown-close">IS LESS THAN</a></li>
                                <li class="anomaly-condition-min-max-option" value="MAX"><a href="#" class="uk-dropdown-close">IS MORE THAN</a></li>
                                <li class="anomaly-condition-min-max-option" value="MINMAX"><a href="#" class="uk-dropdown-close">IS NOT BETWEEN</a></li>
                            </ul>
                        </div>
                    </div>
                </div>
                <div class="uk-form-row uk-form-row uk-display-inline-block" rel="self-service">
                    <input id="anomaly-threshold-min" type="text" placeholder="min threshold" {{#if fnProperties}}value="{{fnProperties/min}}"{{else}}{{/if}}>
                    <span id="and" class="uk-hidden">and</span>
                    <input id="anomaly-threshold-max" class="uk-hidden" type="text" placeholder="max threshold" {{#if fnProperties}}value="{{fnProperties/max}}"{{else}} class="uk-hidden"{{/if}}>
                </div>
            </div>
            <!-- ** END OF MIN_MAX_THRESHOLD PROPERTIES ** -->
        </div>
        <div class="uk-form-row uk-margin-top">

            <!-- ** WEEK_OVER_WEEK_RULE & MIN_MAX_THRESHOLD PROPERTIES ** -->
            <div class="dimension-selection-fields function-type-fields">
                <div id="self-service-view-single-dimension-selector" class="view-single-dimension-selector uk-display-inline-block" rel="self-service">
                    <label class="uk-form-label">in dimension</label>
                    <div data-uk-dropdown="{mode:'click'}" aria-haspopup="true" aria-expanded="false" class="uk-button-group">
                        <div id="selected-dimension" class="uk-button" value="{{#if data/exploreDimensions}}{{data/exploreDimensions}}{{/if}}">{{#if data/exploreDimensions}}{{data/exploreDimensions}}{{else}}All{{/if}}</div>
                        <button class="add-single-dimension-btn uk-button uk-button-primary" type="button"><i class="uk-icon-caret-down"></i></button>
                        <div class="uk-dropdown uk-dropdown-small">
                            <ul class="dimension-list uk-nav uk-nav-dropdown single-select">
                            </ul>
                        </div>
                    </div>
                </div>
                <div class="view-filter-selector uk-display-inline-block" rel="self-service">
                    <label class="uk-form-label  uk-display-inline-block">with filters:</label>
                    <div id="self-service-add-filter" class="add-filter add-btn uk-display-inline-block" rel="self-service" data-uk-dropdown="{mode:'click'}">
                        <button class="uk-button uk-button-primary" type="button"><i class="uk-icon-plus"></i></button>
                        <div id="self-service-filter-panel" class="filter-panel uk-dropdown" rel="self-service" style="width:420px; display:none;">
                            <i class="close-dropdown-btn uk-icon-close" style="position: absolute; right:5px; top: 5px;"></i>
                            <a href="#" class="uk-dropdown-close">
                                <button id="self-service-apply-filter-btn" class="apply-filter-btn uk-button uk-button-primary"  rel="self-service"  style="float:right; margin: 5px;" disabled>Apply
                                </button>
                            </a>
                            <div class="dimension-filter" rel="self-service" style="width:150px;">
                                <ul  class="filter-dimension-list">
                                </ul>
                            </div>
                        </div>
                    </div>
                    <ul  class="selected-filters-list uk-display-inline-block" rel="self-service"</ul>
                </div>
            </div>

            <!-- ** WEEK_OVER_WEEK_RULE & MIN_MAX_THRESHOLD PROPERTIES ** -->
           <!-- ** WEEK_OVER_WEEK_RULE PROPERTIES 2/2 ** -->
           <div class="WEEK_OVER_WEEK_RULE-fields function-type-fields uk-display-inline-block uk-margin-large-top">

               <span>for</span>
               <input id="min-consecutive-size" class="thin-input" type="number" value="{{#if fnProperties}}{{displayAnomalyFunctionProp 'minConsecutiveSize' fnProperties/minConsecutiveSize}}{{else}}1{{/if}}">
               <span>consecutive </span><span class="aggregate-granularity">{{returnAggregateGranularity}}</span><span>.</span>
                   </div>
               </div>
           </div>
        </div>
       <!-- ** END OF WEEK_OVER_WEEK_RULE PROPERTIES 2/2 ** -->
       <!-- ** FUNCTION TYPE PROPERTIES ** -->

        {{#with @root/fnTypeMetaData}}
        {{#each this as |functionProperties functionType|}}
        <div class="{{functionType}}-fields function-type-fields uk-hidden" style="padding-left:50px;">
            <span class="exceed-txt">... exceeds the following:</span>
            <table class="uk-margin">
            {{#each this as |property propertyIndex|}}
            <tr>
                <td>{{property}}</td>
                <td class="" style="padding:5px;">
                    <input id="{{property}}" class="fn-type-property"
                           type="text"
                           data-property="{{property}}"

                    {{#if @root/fnProperties}}
                         value="{{lookupInMapByKey  @root/fnProperties key=property}}"
                    {{else}}
                         {{#lookupComplexScope  @root/propertyDefs/propertyDef primary=functionType secondary=property separator='|'}}
                             value="{{this.[0]}}"
                         {{/lookupComplexScope}}
                    {{/if}}

                    {{#lookupComplexScope  @root/propertyDefs/propertyDef primary=functionType secondary=property separator='|'}}
                          placeholder="{{add_string_if_eq this.[1] 'Pattern' stringToAdd='UP,DOWN'}}{{add_string_if_eq this.[1] 'TimeUnit' stringToAdd='HOURS,DAYS'}}"
                          data-expected="{{this.[1]}}"
                    >
                    <i class="uk-icon-question-circle"  data-uk-tooltip="{delay: 0}" title="expected data type: {{this.[1]}}"></i>
                    {{/lookupComplexScope}}
                </td>
            </tr>
            {{/each}}
            </table>
        </div>
        {{/each}}
        {{/with}}
        <!-- ** END OF FUNCTION PROPERTIES** -->

        <!-- EMAIL ADDRESS CONFIG currently 7/26/2016 not supported by the back end
        <div class="uk-form-row">
            <input class="" rel="self-service" type="checkbox" checked><span>Send me an email when this alert triggers. Email address: </span><input type="email" autocomplete="on">
        </div>-->





        <div class="uk-form-row">
            <span class="uk-form-label uk-display-inline-block">Monitor the data every
            </span>
            <input id="monitoring-repeat-size" type="number" class="thin-input {{hide_if_eq schedule/repeatEveryUnit 'DAYS'}}" value="{{#if schedule/repeatEverySize}}{{schedule/repeatEverySize}}{{else}}1{{/if}}">
            <div data-uk-dropdown="{mode:'click'}" aria-haspopup="true" aria-expanded="false" class="uk-button-group uk-display-inline-block">
                <div id="selected-monitoring-repeat-unit" class="uk-button" value="{{#if schedule/repeatEveryUnit}}{{schedule/repeatEveryUnit}}{{else}}{{returnAggregateGranularity}}{{/if}}">{{#if schedule/repeatEveryUnit}}{{schedule/repeatEveryUnit}}{{else}}{{returnAggregateGranularity}}{{/if}}</div>
                <div class="uk-button uk-button-primary" type="button"><i class="uk-icon-caret-down"></i>
                </div>
                <div class="uk-dropdown uk-dropdown-small uk-dropdown-bottom" style="top: 30px; left: 0px;">
                    <ul class="uk-nav uk-nav-dropdown  single-select">
                        <li class="anomaly-monitoring-repeat-unit-option" value="HOURS" ><a class="uk-dropdown-close">HOUR(S)</a></li>
                        <li class="anomaly-monitoring-repeat-unit-option" value="DAYS" ><a class="uk-dropdown-close">DAY(S)</a></li>
                    </ul>
                </div>
            </div>
                    <span id="monitoring-schedule" class="{{#if schedule}}{{hide_if_eq schedule/repeatEveryUnit 'HOURS'}}{{else}}uk-hidden{{/if}}">
                        <span> at
                        </span>
                        <input id="monitoring-schedule-time" type="text" placeholder="HH:MM or HH,HH,HH" value="{{#if schedule/scheduleHour}}{{schedule/scheduleHour}}{{else}}{{/if}}{{#if schedule/scheduleMinute}}:{{schedule/scheduleMinute}}{{else}}{{/if}}">
                        <span id="schedule-timezone">UTC</span>
                    </span>
           {{#if data/cron}}<span class="form-row uk-hidden">cron: {{data/cron}}</span>{{/if}}

                    <span>assessing the last</span>
                    <input id="monitoring-window-size" class="thin-input" type="number" value="{{#if data/windowSize}}{{data/windowSize}}{{else}}1{{/if}}">

                    <div id="monitoring-window-unit-selector uk-display-inline-block" class="uk-form-row uk-form-row uk-display-inline-block" rel="self-service">
                        <div data-uk-dropdown="{mode:'click'}" aria-haspopup="true" aria-expanded="false" class="uk-button-group uk-display-inline-block">
                            <div id="selected-monitoring-window-unit" class="uk-button" value="{{#if data/windowUnit}}{{data/windowUnit}}{{else}}HOURS{{/if}}">{{#if data/windowUnit}}{{data/windowUnit}}{{else}}HOUR(S){{/if}}</div>
                            <div class="uk-button uk-button-primary" type="button"><i class="uk-icon-caret-down"></i></div>
                            <div class="uk-dropdown uk-dropdown-small uk-dropdown-bottom" style="top: 30px; left: 0px;">
                                <ul class="uk-nav uk-nav-dropdown single-select">
                                    <li class="monitoring-window-unit-option" value="HOURS"><a href="#" class="uk-dropdown-close">HOUR(S)</a></li>
                                    <li class="monitoring-window-unit-option" value="DAYS"><a href="#" class="uk-dropdown-close" >DAY(S)</a></li>
                                </ul>
                            </div>
                        </div>
                    </div>
                    <span>data available.</span>
        </div>

        <div class="uk-form-row">
            <input id="active-alert" rel="self-service" type="checkbox"  {{#if data/isActive}}checked{{else}}{{/if}}><span> Keep monitoring.</span>
        </div>

        <div id="manage-anomaly-fn-error" class="uk-alert uk-alert-danger hidden" rel="self-service">
            <p></p>
        </div>
        <div id="manage-anomaly-function-success" class="uk-alert uk-alert-success hidden" rel="self-service">
            <p></p>
        </div>

        <div>
            {{#if data/id}}
            <button type="button" id="update-anomaly-function" class="uk-button uk-button-primary" rel="self-service">Update</button>
            <button id="close-update-fn-modal" type="button"  class="uk-button uk-modal-close">Cancel</button>
            {{else}}
            <div class="uk-form-row">
                <button type="button" id="create-anomaly-function" class="uk-button uk-button-primary" rel="self-service">Create</button>
                <button type="button" id="create-run-anomaly-function"  class="uk-button uk-button-primary uk-hidden">Create and Run</button>
                <button type="button" id="clear-create-form"  class="uk-button">Clear</button>
            </div>
            {{/if}}
        </div>
    </form>
</script>
</section>
