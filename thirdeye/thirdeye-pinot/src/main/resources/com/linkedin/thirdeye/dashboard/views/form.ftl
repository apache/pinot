<section id="query-input-form-section">
	<script id="form-template" type="text/x-handlebars-template">
        <form id="{{tabName}}-form" class="query-input-form uk-form uk-form-stacked"  style="width: 100%;">
            <div class="view-dataset-selector">
                <label class="uk-form-label">Dataset</label>
                <div data-uk-dropdown="{mode:'click'}" aria-haspopup="true" aria-expanded="false" class="uk-button-group uk-display-inline-block">
                    <div class="selected-dataset uk-button" value="">
                    </div>
                    <div class="uk-button uk-button-primary" type="button"><i class="uk-icon-caret-down"></i>
                    </div>
                    <div class="landing-dataset uk-dropdown uk-dropdown-small uk-dropdown-bottom" style="top: 30px; left: 0px;">
                    </div>
                </div>
            </div>
			{{#if showDashboardSelection}}
            <div class="view-dashboard-selector">
                <label class="uk-form-label">Dashboards</label>
                <div data-uk-dropdown="{mode:'click'}" aria-haspopup="true" aria-expanded="true" class="uk-button-group uk-display-inline-block">
                    <div id="selected-dashboard" class="uk-button">---</div>
                    <button class="uk-button uk-button-primary" type="button"><i class="uk-icon-caret-down"></i></button>
                    <div class="uk-dropdown uk-dropdown-small uk-dropdown-bottom" style="top: 30px; left: 0px;">
                        <ul id="dashboard-list" class="uk-nav uk-nav-dropdown single-select">
                        </ul>
                    </div>
                </div>
            </div>
			{{/if}}
			{{#if showMultiMetricSelection}}
            <div id="{{tabName}}-view-metric-selector" class="view-metric-selector" rel="{{tabName}}">
				<label class="uk-form-label uk-display-inline-block">Metrics</label>
                <div class="add-metrics add-btn uk-display-inline-block" rel="{{tabName}}" data-uk-dropdown="{mode:'click'}">
                    <button class="add-metrics-btn uk-button uk-button-primary" type="button"><i class="uk-icon-plus"></i></button>
                    <div class="uk-dropdown uk-dropdown-small ">
                        <ul class="metric-list uk-nav uk-nav-dropdown multi-select">
                        </ul>
                    </div>
                </div>
                <ul class="selected-metrics-list" rel="{{tabName}}" >
                </ul>
            </div>
			{{/if}}
            {{#if showSingleMetricSelection}}
            <div id="{{tabName}}-view-single-metric-selector" class="view-single-metric-selector" rel="{{tabName}}">
                <label class="uk-form-label">Anomaly metric</label>
                <div data-uk-dropdown="{mode:'click'}" aria-haspopup="true" aria-expanded="false" class="uk-button-group"
                <!--<div class="add-metric add-btn uk-display-inline-block" rel="{{tabName}}" data-uk-dropdown="{mode:'click'}">-->
                    <div id="selected-metric" class="uk-button">Select metric</div>
                    <button class="add-single-metric-btn uk-button uk-button-primary" type="button"><i class="uk-icon-caret-down"></i></button>
                    <div class="uk-dropdown uk-dropdown-small">
                        <ul class="single-metric-list uk-nav uk-nav-dropdown single-select">
                        </ul>
                    </div>
                </div>

            </div>
            {{/if}}
			{{#if showDimensionSelection}}
            <div class="view-dimension-selector" rel="{{tabName}}">
                <label class="uk-form-label  uk-display-inline-block">Dimensions</label>
                <div class="add-dimensions add-btn uk-display-inline-block" rel="{{tabName}}" data-uk-dropdown="{mode:'click'}" aria-haspopup="true" aria-expanded="true">
                    <button class="add-dimensions-btn uk-button uk-button-primary" type="button"><i class="uk-icon-plus"></i></button>
                    <div class="uk-dropdown uk-dropdown-small">
                        <ul class="dimension-list uk-nav uk-nav-dropdown multi-select">
                        </ul>
                    </div>
                </div>
                <a class="dropdown-toggle-all hidden" rel="{{tabName}}" href="#">Select All</a>
                <ul class="selected-dimensions-list" rel="{{tabName}}">
                </ul>
            </div>
			{{/if}}
			{{#if showFilterSelection}}
            <div class="view-filter-selector" rel="{{tabName}}">
                 <label class="uk-form-label  uk-display-inline-block">Filters</label>
                <div id="{{tabName}}-add-filter" class="add-filter add-btn uk-display-inline-block" rel="{{tabName}}" data-uk-dropdown="{mode:'click'}">
                    <button class="uk-button uk-button-primary" type="button"><i class="uk-icon-plus"></i></button>
                    <div id="{{tabName}}-filter-panel" class="filter-panel uk-dropdown" rel="{{tabName}}" style="width:420px; display:none;">
                        <i class="close-dropdown-btn uk-icon-close" style="position: absolute; right:5px; top: 5px;"></i>
                        <a href="#" class="uk-dropdown-close">
                            <button id="{{tabName}}-apply-filter-btn" class="apply-filter-btn uk-button uk-button-primary"  rel="{{tabName}}"  style="float:right; margin: 5px;" disabled>Apply
                            </button>
                        </a>
                        <div class="dimension-filter" rel="{{tabName}}" style="width:150px;">
                            <ul  class="filter-dimension-list">
                            </ul>
                        </div>
                    </div>
                </div>
                <ul  class="selected-filters-list" rel="{{tabName}}"</ul>
            </div>
			{{/if}}
            <div class="view-date-range-selector uk-form-row">
                <label  class="uk-form-label">
                    Date range selector
                </label>
                <span> ( Max date: <span class="max-time"></span> )</span>
                <div class="time-range-selector-dropdown" data-uk-dropdown="{mode:'click'}" rel="{{tabName}}">
                    <div class="date-time-selector-box">
                        <table class="date-time-selector-box-table">
                            <tr>
                                <td class="date-time-selector-box-current-td">
                                    <span id="{{tabName}}-current-start-date" class="current-start-date" rel="{{tabName}}"></span>
                                    <span id="{{tabName}}-current-start-time" class="current-start-time" rel="{{tabName}}"></span>
                                    <span class="date-devider">-</span>
                                    <span id="{{tabName}}-current-end-date" class="current-end-date" rel="{{tabName}}"></span>
                                    <span id="{{tabName}}-current-end-time" class="current-end-time" rel="{{tabName}}"></span>

                                    {{#if needComparisonTimeRange}}
                                    <div class="comparison-display" rel="{{tabName}}"><label class="comparison-display-label">Compare to:</label><br>
                                        <span id="{{tabName}}-baseline-start-date" class="baseline-start-date" rel="{{tabName}}"></span>
                                        <span id="{{tabName}}-baseline-start-time" class="baseline-start-time" rel="{{tabName}}"></span>
                                        <span class="date-devider">-</span>
                                        <span id="{{tabName}}-baseline-end-date" class="baseline-end-date" rel="{{tabName}}"></span>
                                        <span id="{{tabName}}-baseline-end-time" class="baseline-end-time" rel="{{tabName}}"></span>
                                    </div>
                                    {{/if}}
                                </td>
                                <td class="arrow-down uk-button-primary"><i class="uk-icon-caret-down"></i></td>
                            </tr>
                        </table>
                    </div>
                    <div class="uk-dropdown" style="positon: absolute; top:0; left: 0; width:420px;">
                        <div>
                            <i class="close-dropdown-btn uk-icon-close" style="position: absolute; right:5px; top: 5px;"></i>
                            <span class="bold-label">Date Range:</span>
                            <select class="current-date-range-selector" rel="{{tabName}}">
                                <option class="current-date-range-option" value="custom">Custom</option>
                                <option class="current-date-range-option"  value="today">Today</option>
                                <option class="current-date-range-option"  value="yesterday">Yesterday</option>
                                <option class="current-date-range-option"  value="7">Last 7 days</option>
                                <option class="current-date-range-option"  value="24">Last 24 hours</option>
                            </select>
                        </div>
                        <div class="uk-margin-small">
                            <input id="{{tabName}}-current-start-date-input" class="current-start-date-input" rel="{{tabName}}" type="text" placeholder="YYYY-MM-DD" data-uk-datepicker="{ weekstart:0, format:'YYYY-MM-DD' }" style="width:110px;">
                            <input id="{{tabName}}-current-start-time-input" class="current-start-time-input thin-input" rel="{{tabName}}"  type="text" data-uk-timepicker="{format:'24h'}" placeholder="HH:MM"> -
                            <input id="{{tabName}}-current-end-date-input" class="current-end-date-input" rel="{{tabName}}" type="text" placeholder="YYYY-MM-DD" data-uk-datepicker="{ weekstart:0, format:'YYYY-MM-DD' }" style="width:110px;">
                            <input id="{{tabName}}-current-end-time-input" class="current-end-time-input thin-input" rel="{{tabName}}" type="text" data-uk-timepicker="{format:'24h'}" placeholder="HH:MM">
                        </div>
                        {{#if needComparisonTimeRange}}
                        <div>

                            <input class="time-input-compare-checkbox uk-hidden" rel="{{tabName}}" type="checkbox" checked>
                            <label class="bold-label">
                                Compare to:
                            </label>
                            <select id="{{tabName}}-compare-mode-selector" class="compare-mode-selector" rel="{{tabName}}">
                                <option unit="WoW" value="7">WoW</option>
                                <option unit="Wo2W" value="14">Wo2W</option>
                                <option unit="Wo3W" value="21">Wo3W</option>
                                <option unit="Wo4W" value="28">Wo4W</option>
                                <option unit="1" value="1">Custom</option>
                            </select>
                        </div>
                        <div class="uk-margin-small" rel="{{tabName}}">
                            <input id="{{tabName}}-baseline-start-date-input" class="baseline-start-date-input"  rel="{{tabName}}" type="text" placeholder="YYYY-MM-DD" data-uk-datepicker="{ weekstart:0, format:'YYYY-MM-DD'}" style="width:110px;">
                            <input id="{{tabName}}-baseline-start-time-input" class="baseline-start-time-input" rel="{{tabName}}" type="text" data-uk-timepicker="{format:'24h'}" placeholder="HH:MM" style="width:50px;"> -

                            <input id="{{tabName}}-baseline-end-date-input" class="baseline-end-date-input uk-margin-small" rel="{{tabName}}" type="text" placeholder="YYYY-MM-DD" data-uk-datepicker="{weekstart:0, format:'YYYY-MM-DD'}" style="width:110px;">
                            <input id="{{tabName}}-baseline-end-time-input" class="baseline-end-time-input" rel="{{tabName}}" type="text" data-uk-timepicker="{format:'24h'}" placeholder="HH:MM" style="width:50px;">

                        </div>
                        {{/if}}

                        <div class="max-min-time-label uk-margin-small" rel="{{tabName}}">
                            Data available for timerange:
                            <span class="min-time"></span>
                            <span>-</span>
                            <span class="max-time"></span>
                        </div>
                        <div class="time-input-logic-error uk-alert uk-alert-danger hidden" rel="{{tabName}}">
                            <p></p>
                        </div>
                        <a href="#" class="uk-dropdown-close">
                            <button class="time-input-apply-btn uk-button uk-button-primary" rel="{{tabName}}"  style="float:right; margin: 5px;" disabled>Apply
                            </button>
                        </a>
                    </div>
                </div>
            </div>
            <p class="compare-mode uk-form-row hidden" rel="{{tabName}}" ></p>
            {{#if showGranularity}}
            <div class="uk-form-row">
                <label class="uk-form-label">
                    Granularity
                </label>
                <div class="granularity-btn-group uk-button-group vertical radio-buttons">
                    <button class="time-input-form-gran-minutes baseline-aggregate radio-type-button" rel="{{tabName}}" type="button" unit="10_MINUTES" value="600000" style="display:none">10 MINUTES</button>
                    <button class="time-input-form-gran-hours baseline-aggregate radio-type-button uk-active" rel="{{tabName}}" type="button" unit="HOURS" value="3600000">HOUR</button>
                    <button class="time-input-form-gran-days baseline-aggregate radio-type-button" rel="{{tabName}}" unit="DAYS" type="button" value="86400000">DAY</button>
                    {{#if showAggregateAllGranularity}}
                    <button class="time-input-form-gran-aggregate-all baseline-aggregate radio-type-button" rel="{{tabName}}" unit="aggregateAll" type="button" value="0">Aggregate all</button>
                    {{/if}}
                </div>
            </div>
            {{/if}}

            <div id="{{tabName}}-time-input-form-error" class="time-input-form-error uk-alert uk-alert-danger uk-form-row hidden">
                <p></p>
            </div>
            <div id="{{tabName}}-form-tip" class="tip-to-user uk-alert uk-form-row hidden">
                <i class="close-parent uk-icon-close"></i>
                <p></p>
            </div>

            <div class="uk-form-row">
                <button type="button" id="{{tabName}}-form-submit" class="form-submit-btn uk-button uk-button-primary" rel="{{tabName}}">Go</button>
            </div>
        </form>

        {{#if showConfigAnomaly}}
        <!-- This is a button toggling the modal -->

        <button class="uk-button uk-margin-large" data-uk-modal="{target:'#manage-alert-modal', center:true}">Configure anomaly alerts</button>

        <div id="manage-alert-modal" class="uk-modal">
            <div class="uk-modal-dialog uk-modal-dialog-large">
                <a class="uk-button uk-modal-close uk-close"></a>
                <div class="uk-modal-header">Configure anomaly alerts</div>
            <form id="configure-alert-form" class="uk-form">
                <div class="uk-form-row">
                    <label class="uk-form-label bold-label required">Rule</label>
                    <input id="rule" type="text" maxlength="80">
                </div>
                <div class="uk-form-row">
                    <label class="uk-form-label bold-label required">Dataset</label>
                    <div data-uk-dropdown="{mode:'click'}" aria-haspopup="true" aria-expanded="false" class="uk-button-group uk-display-inline-block">
                        <div id="selected-anomaly-dataset" class="uk-button" value="">Select dataset
                        </div>
                        <div class="uk-button uk-button-primary" type="button"><i class="uk-icon-caret-down"></i>
                        </div>
                        <div class="uk-dropdown uk-dropdown-small uk-dropdown-bottom anomaly-dataset" style="top: 30px; left: 0px;">
                        </div>
                    </div>
                </div>
                <div class="uk-display-inline-block">Alert me when </div>
                <div id="metric-selector-manage-alert" class="uk-form-row uk-display-inline-block">
                <div data-uk-dropdown="{mode:'click'}" aria-haspopup="true" aria-expanded="false" class="uk-button-group uk-display-inline-block">
                        <div id="selected-metric-manage-alert" class="uk-button" value="">Metric</div>
                            <div class="uk-button uk-button-primary" type="button"><i class="uk-icon-caret-down"></i>
                        </div>
                        <div class="uk-dropdown uk-dropdown-small uk-dropdown-bottom" style="top: 30px; left: 0px;">
                            <ul class="manage-alert-metric-list uk-nav uk-nav-dropdown">
                            </ul>
                        </div>
                    </div>
                </div>
                <div id="anomaly-condition-selector" class="uk-form-row uk-form-row uk-display-inline-block" rel="{{tabName}}">
                    <div data-uk-dropdown="{mode:'click'}" aria-haspopup="true" aria-expanded="false" class="uk-button-group uk-display-inline-block">
                        <div id="selected-anomaly-condition" class="uk-button" value="">Condition
                        </div>

                        <div class="uk-button uk-button-primary" type="button"><i class="uk-icon-caret-down"></i>
                        </div>
                        <div class="uk-dropdown uk-dropdown-small uk-dropdown-bottom" style="top: 30px; left: 0px;">
                            <ul class="uk-nav uk-nav-dropdown">
                                <li class="anomaly-condition-option" value="DROPS"><a href="#" class="uk-dropdown-close">DROPS</a></li>
                                <li class="anomaly-condition-option" value="INCREASES"><a href="#" class="uk-dropdown-close">INCREASES</a></li>
                            </ul>
                        </div>
                    </div>
                </div>
                <span> by </span>
                <div class="uk-form-row uk-form-row uk-display-inline-block" rel="{{tabName}}">
                    <input id="anomaly-threshold" type="text" placeholder="threshold (1-100)" ><span>%</span>
                </div>
                <div id="anomaly-compare-mode-selector uk-display-inline-block" class="uk-form-row uk-form-row uk-display-inline-block" rel="{{tabName}}">
                    <div data-uk-dropdown="{mode:'click'}" aria-haspopup="true" aria-expanded="false" class="uk-button-group uk-display-inline-block">
                        <div id="selected-anomaly-compare-mode" class="uk-button" value="WoW">WoW</div>
                        <div class="uk-button uk-button-primary" type="button"><i class="uk-icon-caret-down"></i>
                        </div>
                        <div class="uk-dropdown uk-dropdown-small uk-dropdown-bottom" style="top: 30px; left: 0px;">
                            <ul class="uk-nav uk-nav-dropdown">
                                <li class="anomaly-compare-mode-option" unit="w/w"><a href="#" class="uk-dropdown-close">WoW</a></li>
                                <li class="anomaly-compare-mode-option" unit="w/2w"><a href="#" class="uk-dropdown-close">Wo2W</a></li>
                                <li class="anomaly-compare-mode-option" unit="w/3w"><a href="#" class="uk-dropdown-close">Wo3W</a></li>
                            </ul>
                        </div>
                    </div>
                </div>
                <span>for</span>
                <input id="monitoring-window-size" class="thin-input" type="number">
                <span>consecutive</span>
                <div id="monitoring-unit-selector uk-display-inline-block" class="uk-form-row uk-form-row uk-display-inline-block" rel="{{tabName}}">
                    <div data-uk-dropdown="{mode:'click'}" aria-haspopup="true" aria-expanded="false" class="uk-button-group uk-display-inline-block">
                        <div id="selected-monitoring-window-unit" class="uk-button" unit="HOURS">HOUR(S)</div>
                        <div class="uk-button uk-button-primary" type="button"><i class="uk-icon-caret-down"></i></div>
                        <div class="uk-dropdown uk-dropdown-small uk-dropdown-bottom" style="top: 30px; left: 0px;">
                            <ul class="uk-nav uk-nav-dropdown">
                                <li class="monitoring-window-unit-option" unit="HOURS"><a href="#" class="uk-dropdown-close">HOUR(S)</a></li>
                                <li class="monitoring-window-unit-option" unit="DAYS"><a href="#" class="uk-dropdown-close" >DAY(S)</a></li>
                                <li class="monitoring-window-unit-option" unit="WEEKS"><a href="#" class="uk-dropdown-close">WEEK(S)</a></li>
                                <li class="monitoring-window-unit-option" unit="MONTHS"><a href="#" class="uk-dropdown-close">MONTH(S)</a></li>
                            </ul>
                        </div>
                    </div>
                </div>


                <!--<div class="uk-form-row">
                    <input class="" rel="{{tabName}}" type="checkbox" checked><span>Send me an email when this alert triggers. Email address: </span><input type="email" autocomplete="on">
                </div>-->



                <!-- elements needed for KALMAN STATISTICS
                    <#--<div class="uk-form-row hidden">-->
                        <#--<label class="uk-form-label bold-label uk-display-inline-block">Monitoring window</label><br>-->
                        <#--<label class="uk-form-label bold-label uk-display-inline-block">Window size</label>-->
                        <#--<input type="number" class="thin-input">-->
                        <#--<label class="uk-form-label bold-label uk-display-inline-block">Window unit</label>-->
                        <#--<div data-uk-dropdown="{mode:'click'}" aria-haspopup="true" aria-expanded="false" class="uk-button-group uk-display-inline-block">-->
                            <#--<div class="" uk-button" value="">-->
                            <#--</div>-->
                            <#--<div class="uk-button uk-button-primary" type="button"><i class="uk-icon-caret-down"></i>-->
                            <#--</div>-->
                            <#--<div class="uk-dropdown uk-dropdown-small uk-dropdown-bottom" style="top: 14px; left: 0px;">-->
                            <#--</div>-->
                        <#--</div>-->
                    <#--</div>-->

                    <#--<div class="uk-form-row hidden">-->
                        <#--<label class="uk-form-label bold-label uk-display-inline-block">Training window</label><br>-->
                        <#--<label class="uk-form-label bold-label uk-display-inline-block">Window size</label>-->
                        <#--<input type="number" class="thin-input">-->
                        <#--<label class="uk-form-label bold-label uk-display-inline-block">Window unit</label>-->
                        <#--<div data-uk-dropdown="{mode:'click'}" aria-haspopup="true" aria-expanded="false" class="uk-button-group uk-display-inline-block">-->
                            <#--<div class="selected-anomaly-training-window-unit uk-button" value="">-->
                            <#--</div>-->
                            <#--<div class="uk-button uk-button-primary" type="button"><i class="uk-icon-caret-down"></i>-->
                            <#--</div>-->
                            <#--<div class="uk-dropdown uk-dropdown-small uk-dropdown-bottom" style="top: 30px; left: 0px;">-->
                            <#--</div>-->
                        <#--</div>-->
                    <#--</div>-->
                -->

                <div class="uk-form-row">
                    <span class="uk-form-label uk-display-inline-block">Monitor the data every </span>
                    <input id="monitoring-repeat-size" type="number" class="thin-input">
                    <div data-uk-dropdown="{mode:'click'}" aria-haspopup="true" aria-expanded="false" class="uk-button-group uk-display-inline-block">
                        <div id="selected-monitoring-repeat-unit" class="uk-button" unit="HOURS">HOUR(S)</div>
                        <div class="uk-button uk-button-primary" type="button"><i class="uk-icon-caret-down"></i>
                        </div>
                        <div class="uk-dropdown uk-dropdown-small uk-dropdown-bottom" style="top: 30px; left: 0px;">
                            <ul class="uk-nav uk-nav-dropdown">
                                <li class="anomaly-monitoring-repeat-unit-option" unit="HOURS" ><a href="#" class="uk-dropdown-close">HOUR(S)</a></li>
                                <li class="anomaly-monitoring-repeat-unit-option" unit="DAYS" ><a href="#" class="uk-dropdown-close">DAY(S)</a></li>
                            </ul>
                        </div>
                    </div>
                    <span id="monitoring-schedule" class="hidden">
                        <span> at
                        </span>
                        <input id="monitoring-schedule-time" class="thin-input" type="text" data-uk-timepicker="{format:'24h'}" placeholder="HH:MM">
                    </span>
                </div>

                <div class="uk-form-row">
                    <input id="active-alert" rel="{{tabName}}" type="checkbox" checked><span> Keep this alert active.</span>
                </div>

                <div id="manage-alert-error" class="uk-alert uk-alert-danger hidden" rel="{{tabName}}">
                    <p></p>
                </div>
                <div id="manage-alert-success" class="uk-alert uk-alert-success hidden" rel="{{tabName}}">
                    <p></p>
                </div>

                <div class="uk-modal-footer">
                    <button type="button" id="save-alert" class="uk-button uk-button-primary" rel="{{tabName}}">Save Alert</button>
                    <button class="uk-button uk-modal-close">Cancel</button>
                </div>
            </form>
        </div>
        {{/if}}
    </script>
</section>