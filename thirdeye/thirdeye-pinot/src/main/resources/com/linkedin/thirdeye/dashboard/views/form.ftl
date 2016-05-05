<section id="time-input-form-section">
	<script id="form-template" type="text/x-handlebars-template">
        <form id="{{tabName}}-form" class="input-form uk-form uk-form-stacked"  style="width: 100%;">
            <div class="view-dataset-selector">
                <label class="uk-form-label">Dataset</label>
                <div data-uk-dropdown="{mode:'click'}" aria-haspopup="true" aria-expanded="false" class="uk-button-group uk-display-inline-block">
                    <div class="selected-dataset uk-button" value="thirdeyeAbook">thirdeyeAbook
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
			{{#if showMetricSelection}}
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
                <ul class="selected-dimensions-list" rel="{{tabName}}">
                </ul>
            </div>
			{{/if}}
			{{#if showFilterSelection}}
            <div class="view-filter-selector" rel="{{tabName}}">
                 <label class="uk-form-label  uk-display-inline-block">Filters</label>
                <div id="{{tabName}}-add-filter" class="add-filter add-btn uk-display-inline-block" rel="{{tabName}}" data-uk-dropdown="{mode:'click'}">
                    <button class="uk-button uk-button-primary" type="button"><i class="uk-icon-plus"></i></button>
                    <div id="{{tabName}}-filter-panel" class="filter-panel uk-dropdown" style="width:420px; display:none;">
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
                <div data-uk-dropdown="{mode:'click'}" style="position: relative;">
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
                            </select>
                        </div>
                        <div class="uk-margin-small">
                            <input id="{{tabName}}-current-start-date-input" class="current-start-date-input" rel="{{tabName}}" type="text" placeholder="YYYY-MM-DD" data-uk-datepicker="{ weekstart:0, format:'YYYY-MM-DD'}" style="width:110px;">
                            <input id="{{tabName}}-current-start-time-input" class="current-start-time-input" rel="{{tabName}}"  type="text" data-uk-timepicker="{format:'24h'}" placeholder="HH:MM" style="width:50px;"> -
                            <input id="{{tabName}}-current-end-date-input" class="current-end-date-input" rel="{{tabName}}" type="text" placeholder="YYYY-MM-DD" data-uk-datepicker="{ weekstart:0, format:'YYYY-MM-DD'}" style="width:110px;">
                            <input id="{{tabName}}-current-end-time-input" class="current-end-time-input" rel="{{tabName}}" type="text" data-uk-timepicker="{format:'24h'}" placeholder="HH:MM" style="width:50px;">
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

                        <label class="uk-form-label">
                            Granularity
                        </label>
                        <div class="uk-button-group radio-buttons" rel="{{tabName}}">
                            <button class="time-input-form-gran-hours-copy baseline-aggregate-copy  radio-type-button uk-active" rel="{{tabName}}" type="button" unit="HOURS" value="3600000">HOUR</button>
                            <button class="time-input-form-gran-days-copy baseline-aggregate-copy radio-type-button" rel="{{tabName}}" type="button" unit="DAYS" value="86400000">DAY</button>
                            {{#if showAggregateAllGranularity}}
                            <button class="time-input-form-gran-aggregate-all-copy baseline-aggregate-copy radio-type-button" rel="{{tabName}}" type="button" unit="aggregateAll" value="0">Aggregate all</button>
                            {{/if}}
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
            <div class="uk-form-row">
                <label class="uk-form-label">
                    Granularity
                </label>
                <div class="uk-button-group radio-buttons">
                    <button class="time-input-form-gran-hours baseline-aggregate radio-type-button uk-active" rel="{{tabName}}" type="button" unit="HOURS" value="3600000">HOUR</button>
                    <button class="time-input-form-gran-days baseline-aggregate radio-type-button" rel="{{tabName}}" unit="DAYS" type="button" value="86400000">DAY</button>
                    {{#if showAggregateAllGranularity}}
                    <button class="time-input-form-gran-aggregate-all baseline-aggregate radio-type-button" rel="{{tabName}}" unit="aggregateAll" type="button" value="0">Aggregate all</button>
                    {{/if}}
                </div>
            </div>
            <div id="{{tabName}}-time-input-form-error" class="time-input-form-error uk-alert uk-alert-danger uk-form-row hidden">
                <p></p>
            </div>
            <div id="{{tabName}}-form-tip" class="uk-alert uk-form-row hidden">
                <p></p>
            </div>

            <div class="uk-form-row">
                <button type="button" id="{{tabName}}-form-submit" class="form-submit-btn uk-button uk-button-primary" rel="{{tabName}}">Go</button>
            </div>
        </form>
    </script>
</section>