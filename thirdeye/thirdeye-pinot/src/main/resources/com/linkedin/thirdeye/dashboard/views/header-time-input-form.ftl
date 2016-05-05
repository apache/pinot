<section id="time-input-form-section">
    <script id="time-input-form-template" type="text/x-handlebars-template">
        {{#each this as |tabName tabIndex|}}
        <form id="{{tabName}}-form" class="time-input-form uk-form uk-form-stacked"  style="width: 100%;">

            <div class="time-input-form-error uk-alert uk-alert-danger uk-form-row hidden" rel="{{tabName}}">
                <p></p>
            </div>
            <div class="uk-form-row">
                <div data-uk-dropdown="{mode:'click'}" style="position: relative;">
                    <div class="date-time-selector-box">
                        <table class="date-time-selector-box-table">
                            <tr>
                                <td class="date-time-selector-box-current-td">
                                    <span class="current-start-date" rel="{{tabName}}"></span>
                                    <span class="current-start-time" rel="{{tabName}}"></span>
                                    <span class="date-devider">-</span>
                                    <span class="current-end-date" rel="{{tabName}}"></span>
                                    <span class="current-end-time" rel="{{tabName}}"></span>

                                    <div class="comparison-display" rel="{{tabName}}"><label class="comparison-display-label">Compare to:</label><br>
                                        <span class="baseline-start-date" rel="{{tabName}}"></span>
                                        <span  class="baseline-start-time" rel="{{tabName}}"></span>
                                        <span class="date-devider">-</span>
                                        <span class="baseline-end-date" rel="{{tabName}}"></span>
                                        <span class="baseline-end-time" rel="{{tabName}}"></span>
                                    </div>
                                </td>
                                <td class="arrow-down"><i class="uk-icon-caret-down"></i></td>
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
                            <input class="current-start-date-input" rel="{{tabName}}" type="text" placeholder="YYYY-MM-DD" data-uk-datepicker="{ weekstart:0, format:'YYYY-MM-DD'}" style="width:110px;">
                            <input class="current-start-time-input" rel="{{tabName}}"  type="text" data-uk-timepicker="{format:'24h'}" placeholder="HH:MM" style="width:50px;"> -
                            <input class="current-end-date-input" rel="{{tabName}}" type="text" placeholder="YYYY-MM-DD" data-uk-datepicker="{ weekstart:0, format:'YYYY-MM-DD'}" style="width:110px;">
                            <input class="current-end-time-input" rel="{{tabName}}" type="text" data-uk-timepicker="{format:'24h'}" placeholder="HH:MM" style="width:50px;">
                        </div>
                        <div>
                            <label class="bold-label">
                                <input class="time-input-compare-checkbox" rel="{{tabName}}" type="checkbox">
                                Compare to:
                            </label>
                            <select class="compare-mode-selector" rel="{{tabName}}">
                                <option unit="WoW" value="7">WoW</option>
                                <option unit="Wo2W" value="14">Wo2W</option>
                                <option unit="Wo3W" value="21">Wo3W</option>
                                <option unit="Wo4W" value="28">Wo4W</option>
                                <option unit="1" value="1">Custom</option>
                            </select>
                        </div>
                        <div class="time-input-custom-compare uk-margin-small" rel="{{tabName}}">
                            <input class="baseline-start-date-input"  rel="{{tabName}}" type="text" placeholder="YYYY-MM-DD" data-uk-datepicker="{ weekstart:0, format:'YYYY-MM-DD'}" style="width:110px;">
                            <input class="baseline-start-time-input" rel="{{tabName}}" type="text" data-uk-timepicker="{format:'24h'}" placeholder="HH:MM" style="width:50px;"> -
                            <input class="baseline-end-date-input uk-margin-small" rel="{{tabName}}" type="text" placeholder="YYYY-MM-DD" data-uk-datepicker="{weekstart:0, format:'YYYY-MM-DD'}" style="width:110px;">
                            <input class="baseline-end-time-input" rel="{{tabName}}" type="text" data-uk-timepicker="{format:'24h'}" placeholder="HH:MM" style="width:50px;">
                        </div>

                        <label class="uk-form-label">
                            Granularity
                        </label>
                        <div class="uk-button-group radio-buttons" rel="{{tabName}}">
                            <button class="time-input-form-gran-hours-copy baseline-aggregate-copy  radio-type-button uk-active" rel="{{tabName}}" type="button" unit="HOURS" value="3600000">HOUR</button>
                            <button class="time-input-form-gran-days-copy baseline-aggregate-copy radio-type-button" rel="{{tabName}}" type="button" unit="DAYS" value="86400000">DAY</button>
                            <button class="time-input-form-gran-aggregate-all-copy baseline-aggregate radio-type-button {{hideElem 'dashboard' tabName}}" rel="{{tabName}}" type="button" unit="aggregateAll" value="0">Overall timerange</button>
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
            <p class="compare-mode" rel="{{tabName}}" class="uk-form-row hidden"></p>
            <div class="uk-form-row">
                <label class="uk-form-label">
                    Granularity
                </label>
                <div class="uk-button-group radio-buttons">
                    <button class="time-input-form-gran-hours baseline-aggregate radio-type-button uk-active" rel="{{tabName}}" type="button" unit="HOURS" value="3600000">HOUR</button>
                    <button class="time-input-form-gran-days baseline-aggregate radio-type-button" rel="{{tabName}}" unit="DAYS" type="button" value="86400000">DAY</button>
                    <button class="time-input-form-gran-aggregate-all baseline-aggregate radio-type-button {{hideElem 'dashboard' tabName}}" rel="{{tabName}}" unit="aggregateAll" type="button" value="0">Overall timerange</button>
                </div>
            </div>

            <div class="uk-form-row">
                <button type="button" class="time-input-form-submit uk-button uk-button-primary" rel="{{tabName}}" disabled>Go</button>
            </div>
        </form>
        {{/each}}

    </script>
</section>