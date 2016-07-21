<section id="anomaly-function-form-section">
<script id="anomaly-function-form-template" type="text/x-handlebars-template">
    <form id="configure-anomaly-function-form" class="uk-form" >
        {{#if id}}
        <div class="uk-form-row">Function id: {{id}}</div>
        {{else}}
        <div  class="title-box full-width" style="margin-top:15px;">
            <h3>Create anomaly function</h3>
        </div>
        {{/if}}
        <div class="uk-form-row">
            <label class="uk-form-label bold-label required">Name
            </label>
            <input id="name" type="text" maxlength="80" {{#if functionName}}value="{{functionName}}"{{/if}}>
        </div>
        <div class="uk-form-row">
            <label class="uk-form-label bold-label required">Dataset</label>
            <div data-uk-dropdown="{mode:'click'}" aria-haspopup="true" aria-expanded="false" class="uk-button-group uk-display-inline-block">
                <div class="selected-dataset uk-button"
                     value="{{#if collection}}
                                {{collection}}
                           {{/if}}
                ">
                    {{#if collection}}
                        {{collection}}
                    {{else}}
                        Select dataset
                    {{/if}}
                </div>
                <div class="uk-button uk-button-primary" type="button"><i class="uk-icon-caret-down"></i>
                </div>
                <div class="landing-dataset uk-dropdown uk-dropdown-small uk-dropdown-bottom" style="top: 30px; left: 0px;">
                </div>
            </div>
        </div>

        <div class="uk-display-inline-block">Alert me when </div>
        <div id="metric-selector-manage-alert" class="uk-form-row uk-display-inline-block">
            <div data-uk-dropdown="{mode:'click'}" aria-haspopup="true" aria-expanded="false" class="uk-button-group uk-display-inline-block">
                <div id="selected-metric-manage-alert" class="uk-button"
                value="
                    {{#if metric}}
                        {{metric}}
                    {{/if}}
                ">
                    {{#if metric}}
                       {{metric}}
                    {{else}}
                        Metric
                    {{/if}}
                </div>
                <div class="uk-button uk-button-primary" type="button"><i class="uk-icon-caret-down"></i>
                </div>
                <div class="uk-dropdown uk-dropdown-small uk-dropdown-bottom" style="top: 30px; left: 0px;">
                    <ul class="metric-list uk-nav uk-nav-dropdown single-select">
                    </ul>
                </div>
            </div>
        </div>
        <div id="anomaly-condition-selector" class="uk-form-row uk-form-row uk-display-inline-block" rel="self-service">
            <div data-uk-dropdown="{mode:'click'}" aria-haspopup="true" aria-expanded="false" class="uk-button-group uk-display-inline-block">
                <div id="selected-anomaly-condition" class="uk-button" value="">Condition
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
        <span> by </span>
        <div class="uk-form-row uk-form-row uk-display-inline-block" rel="self-service">
            <input id="anomaly-threshold" type="text" placeholder="threshold (1-100)" ><span>%</span>
        </div>
        <div id="anomaly-compare-mode-selector uk-display-inline-block" class="uk-form-row uk-form-row uk-display-inline-block" rel="self-service">
            <div data-uk-dropdown="{mode:'click'}" aria-haspopup="true" aria-expanded="false" class="uk-button-group uk-display-inline-block">
                <div id="selected-anomaly-compare-mode" class="uk-button" value="w/w">WoW</div>
                <div class="uk-button uk-button-primary" type="button"><i class="uk-icon-caret-down"></i>
                </div>
                <div class="uk-dropdown uk-dropdown-small uk-dropdown-bottom" style="top: 30px; left: 0px;">
                    <ul class="uk-nav uk-nav-dropdown single-select">
                        <li class="anomaly-compare-mode-option" value="w/w"><a href="#" class="uk-dropdown-close">WoW</a></li>
                        <li class="anomaly-compare-mode-option" value="w/2w"><a href="#" class="uk-dropdown-close">Wo2W</a></li>
                        <li class="anomaly-compare-mode-option" value="w/3w"><a href="#" class="uk-dropdown-close">Wo3W</a></li>
                    </ul>
                </div>
            </div>
        </div>
        <span>for</span>
        <input id="monitoring-window-size" class="thin-input" type="number" {{#if windowSize}}value="{{windowSize}}"{{/if}}>
        <span>consecutive</span>
        <div id="monitoring-window-unit-selector uk-display-inline-block" class="uk-form-row uk-form-row uk-display-inline-block" rel="self-service">
            <div data-uk-dropdown="{mode:'click'}" aria-haspopup="true" aria-expanded="false" class="uk-button-group uk-display-inline-block">
                <div id="selected-monitoring-window-unit" class="uk-button" value="HOURS">HOUR(S)</div>
                <div class="uk-button uk-button-primary" type="button"><i class="uk-icon-caret-down"></i></div>
                <div class="uk-dropdown uk-dropdown-small uk-dropdown-bottom" style="top: 30px; left: 0px;">
                    <ul class="uk-nav uk-nav-dropdown single-select">
                        <li class="monitoring-window-unit-option" value="HOURS"><a href="#" class="uk-dropdown-close">HOUR(S)</a></li>
                        <li class="monitoring-window-unit-option" value="DAYS"><a href="#" class="uk-dropdown-close" >DAY(S)</a></li>
                    </ul>
                </div>
            </div>
        </div>

        <div class="uk-form-row">
            <div id="self-service-view-single-dimension-selector" class="view-single-dimension-selector uk-display-inline-block" rel="self-service">
                <label class="uk-form-label">in dimension</label>
                <div data-uk-dropdown="{mode:'click'}" aria-haspopup="true" aria-expanded="false" class="uk-button-group">
                    <div id="selected-dimension" class="uk-button" value="">All</div>
                    <button class="add-single-dimension-btn uk-button uk-button-primary" type="button"><i class="uk-icon-caret-down"></i></button>
                    <div class="uk-dropdown uk-dropdown-small">
                        <ul class="dimension-list uk-nav uk-nav-dropdown single-select">
                        </ul>
                    </div>
                </div>
            </div>
        </div>
        <div class="uk-form-row">
            <div class="view-filter-selector  uk-display-inline-block" rel="self-service">
                <label class="uk-form-label  uk-display-inline-block">Filters</label>
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

        <!-- EMAIL ADDRESS CONFIG currently not supported by the back end
        <div class="uk-form-row">
            <input class="" rel="self-service" type="checkbox" checked><span>Send me an email when this alert triggers. Email address: </span><input type="email" autocomplete="on">
        </div>-->

        <div class="uk-form-row">
            <span class="uk-form-label uk-display-inline-block">Monitor the data every </span>
            <input id="monitoring-repeat-size" type="number" class="thin-input" {{#if repeatEverySize}}value="{{repeatEverySize}}"{{/if}}>
            <div data-uk-dropdown="{mode:'click'}" aria-haspopup="true" aria-expanded="false" class="uk-button-group uk-display-inline-block">
                <div id="selected-monitoring-repeat-unit" class="uk-button" value="HOURS">HOUR(S)</div>
                <div class="uk-button uk-button-primary" type="button"><i class="uk-icon-caret-down"></i>
                </div>
                <div class="uk-dropdown uk-dropdown-small uk-dropdown-bottom" style="top: 30px; left: 0px;">
                    <ul class="uk-nav uk-nav-dropdown  single-select">
                        <li class="anomaly-monitoring-repeat-unit-option" value="HOURS" ><a href="#" class="uk-dropdown-close">HOUR(S)</a></li>
                        <li class="anomaly-monitoring-repeat-unit-option" value="DAYS" ><a href="#" class="uk-dropdown-close">DAY(S)</a></li>
                    </ul>
                </div>
            </div>
                    <span id="monitoring-schedule" class="hidden">
                        <span> at
                        </span>
                        <input id="monitoring-schedule-time" class="thin-input" type="text" data-uk-timepicker="{format:'24h'}" placeholder="HH:MM">
                        <span id="local-timezone"></span>
                    </span>
        </div>

        <div class="uk-form-row">
            <input id="active-alert" rel="self-service" type="checkbox"  {{#if isActive}}checked{{/if}}><span> Keep this alert active.</span>
        </div>

        <div id="manage-alert-error" class="uk-alert uk-alert-danger hidden" rel="self-service">
            <p></p>
        </div>
        <div id="manage-alert-success" class="uk-alert uk-alert-success hidden" rel="self-service">
            <p></p>
        </div>

        <div>
            {{#if id}}
            <button type="button" id="update-anomaly-function" class="uk-button uk-button-primary" rel="self-service">Create</button>
            <button type="button"  class="uk-button uk-modal-close">Cancel</button>
            {{else}}
            <button type="button" id="create-anomaly-function" class="uk-button uk-button-primary" rel="self-service">Create</button>
            <button type="button" id="clear-create-form"  class="uk-button">Clear</button>
            {{/if}}
        </div>
    </form>
</script>
</section>