<section id="self-service-forms-section">
    <script id="self-service-template" type="text/x-handlebars-template">
        <ul id="self-service-forms" class="uk-switcher">
            <li id="">
                <div  class="title-box full-width"><h3>Configure anomaly alerts</h3></div>
                <form id="configure-alert-form" class="uk-form">
                    <div class="uk-form-row">
                        <label class="uk-form-label bold-label required">Rule
                        </label>
                        <input id="rule" type="text" maxlength="80">
                    </div>
                    <div class="uk-form-row">
                        <label class="uk-form-label bold-label required">Dataset</label>
                        <div data-uk-dropdown="{mode:'click'}" aria-haspopup="true" aria-expanded="false" class="uk-button-group uk-display-inline-block">
                            <div class="selected-dataset uk-button" value="">Select dataset
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
                            <div id="selected-metric-manage-alert" class="uk-button" value="">Metric</div>
                            <div class="uk-button uk-button-primary" type="button"><i class="uk-icon-caret-down"></i>
                            </div>
                            <div class="uk-dropdown uk-dropdown-small uk-dropdown-bottom" style="top: 30px; left: 0px;">
                                <ul class="metric-list uk-nav uk-nav-dropdown single-select">
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
                                <ul class="uk-nav uk-nav-dropdown single-select">
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

                    <div class="uk-form-row">
                        <div id="{{tabName}}-view-single-dimension-selector" class="view-single-dimension-selector uk-display-inline-block" rel="{{tabName}}">
                            <label class="uk-form-label">in </label>
                            <div data-uk-dropdown="{mode:'click'}" aria-haspopup="true" aria-expanded="false" class="uk-button-group">
                                <div id="selected-dimension" class="uk-button">dimension</div>
                                <button class="add-single-dimension-btn uk-button uk-button-primary" type="button"><i class="uk-icon-caret-down"></i></button>
                                <div class="uk-dropdown uk-dropdown-small">
                                    <ul class="dimension-list uk-nav uk-nav-dropdown single-select">
                                    </ul>
                                </div>
                            </div>
                        </div>

                        <!--<div class="filter-selector-manage-alert uk-display-inline-block">
                            <div id="{{tabName}}-add-filter-manage-alert" class="add-filter-manage-alert add-btn uk-display-inline-block hidden" rel="{{tabName}}" data-uk-dropdown="{mode:'click'}">
                                <button class="uk-button uk-button-primary" type="button"><i class="uk-icon-plus"></i></button>
                                <div id="filter-panel-manage-alert" class="filter-panel uk-dropdown" rel="{{tabName}}" style="width:300px; display:none;">
                                    <i class="close-dropdown-btn uk-icon-close" style="position: absolute; right:5px; top: 5px;"></i>
                                    <a href="#" class="uk-dropdown-close">
                                        <button id="apply-filter-btn-manage-alert" class="apply-filter-btn uk-button uk-button-primary"  rel="{{tabName}}"  style="float:right; margin: 5px;" disabled>Apply
                                        </button>
                                    </a>
                                    <span class="dimension-values-manage-alert"></span>
                                </div>
                            </div>
                            <ul  id="selected-filters-list-manage-alert" class="uk-display-inline-block" rel="{{tabName}}"></ul>
                        </div>-->
                    </div>

                    <div class="view-filter-selector  uk-display-inline-block" rel="{{tabName}}">
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

                    <!-- EMAIL ADDRESS CONFIG currently not supported by the back end
                    <div class="uk-form-row">
                        <input class="" rel="{{tabName}}" type="checkbox" checked><span>Send me an email when this alert triggers. Email address: </span><input type="email" autocomplete="on">
                    </div>-->

                    <!-- ELEMENTS NEEDED FOR KALMAN STATISTICS
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
                        <span id="local-timezone"></span>
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

                    <div>
                        <button type="button" id="save-alert" class="uk-button uk-button-primary" rel="{{tabName}}">Save Alert</button>
                        <button class="uk-button">Cancel</button>
                    </div>
                </form>

            </li>
            <li id="">Add dataset configuration</li>
        </ul>
    </script>
</section>