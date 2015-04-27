<div class="uk-panel uk-panel-box" data-uk-sticky>
    <h3 class="uk-panel-title"><a href="/">ThirdEye</a></h3>
    <ul class="uk-nav uk-nav-side">
        <li>
            <form id="sidenav-time-input-form" class="uk-form uk-form-stacked">
                <div id="sidenav-error" class="uk-alert uk-alert-danger">
                    <p></p>
                </div>

                <div class="uk-form-row">
                    <label class="uk-form-label">Metric(s)</label>
                    <#list collectionSchema.metrics as metric>
                        <label>
                            <input class="sidenav-metric" type="checkbox" value="${metric}"/>
                            ${metric}
                        </label>
                        <br/>
                    </#list>

                    <div id="sidenav-derived-metrics">
                        <label class="uk-form-label">Derived Metric(s)
                          <button id="sidenav-derived-metrics-add" class="uk-button uk-button-mini">
                              <i class="uk-icon-plus-circle"></i>
                          </button>
                        </label>
                        <div id="sidenav-derived-metrics-list"></div>
                    </div>
                </div>

                <div class="uk-form-row">
                    <label class="uk-form-label">Date / Time<br/><span id="sidenav-timezone"></span></label>
                    <div class="uk-form-icon">
                        <i class="uk-icon-calendar"></i>
                        <input id="sidenav-date" type="text" data-uk-datepicker="{format:'YYYY-MM-DD'}">
                    </div>
                </div>

                <div class="uk-form-row">
                    <div class="uk-form-icon" data-uk-timepicker>
                        <i class="uk-icon-clock-o"></i>
                        <input id="sidenav-time" type="text">
                    </div>
                </div>

                <div class="uk-form-row">
                    <label class="uk-form-label">Baseline</label>
                    <div class="uk-form-controls">
                        <input id="sidenav-baseline-size" type="number" min="0" value="1" class="uk-form-width-small" />
                        <select id="sidenav-baseline-unit">
                            <#-- value is unit in milliseconds -->
                            <option unit="HOURS" value="3600000">hour(s)</option>
                            <option unit="DAYS" value="86400000">day(s)</option>
                            <option unit="WEEKS" value="604800000" selected="selected">week(s)</option>
                            <option unit="MONTHS" value="2592000000">month(s)</option>
                        </select>
                    </div>
                </div>

                <div class="uk-form-row">
                  <label class="uk-form-label">Aggregate</label>
                  <div class="uk-form-controls">
                    <input id="sidenav-aggregate-size" type="number" min="1" value="1" class="uk-form-width-small" />
                    <select id="sidenav-aggregate-unit">
                      <option value="HOURS" selected="selected">hour(s)</option>
                      <option value="DAYS">day(s)</option>
                    </select>
                  </div>
                </div>
                
                <div class="uk-form-row">
                    <label class="uk-form-label">
                        Moving Average
                        <input type="checkbox" id="sidenav-moving-average"/>
                    </label>
                    <div id="sidenav-moving-average-controls" class="uk-form-controls">
                        <input id="sidenav-moving-average-size" type="number" min="1" value="1" class="uk-form-width-small" />
                        <select id="sidenav-moving-average-unit">
                          <option value="HOURS" selected="selected">hour(s)</option>
                          <option value="DAYS">day(s)</option>
                        </select>
                    </div>
                </div>

                <div class="uk-form-row">
                    <button id="sidenav-submit" class="uk-button uk-button-small uk-button-primary">Go</button>
                </div>
            </form>
        </li>
    </ul>
</div>
