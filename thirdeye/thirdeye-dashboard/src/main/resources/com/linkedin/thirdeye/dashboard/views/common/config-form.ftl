<div class="title-box uk-clearfix">
    <#if (dimensionView.type == "TABULAR")>
    <div class="uk-display-inline-block">
        <div data-uk-button-checkbox class="hidden">
            <button type="button" id="funnel-cumulative" class="uk-button">Cumulative</button>
        </div>
    </div>
    <!-- Metric selection dropdown -->
    <#elseif (dimensionView.type == "HEAT_MAP")>
        <div style="display: inline-block">Metric:<br>
            <div  class="uk-button uk-form-select" data-uk-form-select>
                <span>Metric</span>
                <i class="uk-icon-caret-down"></i>
                <select id="view-metric-selector" class="section-selector">
                <#list dimensionView.view.metricNames as metric>
                    <option value="${metric}">${metric}</option>
                </#list>
                </select>
            </div>
        </div>
    <#elseif (dimensionView.type == "MULTI_TIME_SERIES")>
        <div style="display: inline-block">Dimension:<br>
            <div  class="uk-button uk-form-select uk-left" data-uk-form-select>
                <span>Dimension</span>
                <i class="uk-icon-caret-down"></i>
                <select id="view-dimension-selector" class="section-selector">
                    <#list dimensionView.view.dimensions as dimension>
                        <option value="${dimension}">${dimension}</option>
                    </#list>
                </select>
            </div>
        </div>
    </#if>

    <!-- Dimension Combination -->
    <#list (metricView.view.metricTables)!metricTables as metricTable>
    <#assign dimensions = metricTable.dimensionValues>
    <#assign dimensionAliases = (metricView.view.dimensionAliases)!dimensionAliases>
        <ul class="dimension-combination" style="display: inline-block;">Filters:
        <#list dimensions?keys as dimensionName>
            <#assign dimensionValue = dimensions[dimensionName]>
            <#assign dimensionDisplay = dimensionAliases[dimensionName]!dimensionName>

            <#if dimensionValue == "*">
            <#--<span>${dimensionDisplay}:</span><br> ALL-->
            <#elseif dimensionValue == "?">
                <li>
                    <a href="#" class="dimension-link" dimension="${dimensionName}"><span>${dimensionDisplay}:</span> OTHER</a>
                </li>
            <#else>
                <li>
                    <a href="#" class="dimension-link" dimension="${dimensionName}"><span>${dimensionDisplay}:</span> ${dimensions[dimensionName]}</a>
                </li>
            </#if>

        </#list>
        </ul>
    </#list>



    <form class="time-input-form uk-form uk-form-stacked uk-float-right">
        <div id="time-input-form-error" class="uk-alert uk-alert-danger hidden">
            <p></p>
        </div>
        <div class="uk-margin-small-top uk-margin-bottom">
            <div class="uk-display-inline-block">
                <label class="uk-form-label">
                    End Date
                </label>
                <div class="uk-form-icon">
                    <i class="uk-icon-calendar"></i>
                    <input id="time-input-form-current-date" type="text" data-uk-datepicker="{format:'YYYY-MM-DD'}">
                </div>
            </div>
            <#if (dimensionView.type == "HEAT_MAP" || dimensionView.type == "MULTI_TIME_SERIES")>
            <div class="uk-display-inline-block">
                <label class="uk-form-label">
                    Start Date
                </label>
                <div class="uk-form-icon">
                    <i class="uk-icon-calendar"></i>
                    <input id="time-input-form-baseline-date" type="text" data-uk-datepicker="{format:'YYYY-MM-DD'}">
                </div>
            </div>
            </#if>
            <div class="uk-display-inline-block">
                <label class="uk-form-label">
                    Granularity
                </label>
                <div  class="uk-button-group" data-uk-button-radio>
                    <button type="button" class="baseline-aggregate uk-button" unit="HOURS" value="3600000" >hour(s)</button>
                    <button type="button" class="baseline-aggregate uk-button uk-active" unit="DAYS" value="86400000" >day(s)</button>
                </div>
            </div>

            <#if (dimensionView.type == "HEAT_MAP" || dimensionView.type == "TABULAR")>
            <div id="time-input-form-moving-average" class="uk-form-label" style="display: inline-block">Overlay<br>
            <div  class="uk-button uk-form-select" data-uk-form-select>
                <span>Moving Average:</span>
                <i class="uk-icon-caret-down"></i>
                    <select id="moving-average-size">
                        <option class="uk-button" unit="WoW" value="">None</option>
                        <option class="uk-button" unit="WoW" value="7">WoW</option>
                        <option class="uk-button" unit="Wo2W" value="14" >Wo2W</option>
                        <option class="uk-button" unit="Wo4W" value="28">Wo4W</option>
                    </select>
                </div>
            </div>
            </#if>

            <div class="uk-display-inline-block uk-margin-right">
                <button type="submit" class="time-input-form-submit uk-button uk-button-small uk-button-primary ">Go</button>
            </div>
        </div>
    </form>

    <#if (dimensionView.type == "HEAT_MAP")>
        <ul class="uk-tab heatmap-tabs" data-uk-tab>
            <li class="uk-active">
                <a href="#">Heatmap</a>
            </li>
            <li>
                <a href="#">Datatable</a>
            </li>
        </ul>
    <#elseif (dimensionView.type == "TABULAR")>
        <ul class="uk-tab funnel-tabs" data-uk-tab>
            <li class="uk-active">
                <a href="#">Summary</a>
            </li>
            <li>
                <a href="#">Details</a>
            </li>
        </ul>
    </#if>
</div>