<div class="title-box uk-clearfix">
<!-- Filters Applied revamped -->
<ul class="filters-applied" style="display: inline-block;">Filters Applied:
    <#list dimensions as dimensionName>
        <#assign dimensionDisplay = dimensionAliases[dimensionName]!dimensionName>
        <#assign dimensionValue = selectedDimensions[dimensionName]!"*">
            <#if dimensionValue == "*">
            <#--<span>${dimensionDisplay}:</span><br> ALL-->
            <#elseif dimensionValue == "?">
                <li>
                    <a href="#" class="dimension-link" dimension="${dimensionName}" dimension-value=""><span>${dimensionDisplay}:</span> OTHER</a>
                </li>
            <#elseif dimensionValue == "">
                <li>
                    <a href="#" class="dimension-link" dimension="${dimensionName}" dimension-value=""><span>${dimensionDisplay}:</span> UNKNOWN</a>
                </li>
            <#else>
                <li>
                    <a href="#" class="dimension-link" dimension="${dimensionName}" dimension-value="${dimensionValue}"><span>${dimensionDisplay}:</span> ${dimensionValue}</a>
                </li>
            </#if>

    </#list>
</ul>



<#if (dimensionView.type == "TABULAR")>
<div class="uk-display-inline-block uk-margin-small">
    <div data-uk-button-checkbox>
        <button type="button" id="funnel-cumulative" class="uk-button">Cumulative</button>
    </div>
</div>
<!-- Metric selection dropdown -->
<#elseif (dimensionView.type == "HEAT_MAP")>
<div class="uk-display-inline-block uk-margin-medium">Metric:<br>
    <div  class="uk-button uk-form-select" data-uk-form-select>
        <span>Metric</span>
        <i class="uk-icon-caret-down"></i>
        <select id="view-metric-selector" class="metric-section-selector">
            <#list dimensionView.view.metricNames as metric>
                <option value="${metric}">${metric}</option>
            </#list>
        </select>
    </div>
</div>
<#elseif (dimensionView.type == "MULTI_TIME_SERIES")>
<div class="uk-display-inline-block uk-margin-medium">Dimension:<br>
    <div  class="uk-button uk-form-select" data-uk-form-select>
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


<form class="time-input-form uk-form uk-float-right uk-form-stacked uk-clearfix">


    <div class="uk-margin-small-top uk-margin-bottom">
        <div id="time-input-form-error" class="uk-alert uk-alert-danger hidden">
            <p></p>
        </div>

        <div class="uk-display-inline-block">
            <button class="select-button uk-button" type="button" style="width: 200px;"><span>Select Filters </span><i class="uk-icon-caret-down"></i></button>
            <div class="dimension-combination hidden">
                <a class="close" href="#">
                    <i class="uk-icon-close"></i>
                </a>
                <br>
                <table>
                    <tr>
                        <td>Select Filters: </td>
                        <#list dimensions as dimensionName>
                            <#assign dimensionDisplay = dimensionAliases[dimensionName]!dimensionName>
                            <#assign dimensionValue = selectedDimensions[dimensionName]!"*">
                            <td style="position:relative;">
                                <span>${dimensionDisplay}:</span><br>

                                <button type="button" class="dimension-selector uk-button">Select Values <i class="uk-icon-caret-down"></i> </button>
                                <div class="hidden" style="position:absolute; top:50px; left:0px; z-index:100; background-color: #f5f5f5; border: 1px solid #ccc; padding:5px;">
                                    <ul style="list-style-type: none; padding-left:0; width:250px;">
                                        <#list dimensionView.dimensionValuesOptions[dimensionName]![] as dimensionValue>
                                            <li style="overflow:hidden;">
                                                <#assign dimensionValueDisplay=dimensionValue?html>
                                                <#if dimensionValue=="">
                                                    <#assign dimensionValueDisplay="UNKNOWN">
                                                <#elseif dimensionValue=="?">
                                                    <#assign dimensionValueDisplay="OTHER">
                                                </#if>
                                                <input class="panel-dimension-option" type="checkbox" dimension-name="${dimensionName}" dimension-value="${dimensionValue?html}"/> ${dimensionValueDisplay?html}
                                            </li>
                                        </#list>

                                    </ul>
                                </div>

                            </td>
                        </#list>
                    </tr>
                </table>

            </div>
        </div>
        <div class="uk-display-inline-block hidden" style="position: relative;">
            <button id="time-input-metrics" type="button" class="uk-button hidden" style="width: 200px;">Select Metrics <i class="uk-icon-caret-down"></i> </button>
            <div id="time-input-metrics-panel" class="hidden" style="position:absolute; top:30px; left:0px; z-index:100; background-color: #f5f5f5; border: 1px solid #ccc; padding:5px;">
                <ul style="list-style-type: none; padding-left:0; width:250px;">
                <#list collectionSchema.metrics as metric>
                    <li style="overflow:hidden;">
                        <input class="panel-metric" type="checkbox" value="${metric}"/>${collectionSchema.metricAliases[metric_index]!metric}
                    </li>
                </#list>
                </ul>
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
                End Date
            </label>
            <div class="uk-form-icon">
                <i class="uk-icon-calendar"></i>
                <input id="time-input-form-current-date" type="text" data-uk-datepicker="{format:'YYYY-MM-DD'}">
            </div>
        </div>

        <div class="uk-display-inline-block">
            <label class="uk-form-label">
                Granularity
            </label>
            <div  class="uk-button-group" data-uk-button-radio>
                <button type="button" class="baseline-aggregate uk-button" unit="HOURS" value="3600000" >HOUR</button>
                <button type="button" class="baseline-aggregate uk-button uk-active" unit="DAYS" value="86400000" >DAY</button>
            </div>
        </div>

    <#if dimensionView.type == "TABULAR">
        <div class="uk-form-label uk-display-inline-block">Compare to:<br>
            <div  class="uk-button uk-form-select" data-uk-form-select>
                <span>Compare:</span>
                <i class="uk-icon-caret-down"></i>
                <select id="time-input-comparison-size">
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


<div id="current-view-settings" class="uk-clearfix" style="padding-right:20px;">

<#if (dimensionView.type == "HEAT_MAP")>
    <ul class="heatmap-tabs uk-tab" data-uk-tab>
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

</div>