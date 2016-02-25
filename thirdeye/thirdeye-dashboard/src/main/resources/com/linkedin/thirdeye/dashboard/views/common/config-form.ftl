<div class="title-box">
<div class="uk-clearfix">
<ul id="current-view-settings" class="uk-display-inline-block">
   <li>
    <!-- Filters Applied revamped -->
    <ul class="filters-applied" style="display: inline-block;max-width: 200px; margin-right: 15px; padding-left: 0px;">Filters Applied:
        <#list dimensions as dimensionName>
            <#assign dimensionDisplay = dimensionAliases[dimensionName]!dimensionName>
            <#assign dimensionValue = (selectedDimensions[dimensionName]!"*")?html>
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
   </li>

<#if (dimensionView.type == "TABULAR")>
    <li>
        <div class="uk-display-inline-block uk-margin-small">
            <div data-uk-button-checkbox>
                <br>
                <button type="button" id="funnel-cumulative" class="uk-button">Cumulative</button>
            </div>
        </div>
    </li>

<#elseif (dimensionView.type == "HEAT_MAP" )>
    <li>
        <div class="uk-margin-medium" style="vertical-align: top;">Metric:<br>
            <div  class="uk-button uk-form-select" data-uk-form-select>
                <span>Metric</span>
                <i class="uk-icon-caret-down"></i>
                <select id="view-metric-selector" class="metric-section-selector">
                </select>
            </div>
        </div>
    </li>
<#elseif (dimensionView.type == "MULTI_TIME_SERIES")>


    <li>
        <div class="uk-margin-medium">Metric:<br>
            <div  class="uk-button uk-form-select" data-uk-form-select>
                <span>Metric</span>
                <i class="uk-icon-caret-down"></i>
                <select id="view-metric-selector" class="metric-section-selector">
                </select>
            </div>
        </div>
    </li>

    <li>
        <div class="uk-margin-medium "><br>
            <div class="uk-button-dropdown" data-uk-dropdown="{mode:'click'}" aria-haspopup="true" aria-expanded="false">
                <button class="uk-button">Dimension <i class="uk-icon-caret-down"></i></button>
                <div class="uk-dropdown uk-dropdown-bottom" style="top: 30px; left: 0px;">
                    <ul id="view-dimension-selector" class="uk-nav uk-nav-dropdown">
                        <#list dimensionView.view.dimensions as dimension>
                            <li><a href="#"><input class="dimension-section-selector" type="checkbox" value="${dimension}">${dimension}</a></li>
                        </#list>
                    </ul>
                </div>
            </div>
        </div>
    </li>

    <li>
        <div class="uk-margin-small">
            <div data-uk-button-checkbox>
                <br>
                <button type="button" id="funnel-cumulative" class="uk-button">Cumulative</button>
            </div>
        </div>
    </li>

</#if>
</ul>

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

        <div class="uk-display-inline-block">
            <label class="uk-form-label">
                Date
            </label>
            <div class="uk-form-icon">
                <i class="uk-icon-calendar"></i>
                <input id="time-input-form-current-date" type="text" data-uk-datepicker="{weekstart:0,format:'YYYY-MM-DD'}">
            </div>
        </div>
        <div id="config-form-time-picker-box" class="uk-display-inline-block hidden">
            <label class="uk-form-label hidden">
                End Time
            </label>
            <div class="uk-form-icon hidden" >
                <i class="uk-icon-clock-o hidden"></i>
                <input id="time-input-form-current-time" class="hidden" type="text" data-uk-timepicker>
            </div>
        </div>
        <div class="uk-display-inline-block">
            <label class="uk-form-label">
                Granularity
            </label>
            <div  class="uk-button-group" data-uk-button-radio>
                <button id="time-input-form-gran-hours" type="button" class="baseline-aggregate uk-button" unit="HOURS" value="3600000" >HOUR</button>
                <button id="time-input-form-gran-days" type="button" class="baseline-aggregate uk-button uk-active" unit="DAYS" value="86400000" >DAY</button>
            </div>
        </div>

        <div class="uk-display-inline-block">
            <label class="uk-form-label">Compare to
            </label>
            <div  class="uk-button uk-form-select" data-uk-form-select>
                <span>Compare:</span>
                <i class="uk-icon-caret-down"></i>
                <select id="time-input-comparison-size">
                    <option class="uk-button" unit="WoW" value="7">WoW</option>
                    <option class="uk-button" unit="Wo2W" value="14" >Wo2W</option>
                    <option class="uk-button" unit="Wo3W" value="21" >Wo3W</option>
                    <option class="uk-button" unit="Wo4W" value="28">Wo4W</option>
                </select>
            </div>
        </div>

        <div class="uk-display-inline-block uk-margin-right">
            <button type="submit" id="time-input-form-submit" class="uk-button uk-button-primary" disabled style="width: 100%">Go</button>
        </div>
    </div>
</form>
</div>

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
<#elseif (dimensionView.type == "TABULAR" || dimensionView.type == "MULTI_TIME_SERIES" )>
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