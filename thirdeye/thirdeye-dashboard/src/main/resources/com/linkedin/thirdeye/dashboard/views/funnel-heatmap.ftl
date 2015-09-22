<script src="/assets/js/thirdeye.funnelheatmap.js"></script>

<#if (funnelViews?size > 0)>
<div class="collapser"><h2>(-) Dashboard(s)</h2></div>
<div id="custom-funnels-container">

        <div id="custom-funnel-title" class="dimension-heat-map-container-title uk-clearfix">

            <form id="funnel-time-input-form" class="uk-form uk-form-stacked uk-float-right">
                <div id="funnel-error" class="uk-alert uk-alert-danger hidden">
                    <p></p>
                </div>

                <div class="uk-margin-small-top uk-margin-bottom">
                    <div class="uk-display-inline-block">
                        <label class="uk-form-label">
                            Current Date
                        </label>
                        <div class="uk-form-icon">
                            <i class="uk-icon-calendar"></i>
                            <input id="funnel-date" type="text" data-uk-datepicker="{format:'YYYY-MM-DD'}">
                        </div>
                    </div>
                    <div class="uk-display-inline-block">
                        <label class="uk-form-label">
                            Baseline
                        </label>
                        <div  class="uk-button-group" data-uk-button-radio>
                            <button type="button" class="funnel-baseline-unit uk-button"  value="86400000" >day(s)</button>
                            <button type="button" class="funnel-baseline-unit uk-button "  value="604800000" >week(s)</button>
                        </div>
                    </div>

                    <div class="uk-display-inline-block">
                        <div class="uk-button-group" data-uk-button-radio>
                            <button type="button" class="funnel-moving-average-size uk-button" unit="WoW" value="7">WoW</button>
                            <button type="button" class="funnel-moving-average-size uk-button" unit="Wo2W" value="14" >Wo2W</button>
                            <button type="button" class="funnel-moving-average-size uk-button" unit="Wo4W" value="28">Wo4W</button>
                        </div>
                    </div>

                    <div class="uk-display-inline-block">
                        <div class="funnel-cumulative" data-uk-button-checkbox><button type="button" id="funnel-cummulative" class="uk-button">cummulative</button>
                        </div>
                    </div>
                    <div class="uk-display-inline-block uk-margin-right">
                        <button type="submit" id="funnel-submit" class="uk-button uk-button-small uk-button-primary ">Go</button>
                    </div>
                </div>
            </form>
        </div>
        <div class="custom-funnel-section" style="min-height:500px;"><i class="uk-icon-spin uk-icon-large" style="margin: auto;"></i></div>

<div id="funnel-thumbnails" class="funnel-thumbnail-container uk-margin-bottom pa" data-uk-grid>

    <#list funnelViews as funnel>
        <div>
            <h3 class="metric-list">
            ${funnel.name} <br> (current = ${funnel.current} & baseline = ${funnel.baseline})
            </h3>

                <table class="uk-table dimension-view-heat-map-rendered">
                    <tr>
                        <td class="metric-label">Hour</td>
                        <#list funnel.aliasToActualMap?keys as key>
                            <td class="metric-label">${key}</td>
                        </#list>
                    </tr>
                    <#list funnel.table as row>
                        <tr>
                            <td>${row.first}</td>
                            <#list row.second as column>
                                <#if (column??)>
                                    <td
                                            class="heat-map-cell custom-tooltip"
                                            tooltip="${column}"
                                            value="${column}"
                                            >${(column * 100)?string["0.0"]}%</td>
                                <#else>
                                    <td class="not-available">N/A</td>
                                </#if>
                            </#list>
                        </tr>
                    </#list>
                </table>

        </div>
    </#list>

</div>
<script>

</script>
</#if>    
