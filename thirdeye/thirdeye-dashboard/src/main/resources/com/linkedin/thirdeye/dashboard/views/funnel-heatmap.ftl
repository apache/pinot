<script src="/assets/js/thirdeye.funnelheatmap.js"></script>

<#if (funnelViews?size > 0)>
<div class="collapser"><h2>(-) Dashboard(s)</h2></div>
<div id="custom-funnels-container">
    <div class="custom-funnel-section" style="min-height:600px;"><i class="uk-icon-spin uk-icon-large" style="margin: auto;"></i></div>
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
</#if>    
