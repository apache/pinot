<script src="/assets/js/thirdeye.funnelheatmap.js"></script>

<#if (funnelViews?size > 0)>
<div id="funnel-heat-map-error"></div>
<div id="custom-funnel-section">
    <i class="uk-icon-spinner uk-icon-spin uk-icon-large" style="margin: 50%;"></i>
</div>
<div id="funnel-thumbnails" class="funnel-thumbnail-container uk-margin-bottom" data-uk-grid data-uk-slider>
    <#list funnelViews as funnel>
        <div class="funnel">
            <h3 class="metric-list">${funnel.name} </h3>
            <h3 class="metric-list"> (current = ${funnel.current} & baseline = ${funnel.baseline})
            </h3>

                <table class="uk-table dimension-view-heat-map-rendered">
                    <thead>
                        <tr>
                            <th class="metric-label">Hour</th>
                            <#list funnel.aliasToActualMap?keys as key>
                                <th class="metric-label">${key}</th>
                            </#list>
                        </tr>
                    </thead>
                    <tbody>
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
                    </tbody>


                </table>

        </div>
    </#list>


</div>

</#if>
