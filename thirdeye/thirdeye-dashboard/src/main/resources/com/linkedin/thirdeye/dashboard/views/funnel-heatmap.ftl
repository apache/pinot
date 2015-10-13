<script src="/assets/js/thirdeye.funnelheatmap.js"></script>

<#if (funnelViews?size > 0)>
<div id="funnel-heat-map-error"></div>
<div id="custom-funnel-section">
    <i class="uk-icon-spinner uk-icon-spin uk-icon-large" style="margin: 50%;"></i>
</div>
<div id="funnel-thumbnails" class="funnel-thumbnail-container uk-margin-bottom" data-uk-grid data-uk-slider>
    <#list funnelViews as funnel>
        <#assign funnel=funnel>
        
        <div class="funnel">
            <h3 class="metric-list">${funnel.name} </h3>
            <h3 class="metric-list"> (current = ${funnel.current} & baseline = ${funnel.baseline})</h3>

            <table class="uk-table dimension-view-funnel-heat-map-rendered">
                <thead>
                    <tr>
                        <th class="metric-label">Hour</th>
                        <#list funnel.aliasToActualMap?keys as key>
                            <th class="metric-label" data-uk-tooltip>${key}</th>
                        </#list>
                    </tr>
                </thead>
                <#macro heatmapTableBody data isCumulative>
                    <tbody id="${isCumulative?string(funnel.name + '-cumulative',funnel.name)}">
                        <#list data as row>
                            <tr>
                                <td class="funnel-table-time" data-hour="${row.hour}"  currentUTC="${funnel.current}" title="baseline date:${funnel.baseline}">${row.hour}</td>
                                <#list row.ratio as column>
                                    <#if (column??)>
                                        <#assign baselineValue = row.baseline[column?index]>
                                        <#assign currentValue = row.current[column?index]>
                                        <td
                                                class="heat-map-cell custom-tooltip"
                                                data-uk-tooltip
                                                tooltip="${column}"
                                                value="${column}"
                                                title="Baseline Value: ${baselineValue}<br> Current Value: ${currentValue}"
                                                >${(column * 100)?string["0.0"]}%</td>
                                    <#else>
                                        <td class="not-available">N/A</td>
                                    </#if>
                                </#list>
                            </tr>
                        </#list>
                    </tbody>
                </#macro>
                
                <@heatmapTableBody data=funnel.table isCumulative=false/>
                <!-- Uncomment this once the logic for hiding the unselected values is in place -->
                <!-- <@heatmapTableBody data=funnel.cumulativeTable isCumulative=true/> -->
            </table>

        </div>
        
    </#list>


</div>

</#if>
