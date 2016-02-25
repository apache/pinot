<script src="/assets/js/thirdeye.funnelheatmap.js"></script>

<#if (dimensionView.view.funnels?size > 0)>
<div id="funnel-heat-map-error"></div>
<div id="custom-funnel-section">
    <i class="uk-icon-spinner uk-icon-spin uk-icon-large" style="margin: 50%;"></i>
</div>
<div id="funnel-thumbnails" class="funnel-thumbnail-container uk-margin-bottom" data-uk-grid data-uk-slider>
    <#list dimensionView.view.funnels as funnel>
        <#assign funnel=funnel>
        
        <div class="funnel">
            <h3 class="metric-list">${funnel.name} </h3>
            <h3 class="metric-list" currentUTC=${funnel.current} baselineUTC=${funnel.baseline} > (current = ${funnel.current} & baseline = ${funnel.baseline})</h3>

            <table class="uk-table dimension-view-funnel-heat-map-rendered">
                <thead>
                    <tr>
                        <th>Date and Time</th>
                        <#list funnel.aliasToActualMap?keys as key>
                            <th class="metric-label" title="${funnel.aliasToActualMap[key]}" style="cursor:pointer;">${key}</th>
                        </#list>
                    </tr>
                    <tr class="subheader hidden">
                        <th></th>
                        <#list funnel.aliasToActualMap?keys as key>
                            <th class="details-cell hidden" title="${funnel.aliasToActualMap[key]}">Baseline</th>
                            <th class="details-cell hidden" title="${funnel.aliasToActualMap[key]}">Current</th>
                            <th title="${funnel.aliasToActualMap[key]}">Ratio</th>
                        </#list>
                    </tr>
                </thead>
                <#macro heatmapTableBody data isCumulative>
                    <tbody class="${isCumulative?string('cumulative-values hidden', 'hourly-values')}">
                        <#list data as row>
                            <tr  currentUTC="${row.currentTime}">
                                <td class="funnel-table-time" currentUTC="${row.currentTime}" title="${row.baselineTime}">${row.currentTime}</td>
                                <#list 0..(row.numColumns-1) as i>
                                    <#if (row.ratio[i]??)>
                                        <#assign ratioValue = row.ratio[i]>
                                        <#assign baselineValue = row.baseline[i]>
                                        <#assign currentValue = row.current[i]>
                                        <td class="details-cell hidden" style="border-left: 1px solid #ddd;">${baselineValue}</td>
                                        <td class="details-cell hidden">${currentValue}</td>
                                        <td
                                                class="heat-map-cell custom-tooltip"
                                                tooltip="${ratioValue}"
                                                value="${ratioValue}"
                                                title="Baseline Value: ${baselineValue} Current Value: ${currentValue}"
                                                >${(ratioValue * 100)?string["0.0"]}%</td>
                                    <#else>
                                        <td class="details-cell hidden">N/A</td>
                                        <td class="details-cell hidden">N/A</td>
                                        <td class="not-available">N/A</td>
                                    </#if>
                                </#list>
                            </tr>
                        </#list>
                    </tbody>
                </#macro>
                
                <@heatmapTableBody data=funnel.table isCumulative=false/>
                <@heatmapTableBody data=funnel.cumulativeTable isCumulative=true/>
            </table>
        </div>
    </#list>
</div>
</#if>

