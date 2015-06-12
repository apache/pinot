<#-- stand-alone -->
<#if (!metricView??)>
    <#include "../common/style.ftl">
    <#include "../common/script.ftl">
</#if>

<script src="/assets/js/thirdeye.metric.table.js"></script>

<div id="metric-table-area">
    <#if (((metricView.view.metricTables)!metricTables)?size == 0)>
        <div class="uk-alert uk-alert-warning">
            <p>No data available</p>
        </div>
    </#if>

    <#list (metricView.view.metricTables)!metricTables as metricTable>
        <#assign dimensions = metricTable.dimensionValues>
        <#include "../common/dimension-header.ftl">
        <table class="uk-table">
            <thead>
                <tr>
                    <th></th>
                    <#assign groupIdx = 0>
                    <#list (metricView.view.metricNames)!metricNames as metricName>
                        <#assign groupId = (groupIdx % 2)>
                        <th colspan="3" class="uk-text-center metric-table-group-${groupId}">${metricName}</th>
                        <#assign groupIdx = groupIdx + 1>
                    </#list>
                </tr>
                <tr>
                    <th></th>
                    <#assign groupIdx = 0>
                    <#list (metricView.view.metricNames)!metricNames as metricName>
                        <#assign groupId = (groupIdx % 2)>
                        <th class="metric-table-group-${groupId}">Current</th>
                        <th class="metric-table-group-${groupId}">Baseline</th>
                        <th class="metric-table-group-${groupId}">Ratio</th>
                        <#assign groupIdx = groupIdx + 1>
                    </#list>
                </tr>
            </thead>

            <tbody>
                <#list metricTable.rows as row>
                    <tr>
                        <#-- This renders time in UTC (moment.js used to convert to local) -->
                        <td class="metric-table-time" title="${row.baselineTime}" currentUTC="${row.currentTime}">${row.currentTime}</td>
                        <#list 0..(row.numColumns-1) as i>
                            <#assign groupId = (i % 2)>
                            <td class="metric-table-group-${groupId}">${row.current[i]?string!"N/A"}</td>
                            <td class="metric-table-group-${groupId}">${row.baseline[i]?string!"N/A"}</td>
                                <#if row.ratio[i]??>
                                    <td class="
                                        ${(row.ratio[i] < 0)?string('metric-table-down-cell', '')}
                                        ${(row.ratio[i] == 0)?string('metric-table-same-cell', '')}
                                        metric-table-group-${groupId}
                                    ">
                                    ${(row.ratio[i] * 100)?string["0.00"] + "%"}
                                    </td>
                                <#else>
                                    <td class="metric-table-group-${groupId}">N/A</td>
                                </#if>
                        </#list>
                    </tr>
                </#list>
            </tbody>
        </table>
    </#list>
</div>
