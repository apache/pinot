<#-- stand-alone -->
<#if (!metricView??)>
    <#include "../common/style.ftl">
    <#include "../common/script.ftl">
</#if>

<script src="/assets/js/thirdeye.metric.table.js"></script>

<div id="metric-table-area">
    <#list (metricView.view.metricTables)!metricTables as metricTable>
        <#assign dimensions = metricTable.dimensionValues>
        <#include "../common/dimension-header.ftl">
        <table class="uk-table">
            <thead>
                <tr>
                    <th></th>
                    <#list (metricView.view.metricNames)!metricNames as metricName>
                        <th>${metricName}</th>
                        <th></th>
                        <th></th>
                    </#list>
                </tr>
                <tr>
                    <th></th>
                    <#list (metricView.view.metricNames)!metricNames as metricName>
                        <th>Current</th>
                        <th>Baseline</th>
                        <th>Ratio</th>
                    </#list>
                </tr>
            </thead>

            <tbody>
                <#list metricTable.rows as row>
                    <tr>
                        <#-- This renders time in UTC (moment.js used to convert to local) -->
                        <td class="metric-table-time" title="${row.baselineTime}" currentUTC="${row.currentTime}">${row.currentTime}</td>
                        <#list 0..(row.numColumns-1) as i>
                            <td>${row.current[i]?string!"N/A"}</td>
                            <td>${row.baseline[i]?string!"N/A"}</td>
                                <#if row.ratio[i]??>
                                    <td class="
                                        ${(row.ratio[i] < 0)?string('metric-table-down-cell', '')}
                                        ${(row.ratio[i] == 0)?string('metric-table-same-cell', '')}
                                    ">
                                    ${(row.ratio[i] * 100)?string["0.00"] + "%"}
                                    </td>
                                <#else>
                                    <td>N/A</td>
                                </#if>
                        </#list>
                    </tr>
                </#list>
            </tbody>
        </table>
    </#list>
</div>
