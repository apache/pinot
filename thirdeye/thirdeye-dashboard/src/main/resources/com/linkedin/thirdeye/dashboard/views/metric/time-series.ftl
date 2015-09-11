<#-- stand-alone -->
<#if (!metricView??)>
    <#include "../common/style.ftl">
    <#include "../common/script.ftl">
</#if>

<script src="/assets/js/thirdeye.metric.timeseries.js"></script>

<#assign dimensions = (metricView.view.dimensionValues)!dimensionValues>
<#assign dimensionAliases = (metricView.view.dimensionAliases)!dimensionAliases>
<#include "../common/dimension-header.ftl">

<div class="collapser"><h2>(-) Metric Time Series View</h2></div>
<div id="metric-time-series-area">
    <div id="metric-time-series-placeholder"></div>
    <div id="metric-time-series-tooltip"></div>
    <div id="metric-time-series-legend" class="time-series-legend"></div>
</div>

<hr />
