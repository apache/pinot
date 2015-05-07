<#-- stand-alone -->
<#if (!metricView??)>
    <#include "../common/style.ftl">
    <#include "../common/script.ftl">
</#if>

<script src="/assets/js/thirdeye.metric.timeseries.js"></script>

<div id="metric-time-series-buttons" data-uk-button-radio>
    <button id="metric-time-series-split" class="metric-time-series-button-mode uk-button" type="button">Split</button>
</div>

<#assign dimensions = (metricView.view.dimensionValues)!dimensionValues>
<#include "../common/dimension-header.ftl">

<div id="metric-time-series-area">
    <div id="metric-time-series-placeholder"></div>
    <div id="metric-time-series-tooltip"></div>
    <div id="metric-time-series-legend" class="time-series-legend"></div>
</div>
