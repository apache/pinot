<#-- stand-alone -->
<#if (!metricView??)>
    <#include "../common/style.ftl">
    <#include "../common/script.ftl">
</#if>

<script src="/assets/js/flot/jquery.flot.funnel.js"></script>
<script src="/assets/js/thirdeye.metric.funnel.js"></script>

<#assign dimensions = (metricView.view.dimensionValues)!dimensionValues>
<#include "../common/dimension-header.ftl">

<div id="metric-funnel-area">
    <div id="metric-funnel-baseline-placeholder"></div>
    <div id="metric-funnel-current-placeholder"></div>
</div>
