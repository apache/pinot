<#-- view removed from dashboard 09/2015 -->
<#-- stand-alone -->
<#if (!metricView??)>
    <#include "../common/style.ftl">
    <#include "../common/script.ftl">
</#if>

<script src="/assets/js/thirdeye.metric.timeseries.js"></script>

<div id="metric-time-series-area">
<#list metricView.view.metricNames as metric>
    <div class="metric-section-wrapper" rel="${metric}">
        <div id="metric-time-series-legend" class="time-series-legend" rel="${metric}"></div>
        <div id="metric-time-series-placeholder" rel="${metric}"></div>
        <div id="metric-time-series-tooltip" rel="${metric}"></div>
    </div>
</#list>
</div>

