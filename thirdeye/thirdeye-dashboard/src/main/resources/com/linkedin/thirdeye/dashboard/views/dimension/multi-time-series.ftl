<script src="/assets/js/thirdeye.dimension.timeseries.js"></script>

<div class="collapser"><h2>(-) Dimension Time Series</h2></div>
<div id="dimension-time-series-area">
    <#list dimensionView.view.dimensions as dimension>
        <div class="collapser"><h3 class="dimension-time-series-title" dimension="${dimension}">(-) ${dimension}</h3></div>
        <div class="dimension-wrapper">
            <div class="dimension-time-series-placeholder" dimension="${dimension}"></div>
            <div class="dimension-time-series-tooltip" dimension="${dimension}"></div>
            <div class="dimension-time-series-legend time-series-legend" dimension="${dimension}"></div>
        </div>
    </#list>
</div>
