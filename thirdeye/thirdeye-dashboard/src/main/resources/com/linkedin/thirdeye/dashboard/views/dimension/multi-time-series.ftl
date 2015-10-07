<script src="/assets/js/thirdeye.dimension.timeseries.js"></script>

<div id="dimension-time-series-area">
    <#list dimensionView.view.dimensions as dimension>
        <div class="section-wrapper" rel="${dimension}">
            <h3 class="dimension-time-series-title" dimension="${dimension}"> ${dimension}</h3>
            <div class="dimension-time-series-placeholder" dimension="${dimension}"></div>
            <div class="dimension-time-series-tooltip" dimension="${dimension}"></div>
            <div class="dimension-time-series-legend time-series-legend" dimension="${dimension}"></div>
        </div>
    </#list>
</div>
