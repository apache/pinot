<script src="/assets/js/thirdeye.dimension.timeseries.js"></script>

<div id="dimension-time-series-buttons">
    <button id="dimension-time-series-button-legend" class="uk-button" type="button">Legend</button>
</div>

<div id="dimension-time-series-area">
    <#list dimensionView.view.dimensions as dimension>
        <h3 class="dimension-time-series-title" dimension="${dimension}"></h3>
        <div class="dimension-time-series-placeholder" dimension="${dimension}"></div>
        <div class="dimension-time-series-tooltip" dimension="${dimension}"></div>
    </#list>
</div>
