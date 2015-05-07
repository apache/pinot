<script src="/assets/js/thirdeye.dimension.heatmap.js"></script>

<div id="dimension-heat-map-buttons">
    <div class="uk-button-group" data-uk-button-radio>
        <button id="dimension-heat-map-button-contribution" class="uk-button dimension-heat-map-button" impl="contribution" type="button">Contribution</button>
        <button id="dimension-heat-map-button-volume" class="uk-button uk-active dimension-heat-map-button" impl="volume" type="button">Volume</button>
    </div>
</div>


<div id="dimension-heat-map-area">
    <#if (dimensionView.view.heatMaps?size == 0)>
        <div class="uk-alert uk-alert-warning">
            <p>No data available</p>
        </div>
    </#if>

    <div id="dimension-heat-map-container"></div>
    <div id="dimension-heat-map-data">
        <#list dimensionView.view.heatMaps as heatMap>
            <div class="dimension-view-heat-map"
                 id='dimension-view-heat-map-${heatMap.metric}-${heatMap.dimension}'
                 metric='${heatMap.metric}'
                 dimension='${heatMap.dimension}'
                 stats-names='${heatMap.statsNamesJson}'>
                <#list heatMap.cells as cell>
                    <div class='dimension-view-heat-map-cell'
                         value='${cell.value}'
                         stats='${cell.statsJson}'></div>
                </#list>
            </div>
        </#list>
    </div>
</div>
