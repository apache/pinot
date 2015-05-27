<script src="/assets/js/thirdeye.dimension.heatmap.js"></script>

<div id="dimension-heat-map-buttons">
    <div class="uk-button-group" data-uk-button-radio>
        <button id="dimension-heat-map-button-contribution" class="uk-button dimension-heat-map-button" impl="contribution" type="button">Contribution</button>
        <button id="dimension-heat-map-button-volume" class="uk-button uk-active dimension-heat-map-button" impl="volume" type="button">Volume</button>
    </div>
    <a 
        href="#heat-map-info-modal" 
        data-uk-modal
    >
        <i 
            class="uk-icon-info-circle"
        ></i>
    </a>
</div>

<div id="heat-map-info-modal" class="uk-modal">
    <div class="uk-modal-dialog">
        <div class="uk-modal-header">
            Heat Map Info
            <a class="uk-modal-close uk-close uk-align-right"></a>
        </div>
        <h3>Contribution Heat Maps</h3>
        <p>
            Contributions Heat maps are based on the Current Ratio / Previous
            Ratio. <br /> If the item in the heat map has a border it is more
            likely to be an outlier. <br /> 
        </p>
        <h3>Volume Heat Maps</h3>
        <p>
            Items are ordered based on current value and colored based on
            previous value,<br /> allowing you to see how an item is affected.
        </p>
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
                 id='dimension-view-heat-map-${heatMap.metric}-${heatMap.dimension?replace(".", "-")}'
                 metric='${heatMap.metric}'
                 dimension='${heatMap.dimension}'
                 stats-names='${heatMap.statsNamesJson}'>
                <#list heatMap.cells as cell>
                    <div class='dimension-view-heat-map-cell'
                         value='${cell.value?html}'
                         stats='${cell.statsJson}'></div>
                </#list>
            </div>
        </#list>
    </div>
</div>
