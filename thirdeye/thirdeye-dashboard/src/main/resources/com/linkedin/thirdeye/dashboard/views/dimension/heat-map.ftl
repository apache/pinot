<script src="/assets/js/thirdeye.dimension.heatmap.js"></script>

<div id="dimension-heat-map-help">
    <a href="#dimension-help-modal" data-uk-modal><i class="uk-icon-button uk-icon-question"></i></a>
</div>

<div id="dimension-help-modal" class="uk-modal">
    <div class="uk-modal-dialog">
        <a class="uk-modal-close uk-close"></a>
        <h1>Heat Map</h1>
        <p>
            The heat map visualization is used to inspect a top-level metric in terms of its
            different dimension values. Each heat map represents the GROUP BY (dimension) for
            the current query (shown above the metric view).
        </p>

        <p>
            Clicking a cell of a heat map
            causes that dimension value to be fixed in the WHERE clause of the query, and all
            of the heat maps to be re-generated with the new query.
        </p>

        <p>
            The cells are ordered by current volume (highest to lowest), shaded by previous
            volume (darker means higher), and colored by the sign of the change (blue means positive,
            red means negative).
        <p>

        <p>
            The number in the left of each cell is the percent change with respect to the baseline:
            <code>
            (newValue - oldValue) / sum(oldValues)
            </code>
        <p>

        <p>

        <p>
            The number in the right of each cell is the contribution difference; that is, the change in
            the contribution of one dimension value to the whole:
            <code>
            (newValue) / sum(newValues) - oldValue / sum(oldValues)
            </code>
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
                 metric-display='${heatMap.metricAlias!heatMap.metric}'
                 dimension='${heatMap.dimension}'
                 dimension-display='${heatMap.dimensionAlias!heatMap.dimension}'
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
