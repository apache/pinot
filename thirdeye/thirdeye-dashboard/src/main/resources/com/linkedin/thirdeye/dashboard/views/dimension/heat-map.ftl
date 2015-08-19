<script src="/assets/js/thirdeye.dimension.heatmap.js"></script>

<div class="heat-map-buttons">
    <button id="dimension-heat-map-filter" class="uk-button dimension-heat-map-filter-btn data-uk-tooltip" title="Only show elements with 0.5% or greater volume change" state="on">
         <i id="heat-map-filter-icon" class="uk-icon-filter"></i>
    </button>
</div>

<div class="uk-button-group heat-map-buttons" data-uk-button-radio>
    <button class="uk-button dimension-heat-map-mode" id="dimension-heat-map-mode-self" mode="self">Self</button>
    <button class="uk-button dimension-heat-map-mode" id="dimension-heat-map-mode-others" mode="others">Others</button>
    <button class="uk-button dimension-heat-map-mode" id="dimension-heat-map-mode-all" mode="all">All</button>
</div>

<div id="dimension-heat-map-explanation">
  <div id="dimension-heat-map-explanation-self">
    <p>
      Shows percent change with respect to self: <code>(current - baseline) / baseline</code>
    </p>
    <p>
      <em>
        This view is appropriate for analyzing dimension values in isolation
      </em>
    </p>
  </div>
  <div id="dimension-heat-map-explanation-others">
    <p>
      Shows baseline percentage of whole, and difference with respect to current ratio: <br/>
      <code>baseline / sum(baseline)</code> +/-<code>(current / sum(current) - baseline / sum(baseline))</code>
    </p>
    <p>
      <em>
        This view shows displacement among dimension values, which can be used to determine the change
        of a dimension's composition
      </em>
    </p>
  </div>
  <div id="dimension-heat-map-explanation-all">
    <p>
      Shows contribution to overall change: <code>(current - baseline) / sum(baseline)</code>
    </p>
    <p>
      <em>
        This view weights dimension values by the total, so it can be used to break down the change in a metric</br>
        (That is, the heat map cells sum to the overall change in the metric)
      </em>
    </p>
  </div>
  <p>
    Cells are ordered in descending order based on current value, and
    shaded based on baseline value (darker is greater)
  </p>
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
