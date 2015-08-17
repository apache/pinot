<script src="/assets/js/thirdeye.dimension.heatmap.js"></script>

<div class="uk-button-group heat-map-buttons">

        <button id="dimension-heat-map-filter" class="uk-button dimension-heat-map-filter-btn data-uk-tooltip" title="Only show elements with 0.5% or greater volume change" state="on">
             <i id="heat-map-filter-icon" class="uk-icon-filter"></i>
        </button>

        <button class="uk-button dimension-heat-map-help-btn" data-uk-modal="{target:'#dimension-help-modal'}">
              <i class="uk-icon-question"></i>
        </button>
</div>

<div id="dimension-help-modal" class="uk-modal">
  <div class="uk-modal-dialog">
    <a class="uk-modal-close uk-close"></a>
    <h1>Heat Map</h1>

    <p>
      The heat map visualization is used to inspect a top-level metric in terms of its
      different dimension values.
    </p>

    <p>
      Each heat map represents the GROUP BY (dimension) for the current query (shown above the metric view). <br/>
      Clicking a cell of a heat map causes that dimension value to be fixed in the WHERE clause of the query, and all
      of the heat maps to be re-generated with the new query.
    </p>

    <section>The cells are:
      <ul>
        <li><b>ordered by</b> current volume (highest to lowest),</li>
        <li><b>colored by</b> the sign of the change (blue means positive, red means negative),</li>
        <li><b>shaded by</b> previous volume (darker means higher change to the previous volume)</li>
      </ul>
    </section>

    <p>
      The number in the lower <strong>left corner</strong> of each cell is the <strong>percent
      contribution to the overall change</strong>. That is, all the values in the lower left corner
      should sum up to the percent change of the current time with respect to the baseline time.
    </p>

    <p>
      The number in the lower <strong>right corner</strong> of each cell is the percent contribution
      at the baseline, as well as the contribution difference between the baseline and the current time.
      For example, a value 50% (-5%) means that the dimension value previously made up 50% of values,
      and now makes up 45% of them.
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
