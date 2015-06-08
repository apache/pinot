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

        <h3>Contribution</h3>
        <p>
          The contribution heat map view is a measure of the difference in each
          dimension value's relative contribution to the total. This is useful
          to determine whether or not there is movement among the dimension
          values (i.e. one value contributes relatively more at the current
          time with respect to the baseline time).
        </p>
        <p>
          The cells are ordered left-to-right, top-to-bottom by the signed
          magnitude of their contribution change. The cells are colored according
          to their 
          <a href="http://en.wikipedia.org/wiki/Cumulative_distribution_function">CDF value</a>,
          so darker cells represent dimension values that make up a bigger component of the total.
        </p>
        <p>
          The formula for the contribution difference in each cell is: <br/>
          <pre>newValue / sum(newValues) - oldValue / sum(oldValues)</pre>
        </p>

        <h3>Volume</h3>
        <p>
          The volume heat map view is a measure of each dimension value's raw
          contribution to the total. This is useful in quantifying exactly
          where drops or increases occurred. The sum of all values in the heat
          map equals the change in the aggregate metric.
        </p>
        <p>
          The cells are ordered left-to-right, top-to-bottom by their current
          raw volume, and colored based on the 
          <a href="http://en.wikipedia.org/wiki/Cumulative_distribution_function">CDF value</a> 
          of their baseline value. Thus lighter cells that are higher in the
          heat map represent a positive change, and darker cells that are lower
          in the heat map represent a negative change.
        </p>
        <p>
          The formula for the volume difference in each cell is: <br/>
          <pre>(newValue - oldValue) / sum(oldValues)</pre>
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
