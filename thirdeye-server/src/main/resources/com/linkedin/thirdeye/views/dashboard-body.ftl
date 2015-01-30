<div class="sticky">
<!-- Header bar -->
<nav id="top-bar" class="top-bar" data-topbar role="navigation" data-options="sticky_on: large">
    <ul class="title-area">
        <li class="name">
            <h1><a href="#">ThirdEye</a></h1>
        </li>
    </ul>

    <section class="top-bar-section">
        <ul class="right">
            <li><a href="#" data-reveal-id="modal-help">Help</a></li>
        </ul>
    </section>
</nav>

<!-- Input -->
<div id="input" class="row">
    <form>
        <div class="large-1 columns">
            <label>
                Collection
                <select id="collections">
                    <#list collections as collection>
                        <option value="${collection}">${collection}</option>
                    </#list>
                </select>
            </label>
        </div>
        <div class="large-3 columns">
            <label>
                Metric
                <select id="metrics">
                </select>
            </label>
        </div>
        <div class="large-1 columns">
            <label>
                Date
                <input type="text" id="date-picker"/>
            </label>
        </div>
        <div class="large-1 columns">
            <label>
                Time (UTC)
                <input id="spinner" name="spinner" value="12:00 PM" />
            </label>
        </div>
        <div class="large-2 columns">
            <label>
                Baseline (<span id="baseline-display"></span>)
                <select id="baseline">
                    <option value="1">w/1w</option>
                    <option value="2">w/2w</option>
                    <option value="3">w/3w</option>
                    <option value="4">w/4w</option>
                </select>
            </label>
        </div>
        <div class="large-2 columns">
            <label>
                Time Window (<span id="time-window-size"></span> <span id="time-window-unit"></span>)
                <input id="time-window" name="time-window" value="1" type="number" min="1" max="24" />
            </label>
        </div>
        <div class="large-2 columns" id="input-buttons">
            <div id="advanced-options">
                <a href="#" data-reveal-id="modal-function"><img src='/assets/images/function-32.png' /></a>
                <a href="#" data-reveal-id="modal-metrics"><img src='/assets/images/line_chart-32.png' /></a>
                <a href="#" data-reveal-id="modal-heat-map"><img src='/assets/images/grid-32.png' /></a>
            </div>
            <div id="query-button">
                <a href="#" class="button" id="query">Go</a>
            </div>
        </div>
    </form>
</div>
</div>

<!-- Breadcrumbs -->
<div id="fixed-dimensions"></div>

<!-- Main content area -->
<div id="content">

    <!-- Image start screen -->
    <div id="image-placeholder">
        <img src='/assets/images/chakra.gif'/>
    </div>

    <!-- Time series -->
    <div id="time-series"></div>

    <!-- Heat maps -->
    <div id="heat-maps"></div>

    <!-- Modal multi-time-series -->
    <div id="modal-time-series" class="reveal-modal" data-reveal></div>

</div>

<!-- Modal metrics -->
<div id="modal-metrics" class="reveal-modal" data-reveal>
    <a class="close-reveal-modal">&#215;</a>
    <form>
        <fieldset>
            <legend>Metrics</legend>
            <div id="metrics-options"></div>
        </fieldset>

        <input type="checkbox" name="normalized" id="normalized" />
        <label for="normalized">Normalized</label>
    </form>
</div>

<!-- Modal function -->
<div id="modal-function" class="reveal-modal" data-reveal>
    <a class="close-reveal-modal">&#215;</a>

    <form>
        <fieldset>
            <legend>Function</legend>
<pre>
/**
 * A JavaScript function to transform metric time series.
 *
 * @param series
 *  A map of metric name to time series, e.g. {"myMetric": [[0,10],[1,20],[2,30]]}
 * @return
 *  A new map of metric name to time series
 */
function(series) {
    // TODO
}
</pre>
<br/>

            <textarea id="user-function" rows="5"></textarea>
        </fieldset>

    </form>
</div>

<!-- Modal heat map -->
<div id="modal-heat-map" class="reveal-modal" data-reveal>
    <a class="close-reveal-modal">&#215;</a>

    <form>
        <fieldset>
            <legend>Heat Map</legend>
            <input type="radio" name="heat-map-option" id="volume" value="volume" checked />
            <label for="volume">Volume</label>
            <br/>
            <input type="radio" name="heat-map-option" id="snapshot" value="snapshot" />
            <label for="snapshot">Outliers</label>
        </fieldset>
    </form>
</div>

<!-- Modal help -->
<div id="modal-help" class="reveal-modal" data-reveal>

    <h1>Help</h1>

    <p>
        This document describes how to generate a query, interpret
        heat-maps, and drill-down into dimension combinations.
    </p>

    <h2>Constructing a Query</h2>

    <p>
        First, select the collection / metric of interest, as well as a
        date / time.  Then select a baseline (e.g. week / week) with
        which the selected date / time will be compared. Finally,
        specify a time window (number of hours to include in the
        aggregate), and click Go.
    </p>

    <!-- select-inputs.png -->
    <img src='/assets/images/help/select-inputs.png' />

    <h2>Interpreting Results</h2>

    <p>
        After clicking Go, a time-series for the date range will be
        displayed, along with a collection of heat-maps for each dimension.
    </p>

    <!-- query-results.png -->
    <img src='/assets/images/help/query-results.png' />

    <p>
        These heat-maps can be considered a breakdown by dimension of the
        time-series chart. That is, each cell in a heat-map represents a
        value of the corresponding dimension, and contains information
        about that value relative to other values.
    </p>

    <p>
        The position of a cell in the heat-map is determined by its current
        metric value, and thecolor of a cell is determined by its previous
        metric value.
    </p>

    <p>
        So if there were no change from baseline to current, one would
        expect to see a gradual blue gradient from top to bottom. Changes
        in a dimension value can be easily identified if this gradient is
        broken. Lightly-colored cells towards the top of the heat-map mean
        a previously low value had an increase. Conversely, darkly-colored
        cells towards the bottom of the heat-map mean a previously high
        value had a decrease.
    </p>

    <p>
        The percentage in each cell represents the contribution of that
        dimension value to the overall change. (The sum of these
        percentages should equal the number displayed in the title of the
        graph for each heat map).
    </p>

    <p>
        One can also hover over the text in the cell to see the absolute
        values for current, baseline, and the percent change from baseline
        to current.
    </p>

    <h2>Drill-down</h2>

    <p>
        The heat-maps can be used to explore different combinations of
        dimensions by clicking on individual dimension values.
    </p>

    <p>
        The following example shows selecting "browserName" as "chrome" and
        "countryCode" as "us":
    </p>

    <!-- drill-down.png -->
    <img src='/assets/images/help/drill-down.png' />

    <p>
        As dimension values are selected, the time-series chart changes to
        reflect the selection. One can iterate through different
        combinations in this way to explore the data and identify any
        anomalies.
    </p>

    <p>
        Dimension values can be de-selected by clicking on the appropriate
        entry in the breadcrumbs area.
    </p>

    <h3>Time-series</h3>

    <p>
        In addition to the heat-maps, one can see time series for the
        dimension values (top 5) by clicking the arrow in the top-right
        corner of any heat map.
    </p>

    <!-- time-series.png -->
    <img src='/assets/images/help/time-series.png' />

    <p>
        This helps in seeing trends among different dimension values.
    </p>

    <a class="close-reveal-modal">&#215;</a>
</div>

<!-- Bigger scripts -->
<script src='/assets/javascripts/vendor/jquery.js'></script>
<script src='/assets/javascripts/vendor/globalize.js'></script>
<script src='/assets/javascripts/foundation.min.js'></script>
<script src='/assets/javascripts/ui/jquery-ui.min.js'></script>
<script src='/assets/javascripts/time.js'></script>
<script src='/assets/javascripts/index.js'></script>
<script src='/assets/javascripts/jquery.flot.js'></script>
<script>
    $(document).foundation()
</script>

<!-- Config -->
<div style="display: none" id="config">
    <div id="min-time"></div>
    <div id="max-time"></div>
    <ol id="dimensions"></ol>
</div>
