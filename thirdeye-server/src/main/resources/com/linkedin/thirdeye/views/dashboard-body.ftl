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
            <li><a href="#" data-reveal-id="modal-metrics"><img class="advanced-option" src="/assets/images/line_chart-32-inverted.png"/></a></li>
            <li><a href="#" data-reveal-id="modal-function"><img class="advanced-option" src="/assets/images/function-32-inverted.png"/></a></li>
            <li><a href="#" data-reveal-id="modal-heat-map"><img class="advanced-option" src="/assets/images/grid-32-inverted.png"/></a></li>
        </ul>
    </section>
</nav>

<!-- Input -->
<div id="input" class="row">
    <form id="input-form">
        <label id="input-metric">
            Metric
            <select id="metrics">
            </select>
        </label>
        <label id="input-date">
            Date
            <input type="text" id="date-picker"/>
        </label>
        <label id="input-time">
            Time (UTC)
            <input id="spinner" name="spinner" value="12:00 PM" />
        </label>
        <label id="input-baseline">
            Baseline (<span id="baseline-display"></span>)
            <select id="baseline">
                <option value="1">w/1w</option>
                <option value="2">w/2w</option>
                <option value="3">w/3w</option>
                <option value="4">w/4w</option>
            </select>
        </label>
        <input type="hidden" id="collections" value="${collection}"/>
        <a href="#" class="button" id="query">Go</a>
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

        <label>
            Time Window (<span id="time-window-size"></span> <span id="time-window-unit"></span>)
            <input id="time-window" name="time-window" value="1" type="number" min="1" max="24" />
        </label>
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
