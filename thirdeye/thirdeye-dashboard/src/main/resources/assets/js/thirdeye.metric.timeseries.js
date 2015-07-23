$(document).ready(function() {
    var hash = parseHashParameters(window.location.hash)
    var aggregateMillis = toMillis($("#sidenav-aggregate-size").val(), $("#sidenav-aggregate-unit").val())

    var options = {
      mode: 'own',
      click: function(event, pos, item) {
        $('body').css('cursor', 'progress')
        var path = parsePath(window.location.pathname)
        var baselineDiff = path.currentMillis - path.baselineMillis
        path.currentMillis = item.datapoint[0]
        path.baselineMillis = item.datapoint[0] - baselineDiff
        window.location.pathname = getDashboardPath(path)
      },
      legendContainer: $("#metric-time-series-legend"),
      aggregateMillis: aggregateMillis
    }

    var path = parsePath(window.location.pathname)
    if (path.metricViewType == 'TIME_SERIES_OVERLAY') {
      options.windowMillis = toMillis(1, 'WEEKS') // TODO make configurable
      options.mode = 'own' // default show baseline w/ its metric
      $("#metric-time-series-split").hide();
    }

    var container = $("#metric-time-series-placeholder")
    var tooltip = $("#metric-time-series-tooltip")

    $("#metric-time-series-placeholder").html("<p>Loading...</p>")
    renderTimeSeries(container, tooltip, options)
})
