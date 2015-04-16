$(document).ready(function() {
    var options = {
      click: function(event, pos, item) {
        var path = parsePath(window.location.pathname)
        var baselineDiff = path.currentMillis - path.baselineMillis
        path.currentMillis = item.datapoint[0]
        path.baselineMillis = item.datapoint[0] - baselineDiff
        window.location.pathname = getDashboardPath(path)
      }
    }

    var path = parsePath(window.location.pathname)
    if (path.metricViewType == 'TIME_SERIES_OVERLAY') {
      options.windowMillis = toMillis(1, 'WEEKS'), // TODO make configurable
      options.windowOffsetMillis = toMillis($("#sidenav-aggregate-size").val(), $("#sidenav-aggregate-unit").val())
    }

    $("#metric-time-series-placeholder").html("<p>Loading...</p>")
    renderTimeSeries($("#metric-time-series-placeholder"), $("#metric-time-series-tooltip"), options)
})
