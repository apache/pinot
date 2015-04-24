$(document).ready(function() {
    var hash = parseHashParameters(window.location.hash)

    var options = {
      mode: hash['timeSeriesMode'] ? hash['timeSeriesMode'] : 'same',
      click: function(event, pos, item) {
        $('body').css('cursor', 'progress')
        var path = parsePath(window.location.pathname)
        var baselineDiff = path.currentMillis - path.baselineMillis
        var aggregateMillis = toMillis($("#sidenav-aggregate-size").val(), $("#sidenav-aggregate-unit").val())
        path.currentMillis = item.datapoint[0] + aggregateMillis
        path.baselineMillis = item.datapoint[0] + aggregateMillis - baselineDiff
        window.location.pathname = getDashboardPath(path)
      }
    }

    var path = parsePath(window.location.pathname)
    if (path.metricViewType == 'TIME_SERIES_OVERLAY') {
      options.windowMillis = toMillis(1, 'WEEKS') // TODO make configurable
    }

    var container = $("#metric-time-series-placeholder")
    var tooltip = $("#metric-time-series-tooltip")

    $(".metric-time-series-button-mode").click(function() {
        var mode = $(this).attr('mode')
        var hash = parseHashParameters(window.location.hash)
        hash['timeSeriesMode'] = mode
        window.location.hash = encodeHashParameters(hash)

        if (options.mode != mode) {
            options.mode = mode
            renderTimeSeries(container, tooltip, options)
        }
    })

    $("#metric-time-series-button-legend").click(function() {
        options.legend = !options.legend
        renderTimeSeries(container, tooltip, options)
    })

    $("#metric-time-series-placeholder").html("<p>Loading...</p>")
    renderTimeSeries(container, tooltip, options)
})
