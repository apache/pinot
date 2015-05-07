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
      },
      legendContainer: $("#metric-time-series-legend")
    }

    var path = parsePath(window.location.pathname)
    if (path.metricViewType == 'TIME_SERIES_OVERLAY') {
      options.windowMillis = toMillis(1, 'WEEKS') // TODO make configurable
      options.mode = 'own' // default show baseline w/ its metric
    }

    var container = $("#metric-time-series-placeholder")
    var tooltip = $("#metric-time-series-tooltip")

    // split button
    $("#metric-time-series-split").click(function(event) {
        var obj = $(this)

        var mode = null
        if (obj.hasClass('uk-active')) {
            mode = 'same'
            obj.removeClass('uk-active')
        } else {
            mode = 'own'
            obj.addClass('uk-active')
        }

        var hash = parseHashParameters(window.location.hash)
        hash['timeSeriesMode'] = mode
        window.location.hash = encodeHashParameters(hash)

        options.mode = mode
        renderTimeSeries(container, tooltip, options)

        return false
    })

    $("#metric-time-series-placeholder").html("<p>Loading...</p>")
    renderTimeSeries(container, tooltip, options)
})
