$(document).ready(function() {
    var containers = {}

    $("#dimension-time-series-area").find('.dimension-time-series-placeholder').each(function(i, container) {
        var containerObj = $(container)
        var dimension = containerObj.attr('dimension')
        containers[dimension] = {
            plot: containerObj
        }
        containerObj.html("<p>Loading " + dimension + "...</p>")
    })

    $("#dimension-time-series-area").find('.dimension-time-series-tooltip').each(function(i, container) {
        var containerObj = $(container)
        var dimension = containerObj.attr('dimension')
        containers[dimension].tooltip = containerObj
    })

    $("#dimension-time-series-area").find('.dimension-time-series-title').each(function(i, container) {
        var containerObj = $(container)
        var dimension = containerObj.attr('dimension')
        containers[dimension].title = containerObj
    })

    var options = {
        legend: true,
        filter: function(data) {
            // Pick the top 4 according to baseline value
            data.sort(function(a, b) {
                var cmp = b.data[0][1] - a.data[0][1]

                if (cmp < 0) {
                    return -1
                } else if (cmp > 0) {
                    return 1
                } else {
                    return 0
                }
            })

            return data.slice(0, 4)
        },
        click: function(event, pos, item) {
            // Parse item.series.dimensions
            var seriesDimensions = JSON.parse(item.series.dimensions)

            // Parse item.series.dimensionNames
            var dimensionNames = JSON.parse(item.series.dimensionNames)

            // Parse dimensionValues from uri
            var dimensionValues = parseDimensionValues(window.location.search)

            // Set all non-star values in URI
            $.each(dimensionNames, function(i, name) {
                var value = seriesDimensions[i]
                if (value && value != "*") {
                    dimensionValues[name] = value
                }
            })

            // Change window location
            var newQuery = encodeDimensionValues(dimensionValues)
            window.location.search = newQuery
        }
    }

    var path = parsePath(window.location.pathname)
    if (path.metricViewType == 'TIME_SERIES_OVERLAY') {
      options.windowMillis = toMillis(1, 'WEEKS'), // TODO make configurable
      options.windowOffsetMillis = toMillis($("#sidenav-aggregate-size").val(), $("#sidenav-aggregate-unit").val())
    }

    function plotAllSeries() {
        $.each(containers, function(dimension, container) {
            container.title.html(dimension)
            options.dimension = dimension
            renderTimeSeries(container.plot, container.tooltip, options)
        })
    }

    $("#dimension-time-series-button-legend").click(function() {
        options.legend = !options.legend
        plotAllSeries()
    })

    $(".dimension-time-series-button-mode").click(function() {
        var mode = $(this).attr('mode')
        var hash = parseHashParameters(window.location.hash)
        hash['timeSeriesMode'] = mode
        window.location.hash = encodeHashParameters(hash)

        if (options.mode != mode) {
            options.mode = mode
            plotAllSeries()
        }
    })

    plotAllSeries()
})
