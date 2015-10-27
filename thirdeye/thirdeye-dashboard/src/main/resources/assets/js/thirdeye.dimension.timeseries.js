$(document).ready(function() {
    var containers = {}
    var aggregateMillis = toMillis($("#sidenav-aggregate-size").val(), $("#sidenav-aggregate-unit").val())

    $("#dimension-time-series-area").find('.dimension-time-series-placeholder').each(function(i, container) {

        var containerObj = $(container)
        var dimension = containerObj.attr('dimension')
        containers[dimension] = {
            plot: containerObj
        }
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

    $("#dimension-time-series-area").find('.dimension-time-series-legend').each(function(i, container) {
        var containerObj = $(container)
        var dimension = containerObj.attr('dimension')
        containers[dimension].legend = containerObj
    })

    var hash = parseHashParameters(window.location.hash)

    var options = {
        mode: hash['dimensionTimeSeriesMode'] ? hash['dimensionTimeSeriesMode'] : 'same',
        legend: true,
        filter: function(data) {
            console.log("Data:",data)
            // Don't show anomaly series in this view
            var i = data.length
            while (i--) {
              if (data[i].label.indexOf("ANOMALY_") >= 0) {
                 // console.log("data[i].label.indexOf(ANOMALY_) >= 0", data[i])
                data.splice(i, 1)
              }
            }

            // Pick the top 4 according to baseline value
            data.sort(function(a, b) {
                //console.log("a:", a)
                //console.log("b:", b)
                if (!b.data[0] && !a.data[0]) {
                    return 0
                } else if (b.data[0] && !a.data[0]) {
                    return 1
                } else if (!b.data[0] && a.data[0]) {
                    return -1
                }
                return b.data[0][1] - a.data[0][1]
            })
            //console.log("data.slice(0, 4)", data.slice(0, 4))
            return data.slice(0, 4)
        },
        click: function(event, pos, item) {
            if(item){
                // Parse item.series.dimensions
                var seriesDimensions = JSON.parse(item.series.dimensions)

                //console.log("item.series", item.series)
                var keyValueList = item.series.label.split("(")[1].split(")")[0].split(",")

                // Parse item.series.dimensionNames
                //var dimensionNames = JSON.parse(item.series.dimensionNames)
                //console.log("dimensionNames", dimensionNames)

                // Parse dimensionValues from uri
                var dimensionValues = parseDimensionValuesAry(window.location.search)

                //add ech dimension and value pair to the dimensionValues array
                for(var i = 0, len = keyValueList.length; i <len; i++){
                    var keyValue = keyValueList[i].split(":")
                    var key = keyValue[0]
                    var value = keyValue[1].replace(/\"/g,'')
                    var URIKeyValue = key + "=" + value
                    dimensionValues.push(URIKeyValue)

                }
                var obj = {}
                //remove duplicates from dimensionValues array
                for(var z = 0, dlen = dimensionValues.length; z<dlen; z++){
                    obj[dimensionValues[z]] = 1;
                }

                var aryNoDuplicates = []
                for(key in obj){
                    aryNoDuplicates.push(key)
                }
                dimensionValues = aryNoDuplicates
                // Set all non-star values in URI
                /*$.each(dimensionNames, function(i, name) {
                    var value = seriesDimensions[i]
                    if (value && value != "*") {

                        dimensionValues.push(name + '=' + value)
                    }
                    console.log("timeseries dimensionValues", dimensionValues)
                })*/
                // Change window location
                var newQuery = encodeDimensionValuesAry(dimensionValues)
                window.location.search = newQuery
            }
        },
        aggregateMillis: aggregateMillis
    }

    var path = parsePath(window.location.pathname)
    if (path.metricViewType == 'TIME_SERIES_OVERLAY') {
      options.windowMillis = toMillis(1, 'WEEKS'), // TODO make configurable
      options.windowOffsetMillis = toMillis($("#sidenav-aggregate-size").val(), $("#sidenav-aggregate-unit").val())
    }

    function plotAllSeries() {
        $.each(containers, function(dimension, container) {
            var optionsCopy = $.extend(true, {}, options)
            optionsCopy.dimension = dimension
            optionsCopy.legendContainer = container.legend
            renderTimeSeries(container.plot, container.tooltip, optionsCopy)
        })
    }

    plotAllSeries()
})