function getQueryParamValue(queryString) {
    var params = {}, queries, temp, i, l;
 
    // Split into key/value pairs
    queryString = queryString.substring(1, queryString.length);
    queries = queryString.split("&");
 
    // Convert the array of strings into an object
    for ( i = 0, l = queries.length; i < l; i++ ) {
        temp = queries[i].split('=');
        params[temp[0]] = temp[1];
    }
    return params;
}

/**
 * @return An object with named path components
 */
function parsePath(dashboardPath) {
    var tokens = dashboardPath.split("/")

    var type = tokens[1]

    if (type == 'dashboard') {
        return {
            collection: tokens[2],
            metricFunction: tokens[3],
            metricViewType: tokens[4],
            dimensionViewType: tokens[5],
            baselineMillis: tokens[6],
            currentMillis: tokens[7]
        }
    } else if (type == 'metric') {
        return {
            collection: tokens[2],
            metricFunction: tokens[3],
            metricViewType: tokens[4],
            baselineMillis: tokens[5],
            currentMillis: tokens[6]
        }
    } else if (type == 'dimension') {
        return {
            collection: tokens[2],
            metricFunction: tokens[3],
            dimensionViewType: tokens[4],
            baselineMillis: tokens[5],
            currentMillis: tokens[6]
        }
    } else {
        throw "Invalid path type: " + type
    }
}

function parseHashParameters(hashString) {
  var params = {}

  if (hashString) {
    if (hashString.charAt(0) == '#') {
      hashString = hashString.substring(1)
    }

    var keyValuePairs = hashString.split('&')

    $.each(keyValuePairs, function(i, pair) {
      var tokens = pair.split('=')
      var key = decodeURIComponent(tokens[0])
      var value = decodeURIComponent(tokens[1])
      params[key] = value
    })
  }

  return params
}

function encodeHashParameters(hashParameters) {
  var keyValuePairs = []

  $.each(hashParameters, function(key, value) {
    keyValuePairs.push(encodeURIComponent(key) + '=' + encodeURIComponent(value))
  })

  return '#' + keyValuePairs.join('&')
}

function setHashParameter(hashString, key, value) {
  var params = parseHashParameters(hashString)
  params[key] = value
  return encodeHashParameters(params)
}

function parseMetricFunction(metricFunction) {
    var stack = []
    var collector = ""

    for (var i = 0; i < metricFunction.length; i++) {
        if (metricFunction.charAt(i) == '(') { // open function
            var name = collector
            collector = ""
            stack.push({
                name: name,
                args: []
            })
        } else if (metricFunction.charAt(i) == ')') { // close function
            if (collector.length > 0) {
                stack[stack.length - 1].args.push(collector)
                collector = ""
            }
            var func = stack.pop()
            if (stack.length == 0) {
                stack.push(func)
            } else {
                stack[stack.length - 1].args.push(func)
            }
        } else if (metricFunction.charAt(i) == ',') { // arg
            stack[stack.length - 1].args.push(collector)
            collector = ""
        } else {
            collector += metricFunction.charAt(i)
        }
    }

    return stack.pop()
}

function getDashboardPath(path) {
    return "/dashboard"
        + "/" + path.collection
        + "/" + path.metricFunction
        + "/" + path.metricViewType
        + "/" + path.dimensionViewType
        + "/" + path.baselineMillis
        + "/" + path.currentMillis
}

function getFlotViewType(metricViewType) {
    if (metricViewType == 'INTRA_DAY') {
        return 'TIME_SERIES_FULL'
    } else if (metricViewType == 'FUNNEL') {
        return 'TIME_SERIES_FULL'
    } else {
        return metricViewType
    }
}

/**
 * @return A pathname suitable for getting the time series from the parsed path
 */
function getFlotPath(path, options) {
    var viewType = getFlotViewType(path.metricViewType)

    if (viewType == 'TIME_SERIES_OVERLAY') {
        return '/flot'
            + '/' + viewType
            + '/' + path.collection
            + '/' + path.metricFunction
            + '/' + path.baselineMillis
            + '/' + path.currentMillis
            + '/' + options.windowMillis
    } else {
        return '/flot'
            + '/' + viewType
            + '/' + path.collection
            + '/' + path.metricFunction
            + '/' + path.baselineMillis
            + '/' + path.currentMillis
    }
}

function parseDimensionValues(queryString) {
    var dimensionValues = {}

    if (queryString) {
        var query = queryString
        if (query.indexOf("?") >= 0) {
            query = query.substring(1)
        }

        var tokens = query.split("&")
        $.each(tokens, function(i, token) {
            var keyValue = token.split("=")
            var key = decodeURIComponent(keyValue[0])
            var value = decodeURIComponent(keyValue[1])
            dimensionValues[key] = value
        })
    }

    return dimensionValues
}

function encodeDimensionValues(dimensionValues) {
    var components = []

    $.each(dimensionValues, function(key, value) {
        var encodedKey = encodeURIComponent(key)
        var encodedValue = encodeURIComponent(value)
        components.push(encodedKey + "=" + encodedValue)
    })

    return "?" + components.join("&")
}

function renderFunnel(container, options) {
    var path = parsePath(window.location.pathname)

    var endMillis = 0
    if (options.mode == 'current') {
        endMillis = path.currentMillis
    } else {
        endMillis = path.baselineMillis
    }

    path.baselineMillis = endMillis
    path.currentMillis = endMillis

    var url = getFlotPath(path, options)

    if (window.location.search) {
        url += window.location.search
        if (options.dimension) {
            url += '&' + encodeURIComponent(options.dimension) + '=!'
        }
    } else if (options.dimension) {
        url += '?' + encodeURIComponent(options.dimension) + '=!'
    }

    if (options.legendContainer) {
        options.legendContainer.empty()
    }

    var render = function(data) {
        container.css('width', container.width())
        container.css('height', '400px')

        data.sort(function(a, b) {
            return b.data[0][1] - a.data[0][1]
        })

        // Max value
        var max = 0
        $.each(data, function(i, datum) {
            datum.rawData = datum.data[0][1]
            if (datum.rawData > max) {
                max = datum.rawData
            }
        })

        // Get ratios to max
        $.each(data, function(i, datum) {
            datum.ratio = datum.rawData / max
        })

        // Subtract the next from each
        for (var i = 0; i < data.length - 1; i++) {
            data[i].data[0][1] -= data[i+1].data[0][1]
        }

        if(data.length == 1) {
            var warning = $('<div></div>', { class: 'uk-alert uk-alert-warning' })
            warning.append($('<p></p>', { html: 'A funnel must consist of more than one metric.' }))
            container.append(warning)
            return;
        }
        container.plot(data, {
            series: {
                funnel: {
                    show: true,
                    label: {
                        show: true,
                        formatter: function(label, slice) {
                            if (slice.ratio >= 0.999) {
                                return slice.rawData
                            }
                            return slice.rawData + ' (' + (slice.ratio * 100).toFixed(2) + '%)'
                        }
                    }
                }
            },
            legend: {
                container: options.legendContainer
            }
        })
    }

    $.ajax({
        url: url,
        statusCode: {
            404: function() {
                container.empty()
                var warning = $('<div></div>', { class: 'uk-alert uk-alert-warning' })
                warning.append($('<p></p>', { html: 'No data available' }))
                container.append(warning)
            },
            500: function() {
                container.empty()
                var error = $('<div></div>', { class: 'uk-alert uk-alert-danger' })
                error.append($('<p></p>', { html: 'Internal server error' }))
                container.append(error)
            }
        }
    }).done(render)
}

/**
 * @param container The jQuery object in which to put the time series
 * @param tooltip The jQuery object which should contain the hover information
 */
function renderTimeSeries(container, tooltip, options) {
    container.empty()
    container.html('Loading...')

    var path = parsePath(window.location.pathname)
    var url = getFlotPath(path, options)

    if (!options) {
        options = {}
    }

    if (options.legendContainer) {
        options.legendContainer.empty()
    }

    if (window.location.search) {
        url += window.location.search
        if (options.dimension) {
            url += '&' + encodeURIComponent(options.dimension) + '=!'
        }
    } else if (options.dimension) {
        url += '?' + encodeURIComponent(options.dimension) + '=!'
    }

    container.css('width', container.width())
    tooltip.css('position', 'absolute')
    tooltip.css('display', 'none')

    options.minTickSize = (path.currentMillis - path.baselineMillis) / 10

    var render = function(data) {
        container.empty()
        if (options.mode == 'own') {
            var groups = {}
            $.each(data, function(i, datum) {
                var label = datum.label
                if (label.indexOf('BASELINE_') >= 0) {
                    label = label.substring('BASELINE_'.length)
                } else if (label.indexOf('ANOMALY_') >= 0) {
                    label = label.substring('ANOMALY_'.length)
                }
                label = label.substring(0, label.indexOf(' '))
                if (!groups[label]) {
                    groups[label] = []
                }
                groups[label].push(datum)
            })

            var groupValues = []
            $.each(groups, function(label, values) {
                groupValues.push(values)
            })

            var totalHeight = 0
            for (var i = 0; i < groupValues.length; i++) {
                var subContainer = $("<div></div>")
                var subCanvas = $("<div></div>")
                var subLegend = $("<div></div>", { class: 'time-series-legend' })
                var optionsCopy = $.extend(true, {}, options)

                subContainer.append(subCanvas).append(subLegend)
                container.append(subContainer)

                subCanvas.css('width', container.width())
                subCanvas.css('height', '200px')
                optionsCopy.legendContainer = subLegend
                plotOne(subCanvas, tooltip, optionsCopy, groupValues[i])

                totalHeight += subContainer.height()
            }

            container.css('height', totalHeight)
        } else {
            container.css('height', '400px')
            plotOne(container, tooltip, options, data)
        }
    }

    $.ajax({
        url: url,
        statusCode: {
            404: function() {
                container.empty()
                var warning = $('<div></div>', { class: 'uk-alert uk-alert-warning' })
                warning.append($('<p></p>', { html: 'No data available' }))
                container.append(warning)
            },
            500: function() {
                container.empty()
                var error = $('<div></div>', { class: 'uk-alert uk-alert-danger' })
                error.append($('<p></p>', { html: 'Internal server error' }))
                container.append(error)
            }
        }
    }).done(render)
}

function plotOne(container, tooltip, options, data) {
    if (options.filter) {
        data = options.filter(data)
    }

    // make anomalies display as points instead of lines
    var dataToPlot = []
    for (var i = 0; i < data.length; i++) {
        if (data[i].label.indexOf("ANOMALY_") >= 0) {
            data[i].lines = { show: false }
            data[i].points = {
                show: true,
                radius: 5,
            }
            data[i].color = "red"
        }
    }

    container.plot(data, {
        legend: {
            show: options.legend == null ? true : options.legend,
                position: "se",
                container: options.legendContainer
        },
        grid: {
            clickable: true,
            hoverable: true
        },
        xaxis: {
            tickFormatter: function(millis) {
                return moment.utc(millis).tz(jstz().timezone_name).format("YYYY-MM-DD HH:mm")
            },
            minTickSize: options.minTickSize
        }
    })

    container.bind("plothover", function(event, pos, item) {
        if (item) {
            var dateString = moment.utc(item.datapoint[0]).tz(jstz().timezone_name).format()
            var value = item.datapoint[1]
            if (item.series.label.indexOf("ANOMALY_") >= 0) {
                var metric =  item.series.label.substring('ANOMALY_'.length, item.series.label.indexOf(' '))
                tooltip.html(metric + " = " + value + " @ " + dateString + "<hr>"
                    + item.series.annotations[item.datapoint[0]].join("<hr>"))
                    .css({
                         top: item.pageY + 5,
                         right: $(window).width() - item.pageX,
                         'background-color': 'white',
                         border: '1px solid red',
                         'z-index': 1000
                    })
                    .fadeIn(100)
            } else {
                tooltip.html(item.series.label + " = " + value + " @ " + dateString)
                   .css({
                        top: item.pageY + 5,
                        right: $(window).width() - item.pageX,
                        'background-color': '#ffcc00',
                        border: '1px solid #cc9900',
                        'z-index': 1000
                   })
                   .fadeIn(100)
            }
        } else {
            tooltip.hide()
        }
    })

    if (options.click) {
        container.bind("plotclick", options.click)
    }
}

/**
 * @param rawData The container with raw data
 * @return an object with the raw data
 */
function extractHeatMapData(rawData) {
    var data = {}


    rawData.find('.dimension-view-heat-map').each(function(i, heatMap) {
        var heatMapObj = $(heatMap)
        var id = heatMapObj.attr('metric').split('.').join('-') + '-' + heatMapObj.attr('dimension').split('.').join('-')
        data[id] = {
          metric: heatMapObj.attr('metric'),
          metricDisplay: heatMapObj.attr('metric-display'),
          dimension: heatMapObj.attr('dimension'),
          dimensionDisplay: heatMapObj.attr('dimension-display'),
          cells: []
        }

        // Get stats name mapping
        var statsNamesMapping = {}
        var statsNames = JSON.parse(heatMapObj.attr('stats-names'))
        $.each(statsNames, function(i, statsName) {
            statsNamesMapping[statsName] = i
        })

        heatMapObj.find('.dimension-view-heat-map-cell').each(function(j, cell) {
            var cellObj = $(cell)

            // Get cell stats
            try {
                var statsList = JSON.parse(cellObj.attr('stats'))
                var cellStats = {}
                $.each(statsNamesMapping, function(name, idx) {
                    cellStats[name] = statsList[idx]
                })

                data[id].cells.push({
                    value: cellObj.attr('value'),
                    stats: cellStats
                })
            } catch (e) {
                console.error('Corrupt heat map cell data', cellObj, e)
            }
        })
    })
    return data
}

/**
 * @param rawData The raw heat map data (XML)
 * @param container The container in which to place the rendered heat map
 * @param options (sortKey, alphaKey, mainDisplayKey, positiveClass, negativeClass)
 */
function renderHeatMap(rawData, container, options) {
    var data = extractHeatMapData(rawData)

    container.empty()

    // Group
    var groups = {}
    $.each(data, function(heatMapId, heatMapSpec) {
        var cells = heatMapSpec.cells
        var tokens = heatMapId.split('-')
        var metric = heatMapSpec.metric
        var dimension = heatMapSpec.dimension
        var groupKey = options.groupBy == 'DIMENSION' ? heatMapSpec.dimensionDisplay : heatMapSpec.metricDisplay
        var caption = options.groupBy == 'DIMENSION' ? heatMapSpec.metricDisplay : heatMapSpec.dimensionDisplay // show the other as caption

        if (!groups[groupKey]) {
            groups[groupKey] = []
        }

        groups[groupKey].push({
            dimension: dimension,
            metric: metric,
            caption: caption,
            cells: cells,
            id: heatMapId
        })
    })

    $.each(groups, function(groupId, group) {
        var header = $('<h2></h2>', { html: groupId })
        container.append(header)

        $.each(group, function(i, heatMap) {
            var table = $('<table></table>', { class: 'uk-table dimension-view-heat-map-rendered' })
            var caption = $('<caption></caption>', { html: heatMap.caption })
            var cells = heatMap.cells
            var heatMapId = heatMap.id
            cells.sort(options.comparator)

            // Group cells into rows
            var numColumns = 5
            var rows = []
            var currentRow = []
            for (var i = 0; i < cells.length; i++) {
                if (options.filter != null && !options.filter(cells[i])) {
                    continue
                }
                currentRow.push(cells[i])
                if (currentRow.length == numColumns) {
                    rows.push(currentRow)
                    currentRow = []
                }
            }
            if (currentRow.length > 0) {
                rows.push(currentRow)
            }

            // Generate table body
            var tbody = $("<tbody></tbody>")
            $.each(rows, function(i, row) {
                var tr = $("<tr></tr>")
                $.each(row, function(j, cell) {
                    var td = $("<td></td>")
                    td.html(options.display(cell))
                    td.css('background-color', options.backgroundColor(cell))
                    td.hover(function() { $(this).css('cursor', 'pointer') })
                    td.attr('title', $.map(cell.stats, function(val, key) {
                        if (val !== null && (key === 'baseline_cdf_value'
                            || key === 'current_cdf_value'
                            || key === 'contribution_difference'
                            || key === 'current_ratio'
                            || key === 'baseline_ratio'
                            || key === 'volume_difference')) {
                            return key + '=' + val.toFixed(2)
                        } else {
                            return key + '=' + val
                        }
                    }).join("\n"))
                    td.attr('dimension', heatMap.dimension);

                    // Annotate outliers
                    if (cell.stats['snapshot_category'] == 1) {
                        td.css('border', '2px solid #580f8b')
                    }

                    // Drill-down click handler
                    td.click(function() {
                        var name = $(this).attr('dimension')
                        var value = cell.value
                        var dimensionValues = parseDimensionValues(window.location.search)
                        dimensionValues[name] = value
                        window.location.search = encodeDimensionValues(dimensionValues)
                    })

                    tr.append(td)
                })
                tbody.append(tr)
            })

            // Append
            table.append(caption)
            table.append(tbody)
            container.append(table)
        })
    })
}

/**
 * @param rawData The container with raw data
 * @return an object with the raw data in an array as required by google treemap charts
 */
/*function extractTreeMapData(rawData){
    var treeMapdata = {}


    rawData.find('.dimension-view-heat-map').each(function(i, heatMap) {
        var heatMapObj = $(heatMap)
        var id = heatMapObj.attr('metric').split('.').join('-') + '-' + heatMapObj.attr('dimension').split('.').join('-')
        data = []

        data.push( [heatMapObj.attr('dimension-display'), heatMapObj.attr('metric-display'), 0,0])

        //data.unshift([heatMapObj.attr('metric-display'),null,0,  0])
        //data.unshift(['value', 'parent', 'current ratio (size)', 'delta (color)'])
    })
    return treeMapdata

}*/

/** @return A {"size": x, "unit": y} object that best describes @param millis */
function describeMillis(millis) {
    var descriptors = [
        [2592000000, "MONTHS"],
        [604800000, "WEEKS"],
        [86400000, "DAYS"],
        [3600000, "HOURS"]
    ]

    for (var i = 0; i < descriptors.length; i++) {
        if (millis >= descriptors[i][0] && millis % descriptors[i][0] == 0) {
            return {
                "sizeMillis": descriptors[i][0],
                "size": millis / descriptors[i][0],
                "unit": descriptors[i][1]
            }
        }
    }

    return null
}

function toMillis(size, unit) {
    if (unit == 'SECONDS') {
        return size * 1000
    } else if (unit == 'MINUTES') {
        return size * 60 * 1000
    } else if (unit == 'HOURS') {
        return size * 60 * 60 * 1000
    } else if (unit == 'DAYS') {
        return size * 24 * 60 * 60 * 1000
    } else if (unit == 'WEEKS') {
        return size * 7 * 24 * 60 * 60 * 1000
    } else if (unit == 'MONTHS') {
        return size * 30 * 24 * 60 * 60 * 1000
    }
}

function getLocalTimeZone() {
    var timeZone = jstz()
    var utcOffset = timeZone.utc_offset
    var utcOffsetHours = Math.abs(utcOffset) / 60
    var utcOffsetMinutes = Math.abs(utcOffset) % 60
    var utcOffsetMagnitude = Math.abs(utcOffsetHours)

    var formatted = ""
    formatted += utcOffset < 0 ? "-" : ""
    formatted += utcOffsetMagnitude < 10 ? "0" + utcOffsetMagnitude : utcOffsetMagnitude
    formatted += ":"
    formatted += utcOffsetMinutes < 10 ? "0" + utcOffsetMinutes : utcOffsetMinutes
    formatted += " " + timeZone.timezone_name

    return formatted
}

/**
 * Get Time Zone
 * @function
 * @public
 * @returns {String} Local timezone from getLocalTimeZone() or hash params
 * timezone
 */
function getTimeZone() {
    var timeZone = jstz()
    if(window.location.hash) {
        var params = parseHashParameters(window.location.hash)
        if(params.timezone) {
            tz = params.timezone.split('-').join('/')
        } else {
            tz = timeZone.timezone_name
        }
    } else {
        tz = timeZone.timezone_name
    }
    return tz
}

function updateTableOrder(tableContainer, orderContainer) {
  // Get column headers
  var currentOrder = $.map(tableContainer.find("thead tr").first().find("th"), function(elt) { 
    return $.trim(elt.innerHTML) 
  }).slice(1) // for time column

  // Get columns
  var columns = {}
  for (var i = 0; i < currentOrder.length; i++) {
    columns[currentOrder[i]] = []
  }
  var tableRows = tableContainer.find("tbody tr")
  for (var i = 0; i < tableRows.size(); i++) {
    var tableCols = $(tableRows[i]).find("td")
    for (var j = 0; j < currentOrder.length; j++) {
      var colIdx = 1 + j * 3 // n.b. first column time
      //columns[j].push({
      columns[currentOrder[j]].push({
        current: tableCols[colIdx].innerHTML,
        baseline: tableCols[colIdx + 1].innerHTML,
        ratio: tableCols[colIdx + 2].innerHTML
      })
    }
  }

  // Get the desired ordering of metric columns
  var nextOrder = $.map(orderContainer.find("li div"), function(elt) { 
    return $.trim(elt.innerHTML)
  })

  // Re-write the column headers
  tableContainer.find("thead tr").first().find("th").each(function(i, elt) {
    if (i > 0) { // first is time column
      var metricIndex = i - 1
      var currentMetric = currentOrder[metricIndex]
      var nextMetric = nextOrder[metricIndex]
      $(elt).html(nextMetric)
    }
  })

  // Re-write the columns
  for (var i = 0; i < tableRows.size(); i++) {
    var tableCols = $(tableRows[i]).find("td")
    for (var j = 0; j < currentOrder.length; j++) {
      var nextMetric = nextOrder[j]
      var column = columns[nextMetric]

      // This is relative to the table itself
      var colIdx = 1 + j * 3;
      $(tableCols[colIdx]).html(column[i].current)
      $(tableCols[colIdx + 1]).html(column[i].baseline)
      $(tableCols[colIdx + 2]).html(column[i].ratio)

      // Set the right class for the value
      var ratio = parseFloat(column[i].ratio)
      var ratioCell = $(tableCols[colIdx + 2])
      ratioCell.removeClass('metric-table-down-cell metric-table-same-cell')
      if (ratio < 0) {
        ratioCell.addClass('metric-table-down-cell')
      } else if (ratio == 0) {
        ratioCell.addClass('metric-table-same-cell')
      }
    }
  }

  // Set hash parameter
  var hashParams = parseHashParameters(window.location.hash)
  $.each(nextOrder, function(i, elt) {
    hashParams['intraDayOrder_' + i] = elt
  })
  window.location.hash = encodeHashParameters(hashParams)
}
