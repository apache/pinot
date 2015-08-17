$(document).ready(function() {
    $("#intra-day-update").click(function() {
      updateTableOrder($("#intra-day-table"), $("#intra-day-table-order"))
    })

    // If order is already defined, set UI component and update
    var hashParams = parseHashParameters(window.location.hash)
    var existingOrder = []
    $.each(hashParams, function(key, val) {
      if (key.indexOf('intraDayOrder_') == 0) {
        var idx = parseInt(key.split('_')[1])
        existingOrder[idx] = val
      }
    })

    if (existingOrder.length > 0) {
      $("#intra-day-table-order li div").each(function(i, elt) {
        $(elt).html(existingOrder[i])
      })
      updateTableOrder($("#intra-day-table"), $("#intra-day-table-order"))
    }

    $(".metric-table-time").each(function(i, cell) {
        var tz = getTimeZone();
        var cellObj = $(cell)
        var currentTime = moment(cellObj.html())
        var baselineTime = moment(cellObj.attr('title'))
        cellObj.html(currentTime.tz(tz).format('YYYY-MM-DD HH:mm:ss z'))
        cellObj.attr('title', baselineTime.tz(tz).format('YYYY-MM-DD HH:mm:ss z'))

        // Click changes current value to that time
        cellObj.click(function() {
          var currentUTC = $(this).attr('currentUTC')
          var dateTime = moment.utc(currentUTC)
          var path = parsePath(window.location.pathname)
          var baselineDiff = path.currentMillis - path.baselineMillis
          var currentMillis = moment.utc(currentUTC)

          path.currentMillis = currentMillis
          path.baselineMillis = currentMillis - baselineDiff
          window.location.pathname = getDashboardPath(path)
        })
    })
})
