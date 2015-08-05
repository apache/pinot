$(document).ready(function() {
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
