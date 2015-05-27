$(document).ready(function() {
    var timeZone = jstz()
    $(".metric-table-time").each(function(i, cell) {
        var cellObj = $(cell)
        var currentTime = moment(cellObj.html())
        var baselineTime = moment(cellObj.attr('title'))
        cellObj.html(currentTime.tz(timeZone.timezone_name).format())
        cellObj.attr('title', baselineTime.tz(timeZone.timezone_name).format())

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
