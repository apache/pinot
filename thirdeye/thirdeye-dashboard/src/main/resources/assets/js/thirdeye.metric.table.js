$(document).ready(function() {
    var timeZone = jstz()
    $(".metric-table-time").each(function(i, cell) {
        var tz = null;
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
        var cellObj = $(cell)
        var currentTime = moment(cellObj.html())
        var baselineTime = moment(cellObj.attr('title'))
        cellObj.html(currentTime.tz(tz).format('YYYY-DD-MM HH:mm:ss'))
        cellObj.attr('title', baselineTime.tz(tz).format('YYYY-DD-MM HH:mm:ss'))

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
