$(document).ready(function() {
    $("#view-links a").each(function(i, link) {
        var linkObj = $(link)
        var linkType = linkObj.attr('type')
        linkObj.click(function() {
            var dashboardPath = parsePath(window.location.pathname)
            if (linkType === 'METRIC') {
                dashboardPath.metricViewType = linkObj.attr('view')
            } else if (linkType === 'DIMENSION') {
                dashboardPath.dimensionViewType = linkObj.attr('view')
            } else {
                throw 'Invalid link type ' + linkType
            }
            window.location.pathname = getDashboardPath(dashboardPath)
        })
    })

    $(".dimension-link").each(function(i, link) {
        var linkObj = $(link)
        var dimension = linkObj.attr('dimension')
        linkObj.click(function() {
            var dimensionValues = parseDimensionValues(window.location.search)
            delete dimensionValues[dimension]
            var updatedQuery = encodeDimensionValues(dimensionValues)
            window.location.search = updatedQuery
        })
    })

    $("#time-nav-left").click(function() {
        var path = parsePath(window.location.pathname)
        var baselineMillis = parseInt(path.baselineMillis)
        var currentMillis = parseInt(path.currentMillis)
        var period = currentMillis - baselineMillis
        path.baselineMillis = baselineMillis - (period / 2)
        path.currentMillis = currentMillis - (period / 2)
        window.location.pathname = getDashboardPath(path)
    })

    $("#time-nav-right").click(function() {
        var path = parsePath(window.location.pathname)
        var baselineMillis = parseInt(path.baselineMillis)
        var currentMillis = parseInt(path.currentMillis)
        var period = currentMillis - baselineMillis
        path.baselineMillis = baselineMillis + (period / 2)
        path.currentMillis = currentMillis + (period / 2)
        window.location.pathname = getDashboardPath(path)
    })
})
