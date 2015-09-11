$(document).ready(function() {
    $(".view-links a").each(function(i, link) {
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
        window.location.href = window.location.href.replace(window.location.pathname, getDashboardPath(path))
    })

    $("#time-nav-right").click(function() {
        var path = parsePath(window.location.pathname)
        var baselineMillis = parseInt(path.baselineMillis)
        var currentMillis = parseInt(path.currentMillis)
        var period = currentMillis - baselineMillis
        path.baselineMillis = baselineMillis + (period / 2)
        path.currentMillis = currentMillis + (period / 2)
        window.location.href = window.location.href.replace(window.location.pathname, getDashboardPath(path))
    })

    $(".collapser").click(function() {
        var $header = $(this);

        //getting the next element
        var $content = $header.next();

        //handle h2, h3, h3 headers
        if($("h2", $header).length > 0) {
            var parseTitle = $("h2", $header).html().split(" ")
            var title = parseTitle.splice(1, parseTitle.length).join(" ")
            var element = "h2"
        }else if($("h3", $header).length > 0){
            var parseTitle = $("h3", $header).html().split(" ")
            var title = parseTitle.splice(1, parseTitle.length).join(" ")
            var element = "h3"
        }else{
            var parseTitle = $("h4", $header).html().split(" ")
            var title = parseTitle.splice(1, parseTitle.length).join(" ")
            var element = "h4"
        }

        //open up the content needed - toggle the slide- if visible, slide up, if not slidedown.
        $content.slideToggle(800, function () {
            $header.html(function () {
                //change text based on condition
                return $content.is(":visible") ? '<' + element + ' style="color:#069;cursor:pointer">(-) ' + title + '</' + element + '>' : '<' + element + ' style="color:#069;cursor:pointer">(+) ' + title + '</' + element + '>';
            });
        });

    });

})
