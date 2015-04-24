$(document).ready(function() {
    $("#metric-funnel-placeholder").html("<p>Loading...</p>")

    var aggregateMillis = toMillis($("#sidenav-aggregate-size").val(), $("#sidenav-aggregate-unit").val())

    renderFunnel($("#metric-funnel-current-placeholder"), {
        mode: 'current',
        aggregateMillis: aggregateMillis
    })

    renderFunnel($("#metric-funnel-baseline-placeholder"), {
        mode: 'baseline',
        aggregateMillis: aggregateMillis
    })
})
