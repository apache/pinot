<ul id="funnel-nav" class="uk-subnav uk-subnav-pill">
    <li>
        <a class="funnel-type-link" value="top" href="">
            Top
        </a>
    </li>
    <li>
        <a class="funnel-type-link" value="previous" href="">
            Previous
        </a>
    </li>
</ul>

<script>
$(document).ready(function() {
    var pathTokens = window.location.pathname.split("/")

    var funnelType = null
    for (var i = 0; i < pathTokens.length; i++) {
        if (pathTokens[i] === "funnel") {
            var funnelTokens = pathTokens[i+1].split(":")
            funnelType = funnelTokens[0]
            break
        }
    }

    $(".funnel-type-link").each(function(i, link) {
        var value = $(link).attr("value")
        if (funnelType === value) {
            $(link).parent().addClass("uk-active")
        }
    })

    $(".funnel-type-link").click(function(event) {
        event.preventDefault()
        for (var i = 0; i < pathTokens.length; i++) {
            if (pathTokens[i] === "funnel") {
                var funnelTokens = pathTokens[i+1].split(":")
                var newFunnelType = $(this).attr("value")
                pathTokens[i+1] = newFunnelType + ":" + funnelTokens[1]
                window.location.href = window.location.origin + pathTokens.join("/") + window.location.search + window.location.hash
                break
            }
        }
    })
})
</script>

<style>
#funnel-nav {
    margin-top: 20px;
}
</style>
