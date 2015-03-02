<ul class="uk-subnav uk-subnav-pill">
    <li>
        <a class="heat-map-type-link" value="volume" href="">
            Volume
        </a>
    </li>
    <li>
        <a class="heat-map-type-link" value="contributionDifference" href="">
            Contribution Difference
        </a>
    </li>
    <li>
        <a class="heat-map-type-link" value="selfRatio" href="">
            Self Ratio
        </a>
    </li>
    <li>
        <a class="heat-map-type-link" value="snapshot" href="">
            Snapshot (beta)
        </a>
    </li>
</ul>

<script>
$(document).ready(function() {

    var heatMapType = window.location.pathname.split("/")[3]

    $(".heat-map-type-link").each(function(i, link) {
        var value = $(link).attr("value")
        if (heatMapType === value) {
            $(link).parent().addClass("uk-active")
        }
    })

    $(".heat-map-type-link").click(function(event) {
        event.preventDefault()
        var pathTokens = window.location.pathname.split("/")
        pathTokens[3] = $(this).attr("value")
        var newPathName = pathTokens.join("/")
        if (window.location.pathname !== newPathName) {
            $("body").css('cursor', 'wait')
            window.location.pathname = newPathName
        }
    })
})
</script>