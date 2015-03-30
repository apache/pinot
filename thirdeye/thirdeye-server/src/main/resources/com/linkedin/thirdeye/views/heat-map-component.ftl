<#list dimensionsByConfig as dimensionName>
    <table id="${dimensionName}-heat-map">
        <caption>
            ${dimensionName}
            <a class="multi-time-series-link" dimension="${dimensionName}" href="#multi-time-series" data-uk-modal><img src="/assets/images/line_chart-32.png"/></a>
        </caption>
        <#list heatMaps[dimensionName] as row>
            <tr>
                <#list row as cell>
                    <td class="heat-map-cell" style="cursor: pointer; background-color: rgba(${cell.color.red}, ${cell.color.green}, ${cell.color.blue}, ${cell.alpha})">
                        <a class="heat-map-link" href="" dimension="${dimensionName}" value="${cell.dimensionValue}" title="${cell.label}">
                            ${cell.dimensionValue}
                        </a>
                        <br/>
                        <#if ((cell.ratio)?is_infinite || (cell.ratio)?is_nan) >
                          NA
                        <#else>
                          ${(cell.ratio * 100)?string["0.00"]}%
                        </#if>
                    </td>
                </#list>
            </tr>
        </#list>
    </table>
</#list>

<div id="multi-time-series" class="uk-modal">
    <div class="uk-modal-dialog uk-modal-dialog-large">
        <a id="multi-time-series-close" class="uk-modal-close uk-close"></a>
        <form class="uk-form">
            <label>
                Min
                <input class="multi-time-series-knob" id="min-multi-time-series" type="number" min="0" value="0"/>
            </label>
            <label>
                Max
                <input class="multi-time-series-knob" id="max-multi-time-series" type="number" min="0" value="5"/>
            </label>
        </form>
        <div id="multi-time-series-area"></div>
    </div>
</div>

<script>
$(document).ready(function() {
    $(".heat-map-cell").click(function(event) {
        event.preventDefault()

        var link = $(this).find(".heat-map-link")
        var dimension = link.attr('dimension')
        var value = link.attr('value')
        var url = window.location.pathname

        var search = window.location.search
        if (search.indexOf("#") > -1) {
            search = search.split("#")[0]
        }

        if (search.indexOf("?") > -1) {
            url = url + search + "&" + encodeURIComponent(dimension) + "=" + encodeURIComponent(value)
        } else {
            url = url + "?" + encodeURIComponent(dimension) + "=" + encodeURIComponent(value)
        }

        var hashRoute = {}
        if (window.location.hash) {
            var hashKeyValuePairs = window.location.hash.substring(1).split("&")
            for (var i = 0; i < hashKeyValuePairs.length; i++) {
                var pair = hashKeyValuePairs[i].split("=")
                hashRoute[decodeURIComponent(pair[0])] = decodeURIComponent(pair[1])
            }
        }

        var baselineSize = $("#input-baseline-size").val()
        var baselineUnit = $('input[name=input-baseline-unit]:checked', '#input-form').val()
        url = url + '#baselineSize=' + baselineSize + '&baselineUnit=' + baselineUnit

        if (hashRoute["selectedMetrics"]) {
            url += "&selectedMetrics=" + hashRoute["selectedMetrics"]
        }

        if ((window.location.origin + url) !== window.location.href) {
            $("body").css('cursor', 'wait')
            window.location = url
        }
    })

    $(".multi-time-series-link").click(function(event) {
        var dimensionName = $(this).attr("dimension")
        var urlTokens = window.location.pathname.split("/")

        // Remove funnel from tokens if present
        var filteredTokens = []
        for (var i = 0; i < urlTokens.length;) {
            if (urlTokens[i] === "funnel") {
                i += 2
            } else {
                filteredTokens.push(urlTokens[i])
                i++
            }
        }
        urlTokens = filteredTokens

        var url = "/timeSeries/" + $("#input-collection").val() + "/" + urlTokens.slice(4).join("/")

        if (window.location.search === "") {
            url += "?" + encodeURIComponent(dimensionName) + "=" + encodeURIComponent("!")
        } else {
            url += window.location.search + "&" + encodeURIComponent(dimensionName) + "=" + encodeURIComponent("!")
        }

        var dimensionPositions = {}
        var idx = 0
        $("#" + dimensionName + "-heat-map .heat-map-link").each(function(i, elt) {
            dimensionPositions[$(elt).attr('value')] = idx++
        })

        function dimensionComparator(a, b) {
            var aValue = a.dimensionValues[dimensionName]
            var bValue = b.dimensionValues[dimensionName]

            if (dimensionPositions[aValue] == null) {
                return 1
            } else if (dimensionPositions[bValue] == null) {
                return -1
            } else if (dimensionPositions[aValue] == null && dimensionPositions[bValue] == null) {
                return 0
            } else {
                return dimensionPositions[aValue] - dimensionPositions[bValue]
            }
        }

        $.get(url, function(data) {
            var minSeries = parseInt($("#min-multi-time-series").val())
            var maxSeries = parseInt($("#max-multi-time-series").val())
            $("#multi-time-series-area").html(data)
            plotTimeSeries("multi-time-series-area", minSeries, maxSeries, dimensionComparator)
        })

        $(".multi-time-series-knob").change(function() {
            $.get(url, function(data) {
                var minSeries = parseInt($("#min-multi-time-series").val())
                var maxSeries = parseInt($("#max-multi-time-series").val())
                $("#multi-time-series-area").html(data)
                plotTimeSeries("multi-time-series-area", minSeries, maxSeries, dimensionComparator)
            })
        })
    })

    $("#multi-time-series-close").click(function() {
        $("#multi-time-series-area").empty()
    })
})
</script>
