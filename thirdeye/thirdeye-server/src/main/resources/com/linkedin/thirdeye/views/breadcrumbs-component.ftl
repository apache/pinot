<#if activeDimension??>
    <ul class="uk-breadcrumb">
        <#list disabledDimensions as disabledDimension>
            <li>${disabledDimension[0]}:${disabledDimension[1]}</li>
        </#list>
        <li><a id="back-link" href="">${activeDimension[0]}:${activeDimension[1]}</a></li>
    </ul>
</#if>

<script>
$(document).ready(function() {
    $("#back-link").click(function(event) {
        event.preventDefault()

        var hashRoute = ""
        if (window.location.href.indexOf("#") > -1) {
            hashRoute = window.location.href.split("#")[1]
        }

        var tokens = window.location.search.substring(1).split("&")
        tokens = tokens.slice(0, tokens.length - 1)

        var url = null
        if (tokens.length > 0) {
            url = window.location.pathname + "?" + tokens.join("&") + "#" + hashRoute
        } else {
            url = window.location.pathname + "#" + hashRoute
        }

        if ((window.location.origin + url) !== window.location.href) {
            $("body").css('cursor', 'wait')
            window.location = url
        }
    })
})
</script>
