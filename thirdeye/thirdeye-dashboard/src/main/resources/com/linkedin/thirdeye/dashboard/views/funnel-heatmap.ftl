
<#if (funnelViews?size > 0)>
<div id="collapser"><h2 style="color:#069;cursor:pointer">(-) Funnel(s)</h2></div>
<div id="custom-funnels-container">
    <#list funnelViews as funnel>
        <h3 class="metric-list">
            ${funnel.name}
        </h3>

        <div class="metric-list">
            <#list funnel.metricIndex?keys as key> 
                <div>${funnel.metricIndex[key]} = ${key}</div>
            </#list> 
        </div>
        <table class="uk-table dimension-view-heat-map-rendered">
            <tr>
                <td class="metric-label">Hour</td>
                <#list funnel.metricIndex?keys as key> 
                    <td class="metric-label">${funnel.metricIndex[key]}</td>
                </#list> 
            </tr>
            <#list funnel.table as row>
                <tr>
                    <td>${row.first}</td>
                    <#list row.second as column>
                        <#if (column??)>
                        <td
                            class="heat-map-cell"
                            value="${column}"
                        >${(column * 100)?string["0.0"]}%</td>
                        <#else>
                            <td class="not-available">N/A</td>
                        </#if>
                    </#list>
                </tr>
            </#list>
        </table>
    </#list> 
</div>
    <script>
    $(document).ready(function() {
        $(".heat-map-cell").each(function(i, cell) {
            var cellObj = $(cell)
            var value = parseFloat(cellObj.attr('value'))
            var absValue = Math.abs(value)

            if (value < 0) {
                cellObj.css('background-color', 'rgba(255,51,51,' + absValue + ')') // red
            } else {
                cellObj.css('background-color', 'rgba(97,114,242,' + absValue + ')') // blue
            }
        });
        $("#collapser").click(function () {
            $header = $(this);
            //getting the next element
            $content = $header.next();
            //open up the content needed - toggle the slide- if visible, slide up, if not slidedown.
            $content.slideToggle(800, function () {
               $header.html(function () {
                    //change text based on condition
                    return $content.is(":visible") ? '<h2 style="color:#069;cursor:pointer">(-) Funnel(s)</h2>' : '<h2 style="color:#069;cursor:pointer">(+) Funnel(s)</h2>';
                });
            });

        });
    });
    </script>
</#if>    