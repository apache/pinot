<#if (funnelViews?size > 0)>
<div class="collapser"><h2>(-) Funnel(s)</h2></div>
<div id="custom-funnels-container">
    <#list funnelViews as funnel>
        <div class="dimension-heat-map-container-title uk-clearfix">

                <h3 class="metric-list uk-float-left">
                ${funnel.name} (current = ${funnel.current} & baseline = ${funnel.baseline})
                </h3>

            <form id="funnel-time-input-form" class="uk-form uk-form-stacked uk-float-right">
                <div id="funnel-error" class="uk-alert uk-alert-danger uk-hidden">
                    <p></p>
                </div>

                <div class="uk-margin-small-top uk-margin-bottom">
                    <div class="uk-display-inline-block">
                        <label class="uk-form-label">
                            Current Date
                        </label>
                        <div class="uk-form-icon">
                            <i class="uk-icon-calendar"></i>
                            <input id="funnel-date" type="text" data-uk-datepicker="{format:'YYYY-MM-DD'}">
                        </div>
                    </div>
                    <div class="uk-display-inline-block">
                        <label class="uk-form-label">
                            Baseline
                        </label>
                        <div  class="uk-button-group" data-uk-button-radio>
                            <button class="funnel-baseline-unit uk-button"  value="86400000" >day(s)</button>
                            <button class="funnel-baseline-unit uk-button "  value="604800000" >week(s)</button>
                        </div>
                    </div>

                    <div class="uk-display-inline-block">
                        <div class="uk-button-group" data-uk-button-radio>
                            <button class="funnel-moving-average-size uk-button" unit="WoW" value="604800000" selected="selected">WoW</button>
                            <button class="funnel-moving-average-size uk-button" unit="Wo2W" value="1209600000" >Wo2W</button>
                            <button class="funnel-moving-average-size uk-button" unit="Wo4W" value="2419200000">Wo4W</button>
                        </div>
                    </div>

                    <div class="uk-display-inline-block">
                        <div class="funnel-cumulative" data-uk-button-checkbox><button class="uk-button"  selected="selected">cummulative</button>
                        </div>
                    </div>
                    <div class="uk-display-inline-block uk-margin-right">
                        <button id="funnel-submit" class="uk-button uk-button-small uk-button-primary ">Go</button>
                    </div>
                </div>
            </form>
        </div>

        <table class="uk-table dimension-view-heat-map-rendered">
            <tr>
                <td class="metric-label">Hour</td>
                <#list funnel.aliasToActualMap?keys as key> 
                    <td class="metric-label">${key}</td>
                </#list> 
            </tr>
            <#list funnel.table as row>
                <tr>
                    <td>${row.first}</td>
                    <#list row.second as column>
                        <#if (column??)>
                        <td
                            class="heat-map-cell custom-tooltip"
                            tooltip="${column}"
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
<div id="funnel-thumbnails" data-uk-grid>

    <#list funnelViews as funnel>
        <div class="thumbnail" style="display: inline-block; margin: 5px;">
            <p class="metric-list" style="font-size: 9px; font-weight:800;">
            ${funnel.name} <br> (current = ${funnel.current} & baseline = ${funnel.baseline})
            </p>

            <table class="uk-table dimension-view-heat-map-rendered" style="width: 20%; height: 200px; font-size:8px; padding:0px; border: 1px solid #ccc">
                <tr>
                    <td class="metric-label">Hour</td>
                    <#list funnel.aliasToActualMap?keys as key>
                        <td class="metric-label">${key}</td>
                    </#list>
                </tr>
                <#list funnel.table as row>
                    <tr>
                        <td>${row.first}</td>
                        <#list row.second as column>
                            <#if (column??)>
                                <td
                                        class="heat-map-cell custom-tooltip"
                                        tooltip="${column}"
                                        value="${column}"
                                        >${(column * 100)?string["0.0"]}%</td>
                            <#else>
                                <td class="not-available">N/A</td>
                            </#if>
                        </#list>
                    </tr>
                </#list>
            </table>
        </div>
    </#list>

</div>
<script>
    $(document).ready(function() {

        var path = parsePath(window.location.pathname)

        var queryParams = getQueryParamValue(window.location.search);

        $("#funnel-submit").click(function(event) {

            event.preventDefault()

            // Clear any existing alert
            var errorAlert = $("#funnel-error")
            var errorMessage = $("#funnel-error p")
            errorMessage.empty()

            //Todo: go through the possibleerrors
            /*if () {
                errorMessage.html("...")
                errorAlert.fadeIn(100)
                return
            }*/

            // Date
            var date = $("#funnel-date").val()
            if (!date) {
                errorMessage.html("Must provide date")
                errorAlert.fadeIn(100)
                return
            }

            // Baseline
            var baselineSize = 1 //parseInt($("#sidenav-baseline-size:selected").val())
            var baselineUnit = parseInt($(".funnel-baseline-unit[selected='selected']").val())

            // Date
            var current = moment.tz(date, timezone)
            var baseline = moment(current.valueOf() - (baselineSize * baselineUnit))
            var currentMillisUTC = current.utc().valueOf()
            var baselineMillisUTC = baseline.utc().valueOf()

            // Moving average
            /*if () {
                var movingAverageSize = $(".funnel-moving-average-size:selected").val()
                var movingAverageUnit = "604800000" *
                metricFunction = "MOVING_AVERAGE_" + movingAverageSize + "_" + movingAverageUnit + "(" + metricFunction + ")"
            }*/

             // Timezone
            var timezone = getTimeZone()

            //Query Parameters
            if(queryParams.hasOwnProperty("")){
                delete queryParams[""]
            }

            // Path
            var path = parsePath(window.location.pathname)
            path.metricViewType = path.metricViewType == null ? "INTRA_DAY" : path.metricViewType
            path.dimensionViewType = path.dimensionViewType == null ? "HEAT_MAP" : path.dimensionViewType
            path.baselineMillis = baselineMillisUTC
            path.currentMillis = currentMillisUTC

            var dashboardPath = getDashboardPath(path)

            var params = parseHashParameters(window.location.hash)
            if(timezone !== getLocalTimeZone().split(' ')[1]) {
                params.timezone = timezone.split('/').join('-')
            }

            errorAlert.hide()
            console.log("URI",dashboardPath + encodeDimensionValues(queryParams) + encodeHashParameters(params))

            window.location = dashboardPath + encodeDimensionValues(queryParams) + encodeHashParameters(params)
        });

        $(".heat-map-cell").each(function(i, cell) {
            var cellObj = $(cell)
            var value = parseFloat(cellObj.attr('value'))
            var absValue = Math.abs(value)

            if (value < 0) {
                cellObj.css('background-color', 'rgba(255,0,0,' + absValue + ')') // red
            } else {
                cellObj.css('background-color', 'rgba(0,0,255,' + absValue + ')') // blue
            }
        });

    });
</script>
</#if>    
