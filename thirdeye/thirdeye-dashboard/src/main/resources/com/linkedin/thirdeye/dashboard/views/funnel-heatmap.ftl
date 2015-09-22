<#if (funnelViews?size > 0)>
<div class="collapser"><h2>(-) Funnel(s)</h2></div>
<div id="custom-funnels-container">

        <div id="custom-funnel-title" class="dimension-heat-map-container-title uk-clearfix">

            <h3 class="metric-list uk-float-left"></h3>

            <form id="funnel-time-input-form" class="uk-form uk-form-stacked uk-float-right">
                <div id="funnel-error" class="uk-alert uk-alert-danger hidden">
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
                            <button type="button" class="funnel-baseline-unit uk-button"  value="86400000" >day(s)</button>
                            <button type="button" class="funnel-baseline-unit uk-button "  value="604800000" >week(s)</button>
                        </div>
                    </div>

                    <div class="uk-display-inline-block">
                        <div class="uk-button-group" data-uk-button-radio>
                            <button type="button" class="funnel-moving-average-size uk-button" unit="WoW" value="7">WoW</button>
                            <button type="button" class="funnel-moving-average-size uk-button" unit="Wo2W" value="14" >Wo2W</button>
                            <button type="button" class="funnel-moving-average-size uk-button" unit="Wo4W" value="28">Wo4W</button>
                        </div>
                    </div>

                    <div class="uk-display-inline-block">
                        <div class="funnel-cumulative" data-uk-button-checkbox><button type="button" class="uk-button">cummulative</button>
                        </div>
                    </div>
                    <div class="uk-display-inline-block uk-margin-right">
                        <button type="submit" id="funnel-submit" class="uk-button uk-button-small uk-button-primary ">Go</button>
                    </div>
                </div>
            </form>
        </div>
        <div class="custom-funnel-section" style="min-height:500px;"><i class="uk-icon-spin uk-icon-large" style="margin: auto;"></i></div>

<div id="funnel-thumbnails" class="funnel-thumbnail-container uk-margin-bottom pa" data-uk-grid>

    <#list funnelViews as funnel>
        <div>
            <p class="metric-list">
            ${funnel.name} <br> (current = ${funnel.current} & baseline = ${funnel.baseline})
            </p>
            <div class="funnel-wrapper">
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
            </div>
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
            var errorMessage = $("#funnel-error > p")
            errorMessage.empty()

            //Todo: go through the possibleerrors
            /*if () {
                errorMessage.html("...")
                errorAlert.fadeIn(100)
                return
            }*/

            // Date input field validation
            var date = $("#funnel-date").val()
            if (!date) {
                errorMessage.html("Must provide date")
                errorAlert.fadeIn(100)
                return
            }

            //Baseline checkbox validation
            if ($(".funnel-baseline-unit.uk-active").length == 0 ) {
                errorMessage.html("Please select a baseline: days or weeks")
                errorAlert.fadeIn(100)
                return
            }

            if($(".funnel-moving-average-size.uk-active").length == 0 ){
                errorMessage.html("Please select a moving avarage size: WoW, Wo2W, Wo4W")
                errorAlert.fadeIn(100)
                return
            }

            // Timezone
            var timezone = getTimeZone()

            // Aggregate  todo: talke the metrics from the URI instead of the sidenav
            var aggregateSize = parseInt($("#sidenav-aggregate-size").val())
            var aggregateUnit = $("#sidenav-aggregate-unit").val()
            var aggregateMillis = toMillis(aggregateSize, aggregateUnit)

            // Baseline
            var baselineSize = 1 //parseInt($("#sidenav-baseline-size:selected").val())
            var baselineUnit = parseInt($(".funnel-baseline-unit.uk-active").val())

            // Date
            var current = moment.tz(date, timezone)
            var baseline = moment(current.valueOf() - (baselineSize * baselineUnit))
            var currentMillisUTC = current.utc().valueOf()
            var baselineMillisUTC = baseline.utc().valueOf()

            // Metric(s)  todo: talke the metrics from the URI instead of the sidenav
            var metrics = []
            $(".sidenav-metric").each(function(i, checkbox) {
                var checkboxObj = $(checkbox)
                if (checkboxObj.is(':checked')) {
                    metrics.push("'" + checkboxObj.val() + "'")
                }
            });

            // Derived metric(s) todo: talke the metrics from the URI instead of the sidenav
            $("#sidenav-derived-metrics-list").find(".uk-form-row").each(function(i, row) {
                var type = $(row).find(".derived-metric-type").find(":selected").val()
                var args = []
                $(row).find(".derived-metric-arg").each(function(j, arg) {
                    args.push("'" + $(arg).find(":selected").val() + "'")
                })
                metrics.push(type + '(' + args.join(',') + ')')
            });

            // Metric function
            var metricFunction = metrics.join(",")

            // Moving average
            if ($(".funnel-moving-average-size.uk-active").length > 0) {
                var movingAverageSize = $(".funnel-moving-average-size.uk-active").val()
                var movingAverageUnit = "DAYS"
                metricFunction = "MOVING_AVERAGE_" + movingAverageSize + "_" + movingAverageUnit + "(" + metricFunction + ")"
            }

            // Aggregate
            metricFunction = "AGGREGATE_" + aggregateSize + "_" + aggregateUnit + "(" + metricFunction + ")"

            //Query Parameters
            if(queryParams.hasOwnProperty("")){
                delete queryParams[""]
            }

            // Path
            var path = parsePath(window.location.pathname)
            path.metricFunction = metricFunction
            path.metricViewType = path.metricViewType == null ? "INTRA_DAY" : path.metricViewType
            path.dimensionViewType = path.dimensionViewType == null ? "HEAT_MAP" : path.dimensionViewType
            path.baselineMillis = baselineMillisUTC
            path.currentMillis = currentMillisUTC
            debugger;
            var dashboardPath = getDashboardPath(path)

            var params = parseHashParameters(window.location.hash)
            if(timezone !== getLocalTimeZone().split(' ')[1]) {
                params.timezone = timezone.split('/').join('-')
            }

            if (queryParams.funnels) {
                var funnels = decodeURIComponent(queryParams.funnels).split( ",")
                if (funnels.length > 0) {
                    queryParams["funnels"] = funnels.join();
                }
            }

            errorAlert.hide()
            window.location = dashboardPath + encodeDimensionValues(queryParams) + encodeHashParameters(params)
        });


        //When a funnel thumbnail is clicked display it's content in the main funnel display section
        $("#funnel-thumbnails>div").click(function(){

            //Draw the selected thumbnail table and title in the main section
            $("#custom-funnel-title > h3").html($('.metric-list', this).html())
            $(".custom-funnel-section").html($(".funnel-wrapper", this).html())

            //Highlight currently selected thumbnail
            $("#funnel-thumbnails>div").removeClass("uk-panel-box")
            $(this).addClass("uk-panel-box")
        })

        //Assign background color value to each cells
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
        $("#funnel-thumbnails>div:first-of-type").trigger("click")

        //Load existing date selection
        var currentDateTime = moment(parseInt(path.currentMillis))
        var dateString = currentDateTime.format("YYYY-MM-DD")
        $("#funnel-date").val(dateString)

        // Load existing metrics selection / function if the value is present in the options of the funnel form
        var metricFunctionObj = parseMetricFunction(decodeURIComponent(path.metricFunction))

        // Always start at AGGREGATE
        var tokens = metricFunctionObj.name.split("_")

        // May have applied moving average as well
        var firstArg = metricFunctionObj.args[0]
        if (typeof(firstArg) === 'object') {
            if (firstArg.name && firstArg.name.indexOf("MOVING_AVERAGE") >= 0) {
                metricFunctionObj = firstArg
                var tokens = metricFunctionObj.name.split("_")
                if($(".funnel-moving-average-size[value='" + tokens[tokens.length - 2] + "']").length > 0){
                    $(".funnel-moving-average-size[value='" + tokens[tokens.length - 2] + "']").trigger("click")
                }
            }
        }

        var baselineDateTime = moment(parseInt(path.baselineMillis))
        var diffMillis = currentDateTime.valueOf() - baselineDateTime.valueOf()
        var diffDescriptor = describeMillis(diffMillis)
        if($(".funnel-baseline-unit[value='" + diffDescriptor.sizeMillis +"'").length > 0){
            $(".funnel-baseline-unit[value='" + diffDescriptor.sizeMillis +"'").trigger("click")
        }
    });
</script>
</#if>    
