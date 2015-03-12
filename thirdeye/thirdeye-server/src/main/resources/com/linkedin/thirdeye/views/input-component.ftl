<form class="uk-form" id="input-form">
    <div class="input-form-component">
        <label class="uk-form-label" for="input-primary-metric">Primary Metric</label>
        <div class="uk-button uk-form-select" data-uk-form-select>
            <span></span>
            <i class="uk-icon-caret-down"></i>
            <select id="input-primary-metric">
                    <#list metricNames as metricName>
                        <#if metricName == primaryMetricName>
                            <option selected>${metricName}</option>
                        <#else>
                            <option>${metricName}</option>
                        </#if>
                    </#list>
            </select>
        </div>
    </div>

    <div class="input-form-component">
        <label class="uk-form-label" for="input-date">Date (UTC)</label>
        <div class="uk-form-icon">
            <i class="uk-icon-calendar"></i>
            <input id="input-date" type="text" data-uk-datepicker="{weekstart:0, format:'MM/DD/YYYY'}"/>
        </div>
    </div>

    <div class="input-form-component">
        <label class="uk-form-label" for="input-time">Time</label>
        <div class="uk-form-icon" data-uk-timepicker>
            <i class="uk-icon-clock-o"></i>
            <input id="input-time" type="text"/>
        </div>
    </div>

    <div class="input-form-component">
        <label class="uk-form-label">
            Baseline
            <input type="number" id="input-baseline-size" min="0" value="1" />
        </label>
    </div>

    <div class="input-form-component">
        <label class="uk-form-label">
            <input type="radio" id="input-baseline-unit-week" name="input-baseline-unit" value="week" checked>
            Week
        </label>

        <label class="uk-form-label">
            <input type="radio" id="input-baseline-unit-month" name="input-baseline-unit" value="month">
            Month
        </label>

        <label class="uk-form-label">
            <input type="radio" id="input-baseline-unit-year" name="input-baseline-unit" value="year">
            Year
        </label>
    </div>


    <div class="input-form-component">
        <input type="hidden" id="input-collection" name="input-collection" value="${collection}"/>
        <input type="hidden" id="input-date-time-millis" name="input-date-time-millis" value="${dateTimeMillis?string["0"]}"/>
        <button class="uk-button uk-button-primary input-go" type="button">Go</button>
    </div>
</form>

<div id="funnel-options" class="uk-modal">
    <div class="uk-modal-dialog">
        <a class="uk-modal-close uk-close"></a>

        <h2>Funnel</h2>

        <p>
            Drag the metrics from left to right to compose a funnel.
        </p>

        <hr/>

        <div id="funnel-drag-area">
            <jquery-ui>
                <ul id="funnel-sortable1" class="connectedSortable">
                    <#list metricNames as metricName>
                        <li class="ui-state-default">${metricName}</li>
                    </#list>
                </ul>

                <ul id="funnel-sortable2" class="connectedSortable">
                </ul>
            </jquery-ui>
        </div>

        <p>
            <button class="uk-button uk-width-1-1 uk-modal-close input-go">Done</button>
        </p>
    </div>
</div>

<div id="normalization-options" class="uk-modal">
    <div class="uk-modal-dialog">
        <a class="uk-modal-close uk-close"></a>

        <h2>Normalization</h2>

        <p>
            Use the following options to normalize the time series.
        </p>

        <p>
            Self-normalization converts each value in a series to a ratio of the current value and the
            first value in the series.
        </p>

        <p>
            Primary-normalization converts each value in a series to a ratio of the current value and the
            first value in the <i>primary</i> series. This can be used to construct funnel-style views.
        </p>

        <hr/>

        <form class="uk-form uk-form-horizontal" id="normalization-options-form">
            <div class="uk-form-row">
                <label>
                    <input type="radio" id="normalization-type-none" name="normalization-type" value="none" checked/>
                    None
                </label>
            </div>
            <div class="uk-form-row">
                <label>
                    <input type="radio" id="normalization-type-self" name="normalization-type" value="self"/>
                    Self
                </label>
            </div>
            <div class="uk-form-row">
                <label>
                    <input type="radio" id="normalization-type-primary" name="normalization-type" value="primary"/>
                    Primary
                    <select id="input-primary-normalization-metric">
                            <#list metricNames as metricName>
                                <#if metricName != primaryMetricName>
                                    <option selected>${metricName}</option>
                                <#else>
                                    <option>${metricName}</option>
                                </#if>
                            </#list>
                    </select>
                </label>
            </div>
        </form>

        <p>
            <button class="uk-button uk-width-1-1 uk-modal-close input-go">Done</button>
        </p>
    </div>
</div>

<div id="function-options" class="uk-modal">
    <div class="uk-modal-dialog">
        <a class="uk-modal-close uk-close"></a>

        <h2>Function</h2>

        <p>
            You can implement the body of a function which accepts <code>series</code> object,
            and returns a modified <code>series</code> object.
        </p>

        <p>
            An example series object is
<pre>
{
    "metricName": {
        "label": <some_label>,
        "data": <[[time,value]]>
    }
}
</pre>
        </p>

        <hr/>

        <form class="uk-form">
                <br/>
                <textarea id="user-function" rows="10" cols="80"></textarea>
                <br/>

                <p>
                    <button class="uk-button uk-width-1-1 uk-modal-close input-go" id="user-function-evaluate">Evaluate</button>
                </p>
        </form>
    </div>
</div>

<div id="smoothing-options" class="uk-modal">
    <div class="uk-modal-dialog">
        <a class="uk-modal-close uk-close"></a>

        <h2>Smoothing</h2>

        <p>
            Use the following options to smooth the time series and heat maps.
        </p>

        <hr/>

        <form class="uk-form uk-form-horizontal" id="smoothing-options-form">

            <div class="uk-form-row">
              <label class="uk-form-label">
                <input type="radio" id="smoothing-option-none" name="smoothing-option" value="none" checked>
                None
              </label>
            </div>

            <div class="uk-form-row">
                <label class="uk-form-label">
                  <input type="radio" id="smoothing-option-aggregate" name="smoothing-option" value="aggregate">
                  Aggregate
                </label>
                <div class="uk-form-controls uk-form-controls-text">
                    <p class="uk-form-controls-condensed">
                        <input type="number" min="1" value="1" id="smoothing-aggregate-size"/>
                        <label>
                            <input type="radio" id="smoothing-aggregate-unit-hour" name="smoothing-aggregate-unit" value="hour" checked>
                            Hour
                        </label>
                        <label>
                            <input type="radio" id="smoothing-aggregate-unit-day" name="smoothing-aggregate-unit" value="day">
                            Day
                        </label>
                        <label>
                            <input type="radio" id="smoothing-aggregate-unit-week" name="smoothing-aggregate-unit" value="week">
                            Week
                        </label>
                    </p>
                </div>
            </div>

            <div class="uk-form-row">
                <label class="uk-form-label">
                  <input type="radio" id="smoothing-option-movingAverage" name="smoothing-option" value="movingAverage">
                  Moving Average
                </label>
                <div class="uk-form-controls uk-form-controls-text">
                    <p class="uk-form-controls-condensed">
                        <input type="number" min="1" value="1" id="smoothing-moving-average-size"/>
                        <label>
                            <input type="radio" id="smoothing-moving-average-unit-hour" name="smoothing-moving-average-unit" value="hour" checked>
                            Hour
                        </label>
                        <label>
                            <input type="radio" id="smoothing-moving-average-unit-day" name="smoothing-moving-average-unit" value="day">
                            Day
                        </label>
                        <label>
                            <input type="radio" id="smoothing-moving-average-unit-week" name="smoothing-moving-average-unit" value="week">
                            Week
                        </label>
                    </p>
                </div>
            </div>
        </form>

        <p>
            <button class="uk-button uk-width-1-1 uk-modal-close input-go">Done</button>
        </p>
    </div>
</div>

<script>
$(document).ready(function() {

    moment.utc().format()

    $("#funnel-sortable1, #funnel-sortable2").sortable({
        connectWith: ".connectedSortable"
    }).disableSelection()

    var millis = parseInt($("#input-date-time-millis").val())
    var dateTime = moment.utc(millis)
    var dateString = dateTime.format("MM/DD/YYYY")
    var timeString = dateTime.format("HH:mm")

    $("#input-date").val(dateString)
    $("#input-time").val(timeString)

    // Set funnel
    var pathTokens = window.location.pathname.split("/")
    for (var i = 0; i < pathTokens.length; i++) {
        if (pathTokens[i] === "funnel") {
            var funnelTokens = pathTokens[i+1].split(":")
            var funnelMetrics = funnelTokens[1].split(",")

            var selectedMetrics = {}

            // Remove from available and add to selected
            $("#funnel-sortable1 li").each(function(i, elt) {
                var funnelMetric = $(elt)
                if ($.inArray(funnelMetric.text(), funnelMetrics) > -1) {
                    funnelMetric.remove()
                    selectedMetrics[funnelMetric.text()] = funnelMetric
                }
            })

            // Add to selected
            $.each(funnelMetrics, function(i, elt) {
                $("#funnel-sortable2").append(selectedMetrics[elt])
            })

            break
        }
    }

    var tokens = window.location.href.split("#")
    if (tokens.length > 1) {
        var hashRoute = {}
        var hashKeyValuePairs = tokens[1].split("&")
        for (var i = 0; i < hashKeyValuePairs.length; i++) {
            var pair = hashKeyValuePairs[i].split("=")
            hashRoute[decodeURIComponent(pair[0])] = decodeURIComponent(pair[1])
        }

        if (hashRoute["baselineSize"]) {
            $("#input-baseline-size").val(hashRoute["baselineSize"])
        }

        if (hashRoute["baselineUnit"]) {
            $("#input-baseline-unit-" + hashRoute["baselineUnit"]).attr('checked', true)
        }

        if (hashRoute["movingAverageSize"]) {
            $("#smoothing-moving-average-size").val(hashRoute["movingAverageSize"])
            $("#smoothing-moving-average-unit-" + hashRoute["movingAverageUnit"]).attr('checked', true)
            $("#smoothing-moving-average").attr('checked', true)
        }

        if (hashRoute["aggregateSize"]) {
            $("#smoothing-aggregate-size").val(hashRoute["aggregateSize"])
            $("#smoothing-aggregate-unit-" + hashRoute["aggregateUnit"]).attr('checked', true)
            $("#smoothing-aggregate").attr('checked', true)
        }

        if (hashRoute["normalizationType"]) {
            $("#normalization-type-" + hashRoute["normalizationType"]).attr('checked', true)
        }

        if (hashRoute["smoothingType"]) {
            $("#smoothing-option-" + hashRoute["smoothingType"]).attr('checked', true)
        }
    }

    $(".input-go").click(function() {

        var inputDate = $("#input-date").val()
        if (!inputDate) {
            alert("Date required")
            return
        }

        var inputTime = $("#input-time").val()
        if (!inputTime) {
            alert("Time required")
            return
        }

        var baselineSize = $("#input-baseline-size").val()
        var baselineUnit = $('input[name=input-baseline-unit]:checked', '#input-form').val()
        var currentMillis = moment.utc(inputDate + " " + inputTime).valueOf()
        var baselineMillis = currentMillis - convertToMillis(baselineSize, baselineUnit)
        var primaryMetricName = $("#input-primary-metric").val()
        var collection = $("#input-collection").val()
        var normalizationType = $("input[name=normalization-type]:checked", "#normalization-options-form").val()

        var url = '/dashboard/' + collection + '/volume/' + primaryMetricName + '/' + baselineMillis + '/' + currentMillis

        var funnel = []
        $("#funnel-sortable2 li").each(function(i, elt) {
            funnel.push($(elt).text())
        })
        if (funnel.length > 0) {
            url += "/funnel/top:" + funnel.join(",")
        }

        var hashRoute = {
            'baselineSize': baselineSize,
            'baselineUnit': baselineUnit,
            'normalizationType': normalizationType
        }

        var incomingHashRoute = {}
        if (window.location.hash) {
            var hashKeyValuePairs = window.location.hash.substring(1).split("&")
            for (var i = 0; i < hashKeyValuePairs.length; i++) {
                var pair = hashKeyValuePairs[i].split("=")
                incomingHashRoute[decodeURIComponent(pair[0])] = decodeURIComponent(pair[1])
            }
        }

        if (incomingHashRoute["selectedMetrics"]) {
            hashRoute["selectedMetrics"] =  incomingHashRoute["selectedMetrics"];
        }

        var smoothingType = $('input[name=smoothing-option]:checked', '#smoothing-options-form').val()
        hashRoute["smoothingType"] = smoothingType

        if (smoothingType === "aggregate") {
            var size = parseInt($("#smoothing-aggregate-size").val())
            var unit = $("input[name=smoothing-aggregate-unit]:checked", "#smoothing-options-form").val()
            var aggregateMillis = convertToMillis(size, unit)
            url += '/aggregate/' + aggregateMillis
            hashRoute['aggregateSize'] = size
            hashRoute['aggregateUnit'] = unit
        } else if (smoothingType === "movingAverage") {
            var size = parseInt($("#smoothing-moving-average-size").val())
            var unit = $("input[name=smoothing-moving-average-unit]:checked", "#smoothing-options-form").val()
            var movingAverageMillis = convertToMillis(size, unit)
            url += '/movingAverage/' + movingAverageMillis
            hashRoute['movingAverageSize'] = size
            hashRoute['movingAverageUnit'] = unit
        }

        if (normalizationType === "self") {
            url += "/normalized/*"
        } else if (normalizationType === "primary") {
            url += "/normalized/" + $("#input-primary-normalization-metric").val()
        }

        url += window.location.search

        url += '#'
        $.each(hashRoute, function(key, val) {
            url += '&' + encodeURIComponent(key) + '=' + encodeURIComponent(val)
        })

        if ((window.location.origin + url) !== window.location.href) {
            window.location = url
        }
    })

    function convertToMillis(size, unit) {
        if (unit === "hour") {
            return size * 60 * 60 * 1000
        } else if (unit === "day") {
            return size * 24 * 60 * 60 * 1000
        } else if (unit === "week") {
            return size * 7 * 24 * 60 * 60 * 1000
        } else if (unit === "month") {
            return size * 30 * 24 * 60 * 60 * 1000
        } else if (unit === "year") {
            return size * 364 * 24 * 60 * 60 * 1000
        }
    }
})
</script>

<style>

#funnel-drag-area {
    margin-bottom: 10px;
}

#funnel-sortable1, #funnel-sortable2 {
    border: 1px solid #eee;
    width: 45%;
    min-height: 20px;
    list-style-type: none;
    margin: 0;
    padding: 5px 0 0 0;
    float: left;
    margin-right: 10px;
}

#funnel-sortable1 {
    float: left;
}

#funnel-sortable2 {
    float: right;
}

#funnel-sortable1 li, #funnel-sortable2 li {
    margin: 0 5px 5px 5px;
    padding: 5px;
    font-size: 1em;
}

#funnel-options .uk-modal-dialog {
    overflow: hidden;
}

#funnel-drag-area {
    overflow: hidden;
}
</style>
