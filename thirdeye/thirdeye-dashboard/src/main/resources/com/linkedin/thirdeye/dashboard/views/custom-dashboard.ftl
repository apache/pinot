<html>
    <head>
        <meta charset="UTF-8">
        <#include "common/script.ftl">

        <style>
        .not-available {
            background-color: #ffd700;
        }

        table {
            table-layout: fixed;
            border-collapse: collapse;
        }

        th,td {
            padding: 5px;
        }

        .time-series-data {
            display: none;
        }

        .time-series-container {
            height: 400px;
        }
        </style>
    </head>
    <body>
        <h1>${name}</h1>

        <#list componentViews as componentView>
            <h2>${componentView.first.name}</h2>
            <#if (componentView.first.type == "FUNNEL")>
                <table>
                    <tr>
                        <th><div class="rotate">Hour</div></th>
                        <#list componentView.second.metricLabels as label>
                            <th title="${label}">${label_index + 1}</th>
                        </#list>
                    </tr>
                    <#list componentView.second.table as row>
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

                <h3>Legend</h3>
                <dl>
                    <#list componentView.second.metricLabels as label>
                        <dt>${label_index + 1}</dt>
                        <dd>${label}</dd>
                    </#list>
                </dl>

                <#if componentView.first.dimensions??>
                    <h3>Dimensions</h3>
                    <dl>
                    <#list componentView.first.dimensions?keys as dimension>
                        <dt>${dimension}</dt>
                        <dd>${componentView.first.dimensions[dimension]}</dd>
                    </#list>
                    </dl>
                </#if>
            <#elseif (componentView.first.type == "TIME_SERIES")>
                <div class="time-series">
                    <div class="time-series-container"></div>
                    <div class="time-series-data">${componentView.second.jsonString}</div>
                </div>
            <#else>
                <p>No component type ${componentView.first.type}</p>
            </#if>
        </#list>

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
            })

            $(".time-series").each(function(i, timeSeries) {
                var timeSeriesObj = $(timeSeries)
                var data = JSON.parse(timeSeriesObj.find(".time-series-data").html())
                var container = $(timeSeriesObj.find(".time-series-container"))
                container.plot(data, {
                    xaxis: {
                        tickFormatter: function(millis) {
                            return moment.utc(millis).tz(jstz().timezone_name).format("YYYY-MM-DD HH:mm")
                        },
                        minTickSize: 3600000
                    }
                })
            })
        })
        </script>
    </body>
</html>
