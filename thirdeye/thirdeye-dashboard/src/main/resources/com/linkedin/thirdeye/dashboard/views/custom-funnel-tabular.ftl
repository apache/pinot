<html>
    <head>
        <meta charset="UTF-8">
        <#include "common/style.ftl">
        <#include "common/script.ftl">

        <style>
        table {
            table-layout: fixed;
            width: 1000px;
            margin-left: 2%;
            margin-top: 1%;
            border-collapse: collapse;
        }
        td {
            word-wrap: break-word;
        }
        .metric-label {
            padding-left: 20px;
            transform: rotate(45deg);
            transform-origin: left top 0;
            word-wrap: normal;
        }
        .not-available {
            background-color: #ffd700;
        }
        </style>
    </head>

    <body>
        <table>
            <#list table as row>
                <tr>
                    <td>${row.first.name}</td>
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
            <tr>
                <td class="metric-label">Hour</td>
                <#list metricLabels as label>
                    <td class="metric-label">${label}</td>
                </#list>
            </tr>
        </table>
    </body>

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
    })
    </script>
</html>