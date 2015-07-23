<html>
    <head>
        <meta charset="UTF-8">
        <#include "common/script.ftl">
        <#include "common/style.ftl">

        <style>
        .not-available {
            background-color: #ffd700;
        }

        table {
            table-layout: fixed;
            border-collapse: collapse;
        }

        th,td {
            padding: 7px;
            overflow: hidden;
        }

        .time-series-data {
            display: none;
        }

        .time-series-container {
            margin-top: 1%;
            height: 400px;
        }

        .uk-datepicker {
            width: 300px;
        }

        body {
            width: 80%;
            margin-left: auto;
            margin-right: auto;
            margin-top: 2%;
        }
        </style>
    </head>
    <body>
        <h1>${name}</h1>
        <form class="uk-form">
          <div class="uk-form-row">
              <div class="uk-form-icon">
                  <i class="uk-icon-calendar"></i>
                  <input id="custom-dashboard-date" type="text" data-uk-datepicker="{format:'YYYY-MM-DD'}" />
              </div>
          </div>
        </form>

        <#list componentViews as componentView>
            <div class="uk-grid component-view">
            <#if (componentView.first.type == "FUNNEL")>
                <div class="uk-width-1-2">
                  <h2>${componentView.first.name}</h2>
                  <table>
                      <col width="60px" />
                      <#list componentView.second.metricLabels as label>
                        <col width="60px" />
                      </#list>

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
                </div>

                <div class="uk-width-1-2">
                  <h3>Legend</h3>
                  <dl class="uk-description-list-horizontal">
                      <#list componentView.second.metricLabels as label>
                          <dt>${label_index + 1}</dt>
                          <dd>${label}</dd>
                      </#list>
                  </dl>

                  <#if componentView.first.dimensions??>
                      <h3>Dimensions</h3>
                      <dl class="uk-description-list-horizontal">
                      <#list componentView.first.dimensions?keys as dimension>
                          <dt>${dimension}</dt>
                          <dd>${componentView.first.dimensions[dimension]}</dd>
                      </#list>
                      </dl>
                  </#if>
                </div>
            <#elseif (componentView.first.type == "TIME_SERIES")>
                <div class="uk-width-1-1">
                  <h2>${componentView.first.name}</h2>
                </div>
                <div class="uk-width-1-1">
                  <div class="time-series">
                      <div class="time-series-container"></div>
                      <div class="time-series-data">${componentView.second.jsonString}</div>
                  </div>
                </div>
            <#else>
                <p>No component type ${componentView.first.type}</p>
            </#if>
            </div>
        </#list>

        <script>
        $(document).ready(function() {

            // Last three components of path are year / month / day
            var pathTokens = window.location.pathname.split('/')
            var day = pathTokens[pathTokens.length - 1]
            var month = pathTokens[pathTokens.length - 2]
            var year = pathTokens[pathTokens.length - 3]
            $("#custom-dashboard-date").val(year + '-' + month + '-' + day)

            $("#custom-dashboard-date").change(function() {
                var timeTokens = $(this).val().split('-')
                var pathTokens = window.location.pathname.split('/')
                pathTokens[pathTokens.length - 3] = timeTokens[0]
                pathTokens[pathTokens.length - 2] = timeTokens[1]
                pathTokens[pathTokens.length - 1] = timeTokens[2]
                window.location.pathname = pathTokens.join('/')
            })

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
