<#-- stand-alone -->
<#if (!metricView??)>
    <#include "../common/style.ftl">
    <#include "../common/script.ftl">
</#if>
<script src="/assets/js/thirdeye.metric.table.js"></script>

<#list (metricView.view.metricTables)!metricTables as metricTable>
    <#assign dimensions = metricTable.dimensionValues>
    <#assign dimensionAliases = (metricView.view.dimensionAliases)!dimensionAliases>
    <#include "../common/dimension-header.ftl">


<div class="collapser"><h2>(-) Metric Intra-day View</h2></div>
<div id="metric-table-area">
    <#if (((metricView.view.metricTables)!metricTables)?size == 0)>
        <div class="uk-alert uk-alert-warning">
            <p>No data available</p>
        </div>
    </#if>

        <div id="intra-day-buttons">
          <button class="uk-button uk-modal-close" data-uk-modal="{target:'#intra-day-config'}">
            <i class="uk-icon-cog"></i>
          </button>
        </div>

        <div id="intra-day-config" class="uk-modal">
          <div class="uk-modal-dialog">
            <form class="uk-form">
              <fieldset>
                  <legend>Order</legend>
                  <div class="uk-form-row">
                    <ul id="intra-day-table-order" class="uk-sortable uk-grid uk-grid-small uk-grid-width-1-1" data-uk-sortable>
                      <#list (metricView.view.metricNames)!metricNames as metricName>
                        <li class="uk-grid-margin">
                          <div class="uk-panel uk-panel-box">
                            ${(metricView.view.metricAliases!metricAliases)[metricName]!metricName}
                          </div>
                        </li>
                      </#list>
                    </ul>
                  </div>
              </fieldset>
            </form>

            <div class="uk-modal-footer uk-text-right">
              <button id="intra-day-update" class="uk-button uk-button-primary uk-modal-close">Update</button>
            </div>
            
          </div>
        </div>

        <table id="intra-day-table" class="uk-table uk-table-striped">
            <thead>
                <tr>
                    <th></th>
                    <#assign groupIdx = 0>
                    <#list (metricView.view.metricNames)!metricNames as metricName>
                        <#assign groupId = (groupIdx % 2)>
                        <th colspan="3" class="uk-text-center metric-table-group-${groupId}">
                          ${(metricView.view.metricAliases!metricAliases)[metricName]!metricName}
                        </th>
                        <#assign groupIdx = groupIdx + 1>
                    </#list>
                </tr>
                <tr>
                    <th></th>
                    <#assign groupIdx = 0>
                    <#list (metricView.view.metricNames)!metricNames as metricName>
                        <#assign groupId = (groupIdx % 2)>
                        <th class="metric-table-group-${groupId}">Current</th>
                        <th class="metric-table-group-${groupId}">Baseline</th>
                        <th class="metric-table-group-${groupId}">Ratio</th>
                        <#assign groupIdx = groupIdx + 1>
                    </#list>
                </tr>
            </thead>

            <tbody>
                <#list metricTable.rows as row>
                    <tr>
                        <#-- This renders time in UTC (moment.js used to convert to local) -->
                        <td class="metric-table-time" title="${row.baselineTime}" currentUTC="${row.currentTime}">${row.currentTime}</td>
                        <#list 0..(row.numColumns-1) as i>
                            <#assign groupId = (i % 2)>
                            <td class="metric-table-group-${groupId}">${row.current[i]?string!"N/A"}</td>
                            <td class="metric-table-group-${groupId}">${row.baseline[i]?string!"N/A"}</td>
                                <#if row.ratio[i]??>
                                    <td class="
                                        ${(row.ratio[i] < 0)?string('metric-table-down-cell', '')}
                                        ${(row.ratio[i] == 0)?string('metric-table-same-cell', '')}
                                        metric-table-group-${groupId}
                                    ">
                                    ${(row.ratio[i] * 100)?string["0.00"] + "%"}
                                    </td>
                                <#else>
                                    <td class="metric-table-group-${groupId}">N/A</td>
                                </#if>
                        </#list>
                    </tr>
                </#list>
            </tbody>
        </table>
    </#list>
</div>