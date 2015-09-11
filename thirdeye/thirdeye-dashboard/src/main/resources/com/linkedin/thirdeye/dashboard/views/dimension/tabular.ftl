<script src="/assets/js/thirdeye.dimension.tabular.js"></script>

<div class="collapser"><h2>(-) Dimension Tabular View</h2></div>
<div id="dimension-table-area">
    <#if (((dimensionView.view.dimensionTables)!dimensionTables)?size == 0)>
        <div class="uk-alert uk-alert-warning">
            <p>No data available</p>
        </div>
    </#if>

    <#list dimensionView.view.dimensionTables as dimensionTable>
        <div class="collapser"><h3>(-) ${dimensionTable.dimensionAlias!dimensionTable.dimensionName}</h3></div>
        <table class="uk-table uk-table-striped dimension-tabular-table">
          <thead>
            <tr>
              <th></th>
              <#assign groupIdx = 0>
              <#list dimensionTable.metricNames as metricName>
                <#assign groupId = (groupIdx % 2)>
                <th colspan="3" class="uk-text-center metric-table-group-${groupId}">
                  ${dimensionTable.metricAliases[metricName]!metricName}
                </th>
                <#assign groupIdx = groupIdx + 1>
              </#list>
            </tr>
          </thead>
          <tbody>
            <#list dimensionTable.rows as row>
              <tr>
                <td>${row.value}</td>
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
