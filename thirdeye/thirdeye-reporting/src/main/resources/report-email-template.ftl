<html>
<head>
  <title><b>${reportConfig.name}</b></title>
</head>
<body>

<table>
  <tr><td><b>Start Time</b></td><td>${reportConfig.startTime}</td></tr>
  <tr><td><b>End Time</b></td><td>${reportConfig.endTime}</td></tr>
</table><br>


<#list tables as table>
${table.thirdeyeURI}
  <table border=1>

    <tr>

      <td>

        <table>
          <tr><b>GroupBy</b></tr>
          <tr><td style="width:300px"><b>Time</b></td><#if (table.tableSpec.groupBy)??><td style="width=200px"><b>${table.tableSpec.groupBy}</b></td></#if></tr>
          <#list table.groupBy as groupBy>
          <tr><td>${groupBy.startTime}</td><#if (table.tableSpec.groupBy)??><td>${groupBy.dimension}</td></#if></tr>
          </#list>
        </table>

      </td>

      <#assign metrics = table.metricReportRows?keys>
      <#list metrics as metric>
      <td>
        <table>
          <tr><b>${metric}</b></tr>
          <tr><td style="width=200px"><b>Current</b></td><td style="width=200px"><b>Baseline</b></td><td style="width=200px"><b>Ratio</b></td></tr>
          <#list table.metricReportRows[metric] as row>
          <tr><td>${row.current}</td><td>${row.baseline}</td><td>${row.ratio}</td></tr>
          </#list>
        </table>
      </td>
      </#list>

    </tr>

  </table>
  <br><br>
</#list>

<#include "thirdeye-anomaly-template.ftl">

</body>
</html>