<html>
<head>
  <title><b>${reportConfig.name}</b></title>
</head>
<body>

<br>
<table cellspacing="2" cellpadding="2" style="${reportEmailCssSpec.tableStyle1}">
  <tbody>
    <tr>
      <td style="${reportEmailCssSpec.timeTitleStyle}">Start</td>
      <td style="${reportEmailCssSpec.cellStyle1}">${reportConfig.startTimeString}</td>
    <tr>
      <td style="${reportEmailCssSpec.timeTitleStyle}">End</td>
      <td style="${reportEmailCssSpec.cellStyle1}">${reportConfig.endTimeString}</td>
    </tr>
  </tbody>
</table><br><br>

<table cellpadding="0" cellspacing="0">
 <tbody><tr height="23" style="height:22.5pt">
<td colspan="1" style="font-family:tahoma;font-size:14.0pt">${reportConfig.name}</td></tr>
<tr style="height:10.5pt"><td></td></tr>
</tbody></table>


<#list tables as table>

<table cellspacing="2" cellpadding="2" style="${reportEmailCssSpec.tableStyle1}">
  <tbody>
    <tr></tr>
    <tr>
      <td style="${reportEmailCssSpec.timeTitleStyle}">Current</td>
      <td style="${reportEmailCssSpec.cellStyle1}">
        ${scheduleSpec.aggregationSize} <#if scheduleSpec.aggregationSize == 1>${scheduleSpec.aggregationUnit?substring(0,scheduleSpec.aggregationUnit?last_index_of("S"))}<#else>${scheduleSpec.aggregationUnit}</#if>
      </td>
    <tr>
      <td style="${reportEmailCssSpec.timeTitleStyle}">Baseline</td>
      <td style="${reportEmailCssSpec.cellStyle1}">
        <#if table.tableSpec.baselineUnit == "DAYS" && table.tableSpec.baselineSize % 7 == 0>
          ${table.tableSpec.baselineSize / 7} <#if table.tableSpec.baselineSize / 7 == 1>WEEK<#else>WEEKS</#if> ago
        <#else>
          ${table.tableSpec.baselineSize} <#if table.tableSpec.baselineSize == 1>${table.tableSpec.baselineUnit?substring(0,table.tableSpec.baselineUnit?last_index_of("S"))}<#else>${table.tableSpec.baselineUnit}</#if> ago
        </#if>
      </td>
    </tr>
  </tbody>
</table>
  <table cellspacing="0" cellpadding="2" style="${reportEmailCssSpec.tableStyle2}">
    <tbody>

      <tr>
        <td style="${reportEmailCssSpec.titleStyle2}">&nbsp;&nbsp;&nbsp;&nbsp;</td>
        <#if (table.tableSpec.groupBy)??><td style="${reportEmailCssSpec.titleStyle3}">&nbsp;&nbsp;&nbsp;&nbsp;</td></#if>
        <#list table.tableReportRows[0].rows as row>
          <td colspan="3" style="${reportEmailCssSpec.metricTitleStyle}">&nbsp;&nbsp;${row.metric}&nbsp;&nbsp;</td>
        </#list>
      </tr>

      <tr>
        <td style="${reportEmailCssSpec.hourTitleStyle}">&nbsp;&nbsp;Hour&nbsp;&nbsp;</td>
        <#if (table.tableSpec.groupBy)??><td style="${reportEmailCssSpec.groupByTitleStyle}">&nbsp;&nbsp;${table.tableSpec.groupBy}&nbsp;&nbsp;</td></#if>
        <#list table.tableReportRows[0].rows as row>
          <td style="${reportEmailCssSpec.currentTitleStyle}">&nbsp;&nbsp;Current&nbsp;&nbsp;</td>
          <td style="${reportEmailCssSpec.baselineTitleStyle}">&nbsp;&nbsp;Baseline&nbsp;&nbsp;</td>
          <td style="${reportEmailCssSpec.ratioTitleStyle}">&nbsp;&nbsp;Ratio&nbsp;&nbsp;</b></td>
        </#list>
      </tr>

      <#list table.tableReportRows as tableReportRow>
      <tr>
        <#if tableReportRow.groupByDimensions.startTime == "Total">
          <td style="${reportEmailCssSpec.cellStyle2}">&nbsp;&nbsp;${tableReportRow.groupByDimensions.startTime}&nbsp;&nbsp;</td>
          <#if (table.tableSpec.groupBy)??><td style="${reportEmailCssSpec.cellStyle2}"></td></#if>

          <#list tableReportRow.rows as row>
            <td style="${reportEmailCssSpec.metricCellTotalPositiveStyle}">&nbsp;&nbsp;${row.current}&nbsp;&nbsp;</td>
            <td style="${reportEmailCssSpec.metricCellTotalPositiveStyle}">&nbsp;&nbsp;${row.baseline}&nbsp;&nbsp;</td>
            <#if (row.ratio)?starts_with("-")>
              <td style="${reportEmailCssSpec.metricCellTotalNegativeStyle}">&nbsp;&nbsp;${row.ratio}%&nbsp;&nbsp;</td>
            <#else>
              <td style="${reportEmailCssSpec.metricCellTotalPositiveStyle}">&nbsp;&nbsp;${row.ratio}%&nbsp;&nbsp;</td>
            </#if>
          </#list>
        <#else>
          <td style="${reportEmailCssSpec.cellStyle3}"><span class="aBn" data-term="goog_481112314" tabindex="0"><span class="aQJ">&nbsp;${tableReportRow.groupByDimensions.startTime}&nbsp;</span></span></td>
          <#if (table.tableSpec.groupBy)??><td style="${reportEmailCssSpec.cellStyle3}"><span class="aBn" data-term="goog_481112314" tabindex="0"><span class="aQJ">&nbsp;&nbsp;&nbsp;&nbsp;${tableReportRow.groupByDimensions.dimension}&nbsp;&nbsp;</span></span></td></#if>

          <#list tableReportRow.rows as row>
            <td style="${reportEmailCssSpec.metricCellPositiveStyle}">&nbsp;&nbsp;${row.current}&nbsp;&nbsp;</td>
            <td style="${reportEmailCssSpec.metricCellPositiveStyle}">&nbsp;&nbsp;${row.baseline}&nbsp;&nbsp;</td>
            <#if (row.ratio)?starts_with("-")>
              <td style="${reportEmailCssSpec.metricCellNegativeStyle}">&nbsp;&nbsp;${row.ratio}%&nbsp;&nbsp;</td>
            <#else>
              <td style="${reportEmailCssSpec.metricCellPositiveStyle}">&nbsp;&nbsp;${row.ratio}%&nbsp;&nbsp;</td>
            </#if>
          </#list>
        </#if>
      </tr>
      </#list>
    </tbody>
  </table>

  <table cellspacing="0" cellpadding="0" style="${reportEmailCssSpec.tableStyle1}">
    <tbody><tr><td>
  <p style="${reportEmailCssSpec.cellStyle1}"><a href=${table.thirdeyeURI} target="_blank">View in Thirdeye</a></p>
  </td></tr></tbody></table>
  <br>
</#list>

<#if missingSegments?has_content>
<table cellspacing="0" cellpadding="0" style="${reportEmailCssSpec.tableStyle1}">
    <tbody>
      <tr><td style="${reportEmailCssSpec.cellStyle1}">*The following data segments are missing from the thirdeye-server</td></tr>
      <#list missingSegments as segment>
        <tr><td style="${reportEmailCssSpec.cellStyle4}">${segment}</td></tr>
      </#list>
    </tbody>
</table>
</#if>

<#if (anomalyTables)??>
<br>
<table cellpadding="0" cellspacing="0">
 <tbody><tr height="23" style="height:22.5pt">
<td colspan="1" style="font-family:tahoma;font-size:14.0pt">Anomaly Reports</td></tr>
<tr style="height:10.5pt"><td></td></tr>
</tbody></table>
<#include "thirdeye-anomaly-template.ftl">
</#if>

</body>
</html>