<html>
<head>
  <title><b>${reportConfig.name}</b></title>
</head>
<body>

<br>
<table cellspacing="2" cellpadding="2" style="border-left:1.0pt solid;border-top:1.0pt solid;border-right:1.0pt solid;border-bottom:1.0pt solid;border-left-color:#ffffff;border-top-color:#ffffff;border-right-color:#ffffff;border-bottom-color:#ffffff">
  <tbody>
    <tr>
      <td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;font-weight:700;color:#000080;text-align:left;vertical-align:middle;border-left:.5pt solid #c0c0c0;border-top:none;border-right:none;border-bottom:.5pt solid #c0c0c0;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">Start</td>
      <td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;text-align:right;vertical-align:middle;border-left:.5pt solid #c0c0c0;border-top:none;border-right:none;border-bottom:.5pt solid #c0c0c0;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">${reportConfig.startTimeString}</td>
    <tr>
      <td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;font-weight:700;color:#000080;text-align:left;vertical-align:middle;border-left:.5pt solid #c0c0c0;border-top:none;border-right:none;border-bottom:.5pt solid #c0c0c0;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">End</td>
      <td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;text-align:right;vertical-align:middle;border-left:.5pt solid #c0c0c0;border-top:none;border-right:none;border-bottom:.5pt solid #c0c0c0;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">${reportConfig.endTimeString}</td>
    </tr>
  </tbody>
</table><br><br>

<table cellpadding="0" cellspacing="0">
 <tbody><tr height="23" style="height:22.5pt">
<td colspan="1" style="font-family:tahoma;font-size:14.0pt">${reportConfig.name}</td></tr>
<tr style="height:10.5pt"><td></td></tr>
</tbody></table>


<#list tables as table>

<table cellspacing="2" cellpadding="2" style="border-left:1.0pt solid;border-top:1.0pt solid;border-right:1.0pt solid;border-bottom:1.0pt solid;border-left-color:#ffffff;border-top-color:#ffffff;border-right-color:#ffffff;border-bottom-color:#ffffff">
  <tbody>
    <tr></tr>
    <tr>
      <td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;font-weight:700;color:#000080;text-align:left;vertical-align:middle;border-left:.5pt solid #c0c0c0;border-top:none;border-right:none;border-bottom:.5pt solid #c0c0c0;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">Current</td>
      <td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;text-align:right;vertical-align:middle;border-left:.5pt solid #c0c0c0;border-top:none;border-right:none;border-bottom:.5pt solid #c0c0c0;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">
        ${scheduleSpec.aggregationSize} <#if scheduleSpec.aggregationSize == 1>${scheduleSpec.aggregationUnit?substring(0,scheduleSpec.aggregationUnit?last_index_of("S"))}<#else>${scheduleSpec.aggregationUnit}</#if>
      </td>
    <tr>
      <td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;font-weight:700;color:#000080;text-align:left;vertical-align:middle;border-left:.5pt solid #c0c0c0;border-top:none;border-right:none;border-bottom:.5pt solid #c0c0c0;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">Baseline</td>
      <td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;text-align:right;vertical-align:middle;border-left:.5pt solid #c0c0c0;border-top:none;border-right:none;border-bottom:.5pt solid #c0c0c0;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">
        <#if table.tableSpec.baselineUnit == "DAYS" && table.tableSpec.baselineSize % 7 == 0>
          ${table.tableSpec.baselineSize / 7} <#if table.tableSpec.baselineSize / 7 == 1>WEEK<#else>WEEKS</#if> ago
        <#else>
          ${table.tableSpec.baselineSize} <#if table.tableSpec.baselineSize == 1>${table.tableSpec.baselineUnit?substring(0,table.tableSpec.baselineUnit?last_index_of("S"))}<#else>${table.tableSpec.baselineUnit}</#if> ago
        </#if>
      </td>
    </tr>
  </tbody>
</table>
  <table cellspacing="0" cellpadding="2" style="border-left:1.0pt solid;border-top:1.0pt solid;border-right:1.0pt solid;border-bottom:1.0pt solid;border-left-color:#c0c0c0;border-top-color:#c0c0c0;border-right-color:#c0c0c0;border-bottom-color:#c0c0c0">
    <tbody>

      <tr>
        <td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;font-weight:700;color:#ffffff;text-align:left;vertical-align:middle;background-color:#dddddd;white-space:normal;border-left:none;border-top:none;border-right:none;border-bottom:none;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">&nbsp;&nbsp;&nbsp;&nbsp;</td>
        <#if (table.tableSpec.groupBy)??><td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;font-weight:700;color:#ffffff;text-align:center;background-color:#dddddd;white-space:normal;border-left:none;border-top:none;border-right:none;border-bottom:none;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">&nbsp;&nbsp;&nbsp;&nbsp;</td></#if>
        <#list table.tableReportRows[0].rows as row>
          <td colspan="3" style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;font-weight:700;color:#000080;text-align:center;background-color:#dddddd;white-space:normal;border-left:1.5pt solid #ffffff;border-top:none;border-right:.5pt solid #ffffff;border-bottom:none;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">&nbsp;&nbsp;${row.metric}&nbsp;&nbsp;</td>
        </#list>
      </tr>

      <tr>
        <td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;font-weight:700;color:#000080;text-align:left;vertical-align:middle;background-color:#c0c0c0;white-space:normal;border-left:.5pt solid #c0c0c0;border-top:none;border-right:.5pt solid #fffff;border-bottom:.5pt solid #c0c0c0;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">&nbsp;&nbsp;Hour&nbsp;&nbsp;</td>
        <#if (table.tableSpec.groupBy)??><td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;font-weight:700;color:#000080;text-align:center;background-color:#c0c0c0;white-space:normal;border-left:1.5pt solid #ffffff;border-top:none;border-right:.5pt solid #c0c0c0;border-bottom:.5pt solid #ffffff;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">&nbsp;&nbsp;${table.tableSpec.groupBy}&nbsp;&nbsp;</td></#if>
        <#list table.tableReportRows[0].rows as row>
          <td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;font-weight:700;color:#000080;text-align:center;background-color:#c0c0c0;white-space:normal;border-left:1.5pt solid #ffffff;border-top:none;border-right:.5pt solid #c0c0c0;border-bottom:.5pt solid #ffffff;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">&nbsp;&nbsp;Current&nbsp;&nbsp;</td>
          <td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;font-weight:700;color:#000080;text-align:center;background-color:#c0c0c0;white-space:normal;border-left:.5pt solid #c0c0c0;border-top:none;border-right:.5pt solid #c0c0c0;border-bottom:.5pt solid #ffffff;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">&nbsp;&nbsp;Baseline&nbsp;&nbsp;</td>
          <td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;font-weight:700;color:#000080;text-align:center;background-color:#c0c0c0;white-space:normal;border-left:.5pt solid #c0c0c0;border-top:none;border-right:none;border-bottom:.5pt solid #ffffff;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">&nbsp;&nbsp;Ratio&nbsp;&nbsp;</b></td>
        </#list>
      </tr>

      <#list table.tableReportRows as tableReportRow>
      <tr>
        <#if tableReportRow.groupByDimensions.startTime == "Total">
          <td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;font-weight:700;color:#000080;text-align:left;vertical-align:middle;border-left:.5pt solid #c0c0c0;border-top:none;border-right:none;border-bottom:.5pt solid #c0c0c0;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">&nbsp;&nbsp;${tableReportRow.groupByDimensions.startTime}&nbsp;&nbsp;</td>
          <#if (table.tableSpec.groupBy)??><td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;font-weight:700;color:#000080;text-align:right;vertical-align:middle;border-left:.5pt solid #c0c0c0;border-top:none;border-right:none;border-bottom:.5pt solid #c0c0c0;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt"></td></#if>

          <#list tableReportRow.rows as row>
            <td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;font-weight:700;text-align:right;vertical-align:middle;border-left:.5pt solid #c0c0c0;border-top:none;border-right:none;border-bottom:.5pt solid #c0c0c0;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">&nbsp;&nbsp;${row.current}&nbsp;&nbsp;</td>
            <td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;font-weight:700;text-align:right;vertical-align:middle;border-left:.5pt solid #c0c0c0;border-top:none;border-right:none;border-bottom:.5pt solid #c0c0c0;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">&nbsp;&nbsp;${row.baseline}&nbsp;&nbsp;</td>
            <#if (row.ratio)?matches('-[0-9]*')>
              <td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;font-weight:700;color:#ff0000;text-align:right;vertical-align:middle;border-left:.5pt solid #c0c0c0;border-top:none;border-right:none;border-bottom:.5pt solid #c0c0c0;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">&nbsp;&nbsp;${row.ratio}%&nbsp;&nbsp;</td>
            <#else>
              <td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;font-weight:700;text-align:right;vertical-align:middle;border-left:.5pt solid #c0c0c0;border-top:none;border-right:none;border-bottom:.5pt solid #c0c0c0;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">&nbsp;&nbsp;${row.ratio}%&nbsp;&nbsp;</td>
            </#if>
          </#list>
        <#else>
          <td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;color:#000080;text-align:left;vertical-align:middle;white-space:normal;border-left:.5pt solid #c0c0c0;border-top:none;border-right:none;border-bottom:.5pt solid #c0c0c0;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt"><span class="aBn" data-term="goog_481112314" tabindex="0"><span class="aQJ">&nbsp;&nbsp;${tableReportRow.groupByDimensions.startTime}&nbsp;&nbsp;&nbsp;&nbsp;</span></span></td>
          <#if (table.tableSpec.groupBy)??><td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;color:#000080;text-align:right;vertical-align:middle;white-space:normal;border-left:.5pt solid #c0c0c0;border-top:none;border-right:none;border-bottom:.5pt solid #c0c0c0;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt"><span class="aBn" data-term="goog_481112314" tabindex="0"><span class="aQJ">&nbsp;&nbsp;&nbsp;&nbsp;${tableReportRow.groupByDimensions.dimension}&nbsp;&nbsp;</span></span></td></#if>

          <#list tableReportRow.rows as row>
            <td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;text-align:right;vertical-align:middle;border-left:.5pt solid #c0c0c0;border-top:none;border-right:none;border-bottom:.5pt solid #c0c0c0;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">&nbsp;&nbsp;${row.current}&nbsp;&nbsp;</td>
            <td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;text-align:right;vertical-align:middle;border-left:.5pt solid #c0c0c0;border-top:none;border-right:none;border-bottom:.5pt solid #c0c0c0;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">&nbsp;&nbsp;${row.baseline}&nbsp;&nbsp;</td>
            <#if (row.ratio)?matches('-[0-9]*')>
              <td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;color:#ff0000;text-align:right;vertical-align:middle;border-left:.5pt solid #c0c0c0;border-top:none;border-right:none;border-bottom:.5pt solid #c0c0c0;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">&nbsp;&nbsp;${row.ratio}%&nbsp;&nbsp;</td>
            <#else>
              <td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;text-align:right;vertical-align:middle;border-left:.5pt solid #c0c0c0;border-top:none;border-right:none;border-bottom:.5pt solid #c0c0c0;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">&nbsp;&nbsp;${row.ratio}%&nbsp;&nbsp;</td>
            </#if>
          </#list>
        </#if>
      </tr>
      </#list>
    </tbody>
  </table>

  <table cellspacing="0" cellpadding="0" style="border-left:1.0pt solid;border-top:1.0pt solid;border-right:1.0pt solid;border-bottom:1.0pt solid;border-left-color:#ffffff;border-top-color:#ffffff;border-right-color:#ffffff;border-bottom-color:#ffffff">
    <tbody><tr><td>
  <p style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;text-align:right;vertical-align:middle;border-left:.5pt solid #c0c0c0;border-top:none;border-right:none;border-bottom:.5pt solid #c0c0c0;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt"><a href=${table.thirdeyeURI} target="_blank">View in Thirdeye</a></p>
  </td></tr></tbody></table>
  <br>
</#list>

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