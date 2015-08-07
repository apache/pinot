<html>
<head>
  <title><b>${reportConfig.name}</b></title>
  <style type="text/css">
    table tr:nth-child(odd) td{
      background-color: #000000;
    }
    table tr:nth-child(even) td{
      background-color: #000080;
    }
  </style>
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
</table><br>

<table cellpadding="0" cellspacing="0">
 <tbody><tr height="23" style="height:22.5pt">
<td colspan="1" style="font-family:tahoma;font-size:16.0pt">${reportConfig.name}</td></tr>
<tr style="height:10.5pt"><td></td></tr>
</tbody></table>
<br>

<#list tables as table>
  <table cellspacing="0" cellpadding="2" style="border-left:1.0pt solid;border-top:1.0pt solid;border-right:1.0pt solid;border-bottom:1.0pt solid;border-left-color:#c0c0c0;border-top-color:#c0c0c0;border-right-color:#c0c0c0;border-bottom-color:#c0c0c0">
    <tbody>
      <tr>
        <td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;font-weight:700;color:#000080;text-align:left;vertical-align:middle;background-color:#c0c0c0;white-space:normal;border-left:.5pt solid #c0c0c0;border-top:none;border-right:.5pt solid #fffff;border-bottom:.5pt solid #c0c0c0;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">Hour&nbsp;&nbsp;</td>
        <#if (table.tableSpec.groupBy)??><td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;font-weight:700;color:#000080;text-align:center;background-color:#c0c0c0;white-space:normal;border-left:1.5pt solid #ffffff;border-top:none;border-right:.5pt solid #c0c0c0;border-bottom:.5pt solid #ffffff;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">&nbsp;&nbsp;${table.tableSpec.groupBy}&nbsp;&nbsp;</td></#if>

        <#list table.tableReportRows[0].rows as row>
        <td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;font-weight:700;color:#000080;text-align:center;background-color:#c0c0c0;white-space:normal;border-left:1.5pt solid #ffffff;border-top:none;border-right:.5pt solid #c0c0c0;border-bottom:.5pt solid #ffffff;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">&nbsp;&nbsp;${row.metric}&nbsp;&nbsp;</td>
        <td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;font-weight:700;color:#000080;text-align:center;background-color:#c0c0c0;white-space:normal;border-left:.5pt solid #c0c0c0;border-top:none;border-right:.5pt solid #c0c0c0;border-bottom:.5pt solid #ffffff;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">&nbsp;&nbsp;Baseline&nbsp;&nbsp;</td>
        <td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;font-weight:700;color:#000080;text-align:center;background-color:#c0c0c0;white-space:normal;border-left:.5pt solid #c0c0c0;border-top:none;border-right:none;border-bottom:.5pt solid #ffffff;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">&nbsp;&nbsp;Ratio&nbsp;&nbsp;</b></td>
        </#list>
      </tr>

      <#list table.tableReportRows as tableReportRow>
      <tr>
        <#if tableReportRow.groupByDimensions.startTime == "Total">
          <td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;font-weight:700;color:#000080;text-align:left;vertical-align:middle;border-left:.5pt solid #c0c0c0;border-top:none;border-right:none;border-bottom:.5pt solid #c0c0c0;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">${tableReportRow.groupByDimensions.startTime}&nbsp;&nbsp;</td>
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
          <td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;color:#000080;text-align:left;vertical-align:middle;white-space:normal;border-left:.5pt solid #c0c0c0;border-top:none;border-right:none;border-bottom:.5pt solid #c0c0c0;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt"><span class="aBn" data-term="goog_481112314" tabindex="0"><span class="aQJ">${tableReportRow.groupByDimensions.startTime}&nbsp;&nbsp;&nbsp;&nbsp;</span></span></td>
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
<td colspan="1" style="font-family:tahoma;font-size:16.0pt">Anomaly Reports</td></tr>
<tr style="height:10.5pt"><td></td></tr>
</tbody></table>
<#include "thirdeye-anomaly-template.ftl">
</#if>

</body>
</html>