<!DOCTYPE html>
<html>

<#assign metrics = anomalyTables?keys>

<#list metrics as metric>
<#assign anomalytable = anomalyTables[metric]>
<#if anomalytable.totalViolationCount gt 0 >
<b><p style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;font-weight:700;text-align:left;vertical-align:middle;border-left:.5pt solid #c0c0c0;border-top:none;border-right:none;border-bottom:.5pt solid #c0c0c0;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">${metric}</p></b>
  <table cellspacing="2" cellpadding="4" style="border-left:1.0pt solid;border-top:1.0pt solid;border-right:1.0pt solid;border-bottom:1.0pt solid;border-left-color:#c0c0c0;border-top-color:#c0c0c0;border-right-color:#c0c0c0;border-bottom-color:#c0c0c0">
    <tbody>
      <tr>
        <td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;font-weight:700;color:#000080;text-align:left;vertical-align:middle;background-color:#c0c0c0;white-space:normal;border-left:.5pt solid #c0c0c0;border-top:none;border-right:.5pt solid #fffff;border-bottom:.5pt solid #c0c0c0;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">Date&nbsp;&nbsp;</td>
        <td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;font-weight:700;color:#000080;text-align:left;vertical-align:middle;background-color:#c0c0c0;white-space:normal;border-left:.5pt solid #c0c0c0;border-top:none;border-right:.5pt solid #fffff;border-bottom:.5pt solid #c0c0c0;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">&nbsp;&nbsp;Dimensions&nbsp;&nbsp;</td>
        <td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;font-weight:700;color:#000080;text-align:left;vertical-align:middle;background-color:#c0c0c0;white-space:normal;border-left:.5pt solid #c0c0c0;border-top:none;border-right:.5pt solid #fffff;border-bottom:.5pt solid #c0c0c0;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">&nbsp;&nbsp;Description&nbsp;&nbsp;</td>
        <td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;font-weight:700;color:#000080;text-align:left;vertical-align:middle;background-color:#c0c0c0;white-space:normal;border-left:.5pt solid #c0c0c0;border-top:none;border-right:.5pt solid #fffff;border-bottom:.5pt solid #c0c0c0;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">&nbsp;&nbsp;Score&nbsp;&nbsp;</td>
        <td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;font-weight:700;color:#000080;text-align:left;vertical-align:middle;background-color:#c0c0c0;white-space:normal;border-left:.5pt solid #c0c0c0;border-top:none;border-right:.5pt solid #fffff;border-bottom:.5pt solid #c0c0c0;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">&nbsp;&nbsp;Volume&nbsp;&nbsp;</td>
      </tr>

      <#list anomalytable.reportRows as row>
        <tr>
            <td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;color:#000080;text-align:left;vertical-align:middle;white-space:normal;border-left:.5pt solid #c0c0c0;border-top:none;border-right:none;border-bottom:.5pt solid #c0c0c0;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">${row.date}&nbsp;&nbsp;</td>
            <td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;color:#000080;text-align:left;vertical-align:middle;white-space:normal;border-left:.5pt solid #c0c0c0;border-top:none;border-right:none;border-bottom:.5pt solid #c0c0c0;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">&nbsp;&nbsp;${row.dimensions}&nbsp;&nbsp;&nbsp;&nbsp;</td>
            <td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;text-align:left;vertical-align:middle;border-left:.5pt solid #c0c0c0;border-top:none;border-right:none;border-bottom:.5pt solid #c0c0c0;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">&nbsp;&nbsp;${row.description}&nbsp;&nbsp;</td>
            <td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;text-align:right;vertical-align:middle;border-left:.5pt solid #c0c0c0;border-top:none;border-right:none;border-bottom:.5pt solid #c0c0c0;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">&nbsp;&nbsp;${row.anomalyScore}<#if row.scoreIsPercent == true>%&nbsp;&nbsp;</#if></td>
            <td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;text-align:right;vertical-align:middle;border-left:.5pt solid #c0c0c0;border-top:none;border-right:none;border-bottom:.5pt solid #c0c0c0;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">&nbsp;&nbsp;${row.anomalyVolume}&nbsp;&nbsp;</td>
        </tr>
      </#list>
  </table>

  <#if (anomalytable.dimensionSchema)??>
  <table cellspacing="0" cellpadding="0" style="border-left:1.0pt solid;border-top:1.0pt solid;border-right:1.0pt solid;border-bottom:1.0pt solid;border-left-color:#ffffff;border-top-color:#ffffff;border-right-color:#ffffff;border-bottom-color:#ffffff">
    <tbody><tr><td>
    <p style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;text-align:right;vertical-align:middle;white-space:normal;border-left:.5pt solid #c0c0c0;border-top:none;border-right:none;border-bottom:.5pt solid #c0c0c0;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">*Using dimensions: ${anomalytable.dimensionSchema}</p>
  </td></tr></tbody></table>
  </#if>
  <table cellspacing="2" cellpadding="2" style="border-left:1.0pt solid;border-top:1.0pt solid;border-right:1.0pt solid;border-bottom:1.0pt solid;border-left-color:#ffffff;border-top-color:#ffffff;border-right-color:#ffffff;border-bottom-color:#ffffff">
    <tbody>
      <tr>
        <td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;font-weight:700;color:#000080;text-align:left;vertical-align:middle;border-left:.5pt solid #c0c0c0;border-top:none;border-right:none;border-bottom:.5pt solid #c0c0c0;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt"><b>Number of (top-level) violations</b></td>
        <td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;text-align:right;vertical-align:middle;border-left:.5pt solid #c0c0c0;border-top:none;border-right:none;border-bottom:.5pt solid #c0c0c0;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">${anomalytable.topLevelViolationCount}</td>
      </tr>
      <tr>
        <td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;font-weight:700;color:#000080;text-align:left;vertical-align:middle;border-left:.5pt solid #c0c0c0;border-top:none;border-right:none;border-bottom:.5pt solid #c0c0c0;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt"><b>Number of violations (total)</b></td>
        <td style="color:windowtext;font-size:10.0pt;font-weight:400;font-style:normal;text-decoration:none;font-family:Arial;text-align:general;vertical-align:bottom;border:none;font-size:8.0pt;text-align:right;vertical-align:middle;border-left:.5pt solid #c0c0c0;border-top:none;border-right:none;border-bottom:.5pt solid #c0c0c0;padding-left:1.0pt;padding-right:1.0pt;padding-top:1.0pt;padding-bottom:1.0pt">${anomalytable.totalViolationCount}</td>
      </tr>
  </table><br><br>
</#if>
</#list>
</html>