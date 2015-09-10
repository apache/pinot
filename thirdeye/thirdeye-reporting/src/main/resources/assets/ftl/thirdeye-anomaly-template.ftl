<!DOCTYPE html>
<html>

<#assign metrics = anomalyTables?keys>

<#list metrics as metric>
<#assign anomalytable = anomalyTables[metric]>
<#if anomalytable.totalViolationCount gt 0 >
<b><p style="${reportEmailCssSpec.anomalyMetric}">${metric}</p></b>
  <table cellspacing="2" cellpadding="4" style="${reportEmailCssSpec.tableStyle2}">
    <tbody>
      <tr>
        <td style="${reportEmailCssSpec.anomalyTitleStyle}">Date&nbsp;&nbsp;</td>
        <td style="${reportEmailCssSpec.anomalyTitleStyle}">&nbsp;&nbsp;Dimensions&nbsp;&nbsp;</td>
        <td style="${reportEmailCssSpec.anomalyTitleStyle}">&nbsp;&nbsp;Description&nbsp;&nbsp;</td>
        <td style="${reportEmailCssSpec.anomalyTitleStyle}">&nbsp;&nbsp;Score&nbsp;&nbsp;</td>
        <td style="${reportEmailCssSpec.anomalyTitleStyle}">&nbsp;&nbsp;Volume&nbsp;&nbsp;</td>
      </tr>

      <#list anomalytable.reportRows as row>
        <tr>
            <td style="${reportEmailCssSpec.anomalyCell1}">${row.date}&nbsp;&nbsp;</td>
            <td style="${reportEmailCssSpec.anomalyCell1}">&nbsp;&nbsp;${row.dimensions}&nbsp;&nbsp;&nbsp;&nbsp;</td>
            <td style="${reportEmailCssSpec.anomalyCell2}">&nbsp;&nbsp;${row.description}&nbsp;&nbsp;</td>
            <td style="${reportEmailCssSpec.anomalyCell3}">&nbsp;&nbsp;${row.anomalyScore}<#if row.scoreIsPercent == true>%&nbsp;&nbsp;</#if></td>
            <td style="${reportEmailCssSpec.anomalyCell3}">&nbsp;&nbsp;${row.anomalyVolume}&nbsp;&nbsp;</td>
        </tr>
      </#list>
  </table>

  <#if (anomalytable.dimensionSchema)??>
  <table cellspacing="0" cellpadding="0" style="${reportEmailCssSpec.tableStyle1}">
    <tbody><tr><td>
    <p style="${reportEmailCssSpec.anomalyDimensionSchema}">*Using dimensions: ${anomalytable.dimensionSchema}</p>
  </td></tr></tbody></table>
  </#if>
  <table cellspacing="2" cellpadding="2" style="${reportEmailCssSpec.tableStyle1}">
    <tbody>
      <tr>
        <td style="${reportEmailCssSpec.anomalyViolations}"><b>Number of (top-level) violations</b></td>
        <td style="${reportEmailCssSpec.anomalyViolationsCount}">${anomalytable.topLevelViolationCount}</td>
      </tr>
      <tr>
        <td style="${reportEmailCssSpec.anomalyViolations}"><b>Number of violations (total)</b></td>
        <td style="${reportEmailCssSpec.anomalyViolationsCount}">${anomalytable.totalViolationCount}</td>
      </tr>
  </table><br><br>
</#if>
</#list>
</html>