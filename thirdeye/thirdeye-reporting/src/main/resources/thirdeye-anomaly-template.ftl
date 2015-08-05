<!DOCTYPE html>
<html>

<#list anomalyTables as anomalytable>
  <table border=1>
      <tr>
        <td width=300><b>Date</b></td>
        <td width=400><b>Dimensions</b></td>
        <td width=800><b>Description</b></td>
        <td width=100><b>Score</b></td>
        <td width=100><b>Volume</b></td>
      </tr>

      <#list anomalytable.reportRows as row>
        <tr>
            <td>${row.date}</td>
            <td>${row.dimensions}</td>
            <td>${row.description}</td>
            <td>${row.anomalyScore}<#if row.scoreIsPercent == true>%</#if></td>
            <td>${row.anomalyVolume}</td>
        </tr>
      </#list>
  </table><br>

  <#if (row.dimensionSchema)??>
    Using dimensions: ${row.dimensionSchema}
  </#if><br>

  <table border=1>
      <tr>
        <td width=200><b>Number of (top-level) violations</b></td>
        <td width=50>${anomalytable.topLevelViolationCount}</td>
      </tr>
      <tr>
        <td><b>Number of violations (total)</b></td>
        <td>${anomalytable.totalViolationCount}</td>
      </tr>
  </table><br>
</#list>
</html>