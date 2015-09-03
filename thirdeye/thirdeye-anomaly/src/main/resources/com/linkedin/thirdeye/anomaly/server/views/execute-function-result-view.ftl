<!DOCTYPE html>
<html>
<head>
<style>
table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
}
th, td {
    padding: 5px;
}
</style>
</head>
<body>

<center>
  <h1>Execute Function</h1>
  <b>Database:</b> ${database} <br>
  <b>Table:</b> ${functionTable} <br>
  <b>Collection:</b> ${collection} <br>
  <b>Function:</b> ${functionId} <br>
  <b>FunctionName:</b> ${functionName} <br>
  <b>FunctionDescription:</b> ${functionDescription} <br>
  <br>
</center>

<table style="width:100%">
  <tr>
    <th>timeWindow</th>
    <th>anomalyScore</th>
    <th>anomalyVolume</th>
    <th>properties</th>
  </tr>
  <#list anomalies as anomaly>
    <tr>
      <td>${anomaly.timeWindow?c}</td>
      <td>${anomaly.anomalyScore}</td>
      <td>${anomaly.anomalyVolume}</td>
      <td>${anomaly.propertiesString?replace("\n", "<br>")}</td>
    </tr>
  </#list>
</table>

</body>
</html>
