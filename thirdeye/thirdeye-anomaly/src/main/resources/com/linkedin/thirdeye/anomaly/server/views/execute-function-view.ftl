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
  <br>
</center>

<form action="${responseUrl}" method="post">
  <table style="width:100%">
    <col width="150">
    <tr>
      <td>
        <b>Function Id</b>
      </td>
      <td>
        <b>${functionId}</b>
      </td>
    </tr>
    <tr>
      <td>Start Time (millis)</td>
      <td>
        <input type="number" name="StartTime" step="1000" style="width:100%" required>
      </td>
    </tr>
    <tr>
      <td>End Time (millis)</td>
      <td>
        <input type="number" name="EndTime" step="1000" style="width:100%" required>
      </td>
    </tr>
    <tr>
      <td>
        <b>Dimension</b>
      </td>
      <td>
        <b>Value</b>
      </td>
    </tr>
    <#list dimensionNames as dimensionName>
      <tr>
        <td>${dimensionName}</td>
        <td>
          <input type="text" name="${dimensionName}" value="*" style="width:100%" required>
        </td>
      </tr>
    </#list>
  </table>
  <input type="submit" value="Run">
</form>

<p>Please do not hit run more than once or refresh the page once you have pressed run.</p>

</body>
</html>
