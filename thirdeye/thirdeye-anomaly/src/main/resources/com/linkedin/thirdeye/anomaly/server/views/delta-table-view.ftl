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
  <h1>Delta Table (RuleBased)</h1>
  <b>Database:</b> ${database} <br>
  <b>Collection:</b> ${collection} <br>
  <br>
</center>

<p>
  Submitting the same dimension combination will overwrite the existing entry.
  Note: "?" denotes "any" but does not count toward the degree of a match.
</p>

<form action="${responseUrl}" method="post">
  <table style="width:100%">
    <tr>
      <#list columnNames as columnName>
        <th>${columnName}</th>
      </#list>
    </tr>
    <#list rows as row>
      <tr>
        <#list row as value>
          <td>${value}</td>
        </#list>
      </tr>
    </#list>
    <tr>
      <#list columnNames as columnName>
        <td>
          <#if columnName == "delta">
            <input type="number" name="${columnName}" step="0.01" style="width:100%" required>
          <#else>
            <input type="text" name="${columnName}" value="?" style="width:100%" required>
          </#if>
        </td>
      </#list>
    <tr>
  </table>
  <input type="submit" value="Submit">
</form>

</body>
</html>