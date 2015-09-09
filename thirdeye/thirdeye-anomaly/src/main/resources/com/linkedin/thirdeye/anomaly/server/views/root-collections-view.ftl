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

<table align="center">
  <tr>
    <th>collection</th>
    <th>functions</th>
    <th>configuration</th>
  </tr>
  <#list collections as collection>
    <tr>
      <td>${collection}</td>
      <td>
        <a href="/${collection}/functions">functions</a>
      </td>
      <td>
        <a href="/configuration/${collection}">configuration</a>
      </td>
    </tr>
  </#list>
</table>

</body>
</html>