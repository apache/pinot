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
  <h1>All Functions (Generic)</h1>
  <b>Database:</b> ${database} <br>
  <b>Table:</b> ${functionTable} <br>
  <b>Collection:</b> ${collection} <br>
  <br>
    <a href="/functions/add">Add a new function.</a>
  <br>
  <br>
    <a href="/functions?hideInactive=true">Hide inactive.</a>
  <br>
  <br>
</center>

<table style="width:100%">
  <tr>
    <th>Action</th>
    <th>FunctionId</th>
    <th>FunctionName</th>
    <th>Description</th>
    <th>JarUrl</th>
    <th>ClassName</th>
    <th>Properties</th>
  </tr>
  <#list functions as row>
    <tr>
      <td>
        <#if (row.active)>
          <a href="/functions/deactivate/${row.functionId}">deactivate</a>
        <#else>
          <a href="/functions/activate/${row.functionId}">activate</a>
        </#if>
        <br>
        <a href="/functions/execute/${row.functionId}">execute</a>
      </td>
      <td>${row.functionId}</td>
      <td>${row.functionName}</td>
      <td>${row.functionDescription}</td>
      <td>${row.jarUrl!""}</td>
      <td>${row.className}</td>
      <td>
        <#if row.functionProperties??>
          ${row.functionProperties?replace("\n", "<br>")}
        </#if>
      </td>
    </tr>
  </#list>
</table>

</body>
</html>
