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
  <h1>Add Function (Generic)</h1>
  <b>Database:</b> ${database} <br>
  <b>Table:</b> ${functionTable} <br>
  <b>Collection:</b> ${collection} <br>
  <br>
</center>

<form action="${responseUrl}" method="post">
  <table style="width:100%">
    <col width="125">
    <tr>
      <td>Name</td>
      <td>
        <input type="text" name="Name" style="width:100%">
      </td>
    </tr>
    <tr>
      <td>Description</td>
      <td>
        <input type="text" name="Description" style="width:100%">
      </td>
    </tr>
    <tr>
      <td>JarUrl</td>
      <td>
        <input type="text" name="JarUrl" style="width:100%">
      </td>
    </tr>
    <tr>
      <td>ClassName</td>
      <td>
        <input type="text" name="ClassName" style="width:100%">
      </td>
    </tr>
    <tr>
      <td>Properties</td>
      <td>
        <textarea type="text" name="Properties" style="width:100%" rows="20"></textarea>
      </td>
    </tr>
  </table>
  <input type="submit" value="Submit">
</form>


</body>
</html>
