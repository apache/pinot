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
  <h1>Add Function (RuleBased)</h1>
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
        <select name="Name">
          <option value="percentchange">Percent Change</option>
          <option value="absolutechange">Absolute Change</option>
        </select>
      </td>
      <td>
        The base function to evaluate.
      </td>
    </tr>
    <tr>
      <td>Description</td>
      <td>
        <input type="text" name="Description" style="width:100%" required>
      </td>
      <td>
        A brief summary of the event we are trying to detect. <br>
        (E.g., -10% in ___ for 3 consecutive hours on weekday during daytime.)
      </td>
    </tr>
    <tr>
      <td>Metric</td>
      <td>
        <input type="text" name="Metric" style="width:100%" required>
      </td>
      <td>
        The metric to analyze (e.g., "impressions", or RATIO('x','y')).
      </td>
    </tr>
    <tr>
      <td>Delta</td>
      <td>
        <input type="number" name="Delta" step="0.01" style="width:100%" required>
      </td>
      <td>
        The default threshold to apply.
      </td>
    </tr>
    <tr>
      <td>Aggregate Unit</td>
      <td>
        <select name="AggregateUnit">
          <option value="MINUTES">MINUTES</option>
          <option value="HOURS" selected="selected">HOURS</option>
          <option value="DAYS">DAYS</option>
        </select>
      </td>
      <td>
        The granularity at which the rules will execute (e.g., 1-HOURS will lead to hourly data).
      </td>
    </tr>
    <tr>
      <td>Aggregate Size</td>
      <td>
        <input type="number" name="AggregateSize" style="width:100%" value="1" required>
      </td>
      <td>

      </td>
    </tr>
    <tr>
      <td>Baseline Unit</td>
      <td>
        <select name="BaselineUnit">
          <option value="MINUTES">MINUTES</option>
          <option value="HOURS">HOURS</option>
          <option value="DAYS" selected="selected">DAYS</option>
        </select>
      </td>
      <td>
        The period to compare against. (e.g., 7-DAYS for week-over-week).
      </td>
    </tr>
    <tr>
      <td>Baseline Size</td>
      <td>
        <input type="number" name="BaselineSize" style="width:100%" value="7" required>
      </td>
      <td>

      </td>
    </tr>
    <tr>
      <td>Consecutive Buckets</td>
      <td>
        <input type="number" name="ConsecutiveBuckets" style="width:100%">
      </td>
      <td>
        The number of consecutive buckets containing violations.
      </td>
    </tr>
    <tr>
      <td>Cron Definition</td>
      <td>
        <input type="text" name="CronDefinition" style="width:100%">
      </td>
      <td>
        Restriction for when the function is active.
        <br>

        <b>Format:</b> SECONDS MINUTES HOURS DAYOFMONTH MONTH DAYOFWEEK YEAR
        <br>

        (e.g., "0 15 10 ? * MON-FRI"  to "Fire at 10:15am every weekday")
      </td>
    </tr>
    <tr>
      <td>Delta Table</td>
      <td>
        <input type="text" name="DeltaTable" style="width:100%">
      </td>
      <td>
        Table for storing custom thresholds for dimensions.
      </td>
    </tr>

  </table>
  <input type="submit" value="Submit">
</form>


</body>
</html>
