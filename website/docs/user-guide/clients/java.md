---
title: Java
sidebar_label: java
description: Pinot Java Client
---

import Alert from '@site/src/components/Alert';

The java client can be found in pinot-clients/pinot-java-client. Here's an example of how to use the pinot-java-client to query Pinot.

```java
import org.apache.pinot.client.Connection;
import org.apache.pinot.client.ConnectionFactory;
import org.apache.pinot.client.Request;
import org.apache.pinot.client.ResultSetGroup;
import org.apache.pinot.client.ResultSet;

/**
 * Demonstrates the use of the pinot-client to query Pinot from Java
 */
public class PinotClientExample {

  public static void main(String[] args) {

    // pinot connection
    String zkUrl = "localhost:2181";
    String pinotClusterName = "PinotCluster";
    Connection pinotConnection = ConnectionFactory.fromZookeeper(zkUrl + "/" + pinotClusterName);
  
    String query = "SELECT COUNT(*) FROM myTable GROUP BY foo";

    // set queryType=sql for querying the sql endpoint
    Request pinotClientRequest = new Request("sql", query);
    ResultSetGroup pinotResultSetGroup = pinotConnection.execute(pinotClientRequest);
    ResultSet resultTableResultSet = pinotResultSetGroup.getResultSet(0);

    int numRows = resultTableResultSet.getRowCount();
    int numColumns = resultTableResultSet.getColumnCount();
    String columnValue = resultTableResultSet.getString(0, 1);
    String columnName = resultTableResultSet.getColumnName(1);

    System.out.println("ColumnName: " + columnName + ", ColumnValue: " + columnValue);
  }
}
```

Connections to Pinot are created using the ConnectionFactory class' utility methods to create connections to a Pinot cluster given a Zookeeper URL, a Java Properties object or a list of broker addresses to connect to.

```java
Connection connection = ConnectionFactory.fromZookeeper
  ("some-zookeeper-server:2191/zookeeperPath");

Connection connection = ConnectionFactory.fromProperties("demo.properties");

Connection connection = ConnectionFactory.fromHostList
  ("some-server:1234", "some-other-server:1234", ...);
```

Queries can be sent directly to the Pinot cluster using the `Connection.execute(org.apache.pinot.client.Request)` and `Connection.executeAsync(org.apache.pinot.client.Request)` methods of Connection:

```java
ResultSetGroup resultSetGroup =
  connection.execute(new Request("sql", "select * from foo..."));
// OR
Future<ResultSetGroup> futureResultSetGroup =
  connection.executeAsync(new Request("sql", "select * from foo..."));
```

Queries can also use a `PreparedStatement` to escape query parameters:

```java
PreparedStatement statement =
    connection.prepareStatement(new Request("sql", "select * from foo where a = ?"));
statement.setString(1, "bar");

ResultSetGroup resultSetGroup = statement.execute();
// OR
Future<ResultSetGroup> futureResultSetGroup = statement.executeAsync();
```

Results can be obtained with the various get methods in the first ResultSet, obtained through the getResultSet(int) method:

```java
Request request = new Request("sql", "select foo, bar from baz where quux = 'quuux'");
ResultSetGroup resultSetGroup = connection.execute(request);
ResultSet resultTableResultSet = pinotResultSetGroup.getResultSet(0);

for (int i = 0; i < resultSet.getRowCount(); ++i) {
  System.out.println("foo: " + resultSet.getString(i, 0));
  System.out.println("bar: " + resultSet.getInt(i, 1));
}
```

<Alert type="info"> The examples for the sections below this note, are for querying the PQL endpoint, which is deprecated and will be deleted soon. For more information about the 2 endpoints, visit [Querying Pinot](../../user-guide/query-pinot.md).</Alert>

If queryFormat pql is used in the Request,  there are some differences in how the results can be accessed, depending on the query.
In the case of aggregation, each aggregation function is within its own ResultSet. A query with multiple aggregation function will return one result set per aggregation function, as they are computed in parallel.

```java
ResultSetGroup resultSetGroup =
    connection.execute(new Request("pql", "select max(foo), min(foo) from bar"));

System.out.println("Number of result groups:" +
    resultSetGroup.getResultSetCount(); // 2, min(foo) and max(foo)
ResultSet resultSetMax = resultSetGroup.getResultSet(0);
System.out.println("Max foo: " + resultSetMax.getInt(0));
ResultSet resultSetMin = resultSetGroup.getResultSet(1);
System.out.println("Min foo: " + resultSetMin.getInt(0));
```

In case of aggregation group by, there will be as many ResultSets as the number of aggregations, each of which will contain multiple results grouped by a group key.

```java
ResultSetGroup resultSetGroup =
    connection.execute(
        new Request("pql", "select min(foo), max(foo) from bar group by baz"));

System.out.println("Number of result groups:" +
    resultSetGroup.getResultSetCount(); // 2, min(foo) and max(foo)

ResultSet minResultSet = resultSetGroup.getResultSet(0);
for(int i = 0; i < minResultSet.length(); ++i) {
    System.out.println("Minimum foo for " + minResultSet.getGroupKeyString(i, 1) +
        ": " + minResultSet.getInt(i));
}

ResultSet maxResultSet = resultSetGroup.getResultSet(1);
for(int i = 0; i < maxResultSet.length(); ++i) {
    System.out.println("Maximum foo for " + maxResultSet.getGroupKeyString(i, 1) +
        ": " + maxResultSet.getInt(i));
}
```
