..
.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at
..
..   http://www.apache.org/licenses/LICENSE-2.0
..
.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.
..

.. warning::  The documentation is not up-to-date and has moved to `Apache Pinot Docs <https://docs.pinot.apache.org/>`_.

Executing queries via REST API on the Broker
============================================

The Pinot REST API can be accessed by invoking ``POST`` operation with a JSON body containing the parameter ``pql``
to the ``/query`` URI endpoint on a broker. Depending on the type of query, the results can take different shapes.
The examples below use curl.

Aggregation
-----------

.. code-block:: none

  curl -X POST -d '{"pql":"select count(*) from flights"}' http://localhost:8099/query


  {
   "traceInfo":{},
   "numDocsScanned":17,
   "aggregationResults":[
      {
         "function":"count_star",
         "value":"17"
      }
   ],
   "timeUsedMs":27,
   "segmentStatistics":[],
   "exceptions":[],
   "totalDocs":17
  }


Aggregation with grouping
-------------------------

.. code-block:: none

  curl -X POST -d '{"pql":"select count(*) from flights group by Carrier"}' http://localhost:8099/query


  {
   "traceInfo":{},
   "numDocsScanned":23,
   "aggregationResults":[
      {
         "groupByResult":[
            {
               "value":"10",
               "group":["AA"]
            },
            {
               "value":"9",
               "group":["VX"]
            },
            {
               "value":"4",
               "group":["WN"]
            }
         ],
         "function":"count_star",
         "groupByColumns":["Carrier"]
      }
   ],
   "timeUsedMs":47,
   "segmentStatistics":[],
   "exceptions":[],
   "totalDocs":23
  }


Selection
---------

.. code-block:: none

  curl -X POST -d '{"pql":"select * from flights limit 3"}' http://localhost:8099/query


  {
   "selectionResults":{
      "columns":[
         "Cancelled",
         "Carrier",
         "DaysSinceEpoch",
         "Delayed",
         "Dest",
         "DivAirports",
         "Diverted",
         "Month",
         "Origin",
         "Year"
      ],
      "results":[
         [
            "0",
            "AA",
            "16130",
            "0",
            "SFO",
            [],
            "0",
            "3",
            "LAX",
            "2014"
         ],
         [
            "0",
            "AA",
            "16130",
            "0",
            "LAX",
            [],
            "0",
            "3",
            "SFO",
            "2014"
         ],
         [
            "0",
            "AA",
            "16130",
            "0",
            "SFO",
            [],
            "0",
            "3",
            "LAX",
            "2014"
         ]
      ]
   },
   "traceInfo":{},
   "numDocsScanned":3,
   "aggregationResults":[],
   "timeUsedMs":10,
   "segmentStatistics":[],
   "exceptions":[],
   "totalDocs":102
  }


.. _java-client:

Executing queries via Java Client API
=====================================

The Pinot client API is similar to JDBC, although there are some differences, due to how Pinot behaves. For example, a query with multiple aggregation function will return one result set per aggregation function, as they are computed in parallel.

Connections to Pinot are created using the ConnectionFactory class' utility methods to create connections to a Pinot cluster given a Zookeeper URL, a Java Properties object or a list of broker addresses to connect to.

.. code-block:: java

   Connection connection = ConnectionFactory.fromZookeeper
     ("some-zookeeper-server:2191/zookeeperPath");

   Connection connection = ConnectionFactory.fromProperties("demo.properties");

   Connection connection = ConnectionFactory.fromHostList
     ("some-server:1234", "some-other-server:1234", ...);


Queries can be sent directly to the Pinot cluster using the Connection.execute(java.lang.String) and Connection.executeAsync(java.lang.String) methods of Connection.

.. code-block:: java

   ResultSetGroup resultSetGroup = connection.execute("select * from foo...");
   Future<ResultSetGroup> futureResultSetGroup = connection.executeAsync
     ("select * from foo...");


Queries can also use a PreparedStatement to escape query parameters:

.. code-block:: java

   PreparedStatement statement = connection.prepareStatement
     ("select * from foo where a = ?");
   statement.setString(1, "bar");

   ResultSetGroup resultSetGroup = statement.execute();
   Future<ResultSetGroup> futureResultSetGroup = statement.executeAsync();


In the case of a selection query, results can be obtained with the various get methods in the first ResultSet, obtained through the getResultSet(int) method:

.. code-block:: java

   ResultSet resultSet = connection.execute
     ("select foo, bar from baz where quux = 'quuux'").getResultSet(0);

   for (int i = 0; i < resultSet.getRowCount(); ++i) {
     System.out.println("foo: " + resultSet.getString(i, 0));
     System.out.println("bar: " + resultSet.getInt(i, 1));
   }

   resultSet.close();


In the case of aggregation, each aggregation function is within its own ResultSet:

.. code-block:: java

   ResultSetGroup resultSetGroup = connection.execute("select count(*) from foo");

   ResultSet resultSet = resultSetGroup.getResultSet(0);
   System.out.println("Number of records: " + resultSet.getInt(0));
   resultSet.close();


There can be more than one ResultSet, each of which can contain multiple results grouped by a group key.

.. code-block:: java

 ResultSetGroup resultSetGroup = connection.execute
     ("select min(foo), max(foo) from bar group by baz");

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

 resultSet.close();

