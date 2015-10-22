thirdeye-detector
=================

This module can schedule multiple anomaly detection algorithms, which populate a database with anomaly results.

A basic way to visualize anomalies on a time series chart is provided, as well as the ability to periodically
send email reports containing anomalies.

This project uses Java 1.7 or above.

Build
-----

To build this project, from the root project, execute:

```
   mvn install -pl thirdeye-detector -am -DskipTests
```

This creates an artifact `thirdeye-detector/target/thirdeye-detector-*.jar` which will be referred to as
`thirdeye-detector.jar` in this document.

Configure
---------

See `src/test/resources/sample-detector-server.yml` for a sample server configuration. A configuration
such as this will be referred to as `detector.yml` in this document.

This configuration connects to a `thirdeye-server` instance running on `localhost:10000`, as well as to a
MySQL instance running on `localhost:5075`.

See [Dropwizard Configuration Reference](http://www.dropwizard.io/manual/configuration.html) for more.

Migrate
-------

To set up your database tables, first create the database and thirdeye user, e.g.

```
   mysql -u root -e "CREATE DATABASE thirdeye"
```

```
   mysql -u root -e "CREATE USER 'thirdeye'@'localhost' IDENTIFIED BY 'thirdeye'"
```

```
   mysql -u root -e "GRANT ALL ON thirdeye.* TO 'thirdeye'@'localhost'"
```

Then run the migration script to create the tables:

```
   java -jar thirdeye-detector.jar db migrate detector.yml
```

(The migration script can be found at `src/main/resources/migrations.xml`)

Run
---

To run the server, execute the following command:

```
   java -jar thirdeye-detector.jar server detector.yml
```

If the sample configuration is used, a detector server should be running on `localhost:8080`.

REST Resources
--------------

The following resources are available, with GET, POST, DELETE methods on each:

* `/api/anomaly-results`: CRUD for anomaly result objects
* `/api/email-reports`: Configures and manages the scheduled email reports
* `/api/anomaly-functions`: Configures and manages anomaly functions
* `/api/contextual-events`: CRUD for contextual event objects (e.g. deployment, holiday, etc.)
* `/api/anomaly-jobs`: Start, stops, run ad-hoc anomaly detection jobs

See the `com.linkedin.thirdeye.resources` package for the resource definitions.

On server start-up, a full list of resources and their HTTP methods is displayed in the application log.

Setup Function
--------------

To install an anomaly detection function, one can `POST` to the `/api/anomaly-functions` resource. `DELETE` removes
a function, and `GET` returns the function configuration (or all functions.

Example functions include

* `src/test/resources/kalman1.json`
* `src/test/resources/scan1.json`
* `src/test/resources/scan2.json`
* `src/test/resources/scan3.json`

Consider the `kalman1.json` function:

```
{
  "collection": "myCollection",
  "metric": "myMetric",
  "type": "KALMAN_FILTER",
  "isActive": true,
  "cron": "0 * * * * ?",
  "windowSize": 912,
  "windowUnit": "HOURS",
  "windowDelay": 3,
  "bucketSize": 1,
  "bucketUnit": "HOURS",
  "properties": "seasonal=168"
}
```

The fields are interpreted as follows:

* `collection`: The collection name, i.e. database name, in the `thirdeye-server` backend
* `metric`: The specific metric (one) that is part of the collection
* `type`: The function type (e.g. KALMAN_FILTER, SCAN_STATISTICS, USER_DEFINED)
* `isActive`: If false, function is disabled; otherwise is active
* `cron`: A [cron expression](http://www.quartz-scheduler.org/documentation/quartz-2.x/tutorials/crontrigger) that defines when this function runs
* `windowSize`: The size of the time series that should be passed to the function
* `windowUnit`: The unit of windowSize
* `windowDelay`: The amount of time to subtract from the current time for the window end (default: 0)
* `bucketSize`: The granularity of the points in the time series
* `bucketUnit`: The unit of bucketSize
* `properties`: Arbitrary function-specific Java properties (allows `;` separated to avoid new lines)
* `exploreDimensions`: A CSV list of dimension names to drill down into (via GROUP BY time series query)

To install this function, we can use cURL, or any HTTP library:

```
curl -X POST --data-binary @src/test/resources/kalman1.json -H "content-type: application/json" http://localhost:8080/api/anomaly-functions
```

This will return the function ID, e.g. 1. We can also see all the installed functions via:

```
curl -X GET http://localhost:8080/api/anomaly-functions
```

Run Function
------------

After installing a new function to a live server, one must manually schedule it via the `/api/anomaly-jobs` resource.

For example, to start function 1, (after having performed the previous steps):

```
   curl -X POST http://localhost:8080/api/anomaly-jobs/1
```

(Note: if the `isActive` field is set to true, then it will be automatically started on server start up)

This schedules the function according to its cron expression, so it will not run until the time satisfies that
expression.

It may be desirable to run an ad-hoc instance of the function on an arbitrary time window. To accomplish this,
one can use the `/api/anomaly-jobs/{id}/ad-hoc` resource. This resource also accepts two query parameters:

* `windowEnd`: ISO8601 string which if present, sets the end time of the range (default: now minus windowDelay)
* `windowStart`: ISO8601 string which if present, sets the start time of the range (default: windowEnd minus windowSize)

For example, to run function 1 on three weeks of data:

```
    curl -X POST 'http://localhost:8080/api/anomaly-jobs/1/ad-hoc?windowStart=2015-10-01&windowEnd=2015-10-21'
```

[ISO8601](https://en.wikipedia.org/wiki/ISO_8601) strings are described in detail here.

Sending Emails
--------------

To send periodic emails, one can `POST` a configuration like the following (say, named `email.json`):

```
{
    "isActive": true,
    "collection": "myCollection",
    "cron": "0 0 10 * * ?",
    "fromAddress": "thirdeye-detector@yourcompany.com",
    "metric": "myMetric",
    "smtpHost": "email.yourcompany.com",
    "smtpPassword": null,
    "smtpPort": 25,
    "smtpUser": null,
    "toAddresses": "you@yourcompany.com,yourboss@yourcompany.com",
    "windowSize": 1,
    "windowUnit": "DAYS"
}
```

To the `/api/email-reports` resource:

```
curl -X POST --data-binary @email.json http://localhost:8080/api/email-reports
```

After this is done, the email scheduler needs to be reset via:

```
curl -X POST http://localhost:8080/admin/tasks/email?action=reset
```

(TODO: clean up task / api HTTP method semantics... `/api/anomaly-jobs` may be better implemented as a task)

To send an ad-hoc email not on the schedule in the configuration, one can do the following (e.g. for
email report id 1):

```
   curl -X POST http://localhost:8080/api/email-reports/1/ad-hoc
```

Viewing Anomalies
-----------------

An Angular.js application that uses [MetricsGraphics.js](http://metricsgraphicsjs.org/) is available at
the `/` resource, which can be used to plot the time series data annotated with anomalies.

For example, to see anomalies in the month of October for myMetric, which is a part of myCollection, 
one might navigate to the following URL:

```
http://localhost:8080/#/time-series/myCollection/myMetric/2015-10-01/2015-10-31
```

This URL accepts the following query parameters:

* `overlay`: Show a time-shifted time series (e.g. overlay=1w shows time series from previous week on same chart)
* `groupBy`: Shows the GROUP BY time series for a dimension (e.g. groupBy=source)
* `topK`: The number of GROUP BY time series to display (default: 5)
* `functionId`: Filter by a specific anomaly detection function ID
