Schema TimeSpec Refactoring
============================

Problems with current schema design
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The pinot schema timespec looks like this:

.. code-block:: none

  {
    "timeFieldSpec":
    {
      "name" : <name of time column>,
      "dataType" : <datatype of time column>,
      "timeFormat" : <format of time column, EPOCH or SIMPLE_DATE_FORMAT:format>,
      "timeUnitSize" : <time column granularity size>,
      "timeType" : <time unit of time column>
    }
  }

We are missing data granularity information in pinot schema.
TimeUnitSize, timeType and timeFormat allow us to define the granularity of the time column, but don’t provide a way for applications to know in what buckets the data granularity is.
Currently, we can only have one time column in the table which is limiting some use cases. We should allow multiple time columns and even allow derived time columns. Derived columns can be useful in performing roll ups or generating star tree aggregate nodes.


Changes
~~~~~~~

We have added a List<DateTimeFieldSpec> _dateTimeFieldSpecs to the pinot schema

.. code-block:: none

  {
    “dateTimeFieldSpec”:
      {
        “name” : <name of the date time column>,
        “dataType” : <datetype of the date time column>,
        “format” : <string for interpreting the datetime column>,
        “granularity” : <string for data granularity buckets>,
        “dateTimeType” : <DateTimeType enum PRIMARY,SECONDARY or DERIVED>
      }
  }

#. name - this if the name of the date time column, similar to the older timeSpec

#. dataType - this is the DataType of the date time column, similar to the older timeSpec

#. format - defines how to interpret the numeric value in the date time column.
<br>Format has to follow the pattern - size:timeunit:timeformat, where size and timeUnit together define the granularity of the time column value.
<br>Size is the integer value of the granularity size.
<br>TimeFormat tells us whether the time column value is expressed in epoch or is a simple date format pattern.
<br>Consider 2 date time values for example 2017/07/01 00:00:00 and 2017/08/29 05:20:00:
1. If the time column value is defined in millisSinceEpoch (1498892400000, 1504009200000), this configuration will be 1:MILLISECONDS:EPOCH
2. If the time column value is defined in 5 minutes since epoch (4996308, 5013364), this configuration will be 5:MINUTES:EPOCH
3. If the time column value is defined in a simple date format of a day (e.g. 20170701, 20170829), this configuration will be 1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd (the pattern can be configured as desired)

#. granularity - defines in what granularity the data is bucketed.
<br>Granularity has to follow pattern- size:timeunit, where size and timeUnit together define the bucket granularity of the data. This is independent of the format, which is purely defining how to interpret the numeric value in the datetime column.
1. if a time column is defined in millisSinceEpoch (format=1:MILLISECONDS:EPOCH), but the data buckets are 5 minutes, the granularity will be 5:MINUTES.
2. if a time column is defined in hoursSinceEpoch (format=1:HOURS:EPOCH), and the data buckets are 1 hours, the granularity will be 1:HOURS

#. dateTimeType - this is an enum of values
1. PRIMARY: The primary date time column. This will be the date time column which keeps the milliseconds value. This will be used as the default time column, in references by pinot code (e.g. retention manager)
2. SECONDARY: The date time columns which are not the primary columns with milliseconds value. These can be date time columns in other granularity, put in by applications for their specific use cases
3. DERIVED: The date time columns which are derived, say using other columns, generated via rollups, etc

Examples:

.. code-block:: none

  “dateTimeFieldSpec”:
  {

    “name” : “Date”,
    “dataType” : “LONG”,
    “format” : “1:HOURS:EPOCH”,
    “granularity” : “1:HOURS”,
    “dateTimeType” : "PRIMARY"

  }

  “dateTimeFieldSpec”:
  {

    “name” : “Date”,
    “dataType” : “LONG”,
    “format” : “1:MILLISECONDS:EPOCH”,
    “granularity” : “5:MINUTES”,
    “dateTimeType” : "PRIMARY"

  }

  “dateTimeFieldSpec”:
  {

    “name” : “Date”,
    “dataType” : “LONG”,
    “format” : “1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd”,
    “granularity” : “1:DAYS”,
    “dateTimeType” : "SECONDARY"

  }

Migration
~~~~~~~~~

Once this change is pushed in, we will migrate all our clients to start populating the new DateTimeFieldSpec, along with the TimeSpec.
<br>We can then go over all older schemas, and fill up the DateTimeFieldSpec referring to the TimeFieldSpec.
<br>We then migrate our clients to start using DateTimeFieldSpec instead of TimeFieldSpec.
<br>At this point, we can deprecate the TimeFieldSpec.
