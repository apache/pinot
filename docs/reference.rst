.. _reference:

Pinot Reference Manual
======================

.. _pql:

PQL
---

* SQL derivative, no support for joins
* Support for selection, projection, aggregation, grouping aggregation
* Examples of these types of queries
* PQL grammar examples
* Things that are specific to Pinot:
 - Grouping keys always appear in query results, even if not requested
 - Aggregations are computed in parallel
 - Results of aggregations with large amounts of group keys (>1M) are approximated
 - ``ORDER BY`` only works for selection queries, for aggregations one must use the ``TOP`` keyword
*
*
*
*
*
*

.. _client-api:

Client API
----------

* There is a REST client API and a Java client API
*

Management REST API
-------------------

Architecture
------------
