Partition Aware Query Routing
=============================

In ongoing efforts to support use cases with low latency high throughput requirements, we have started to notice scaling issues in Pinot broker where adding more replica sets does not improve throughput as expected beyond a certain point. One of the reason behind this is the fact that the broker does not perform any pruning of segments, and so every query ends up processing each segment in the data set. This processing of potentially unnecessary segments has been shown to impact throughput adversely.

Details
-------

The Pinot broker component is responsible for receiving queries, fanning them out to Pinot servers, merging the responses  from servers and finally sending the merged responses back to the client. The broker maintains multiple randomly generated routing tables that map each server to a subset of segments, such that one routing table covers one replica set (across various servers). This implies that for each query all segments of a replica set are processed for a server.

This becomes an overhead when the answer for the given query lies within a small subset of segments. One very common example is when queries have a narrow time filter (say 30 days), but the retention is two years (730 segments, at the rate of one segment per day). For each such query there are multiple overheads:

	Broker needs to use connections to servers that may not even be hosting any segments worth processing.
	On the server side, there is query planning as well as execution once per segment. This happens for hundreds (or even few thousands) of segments, when only a few need to be actually processed.
	
We observed through experiments as well as prototyping that while these overheads may (or may not) impact latency, they certainly impact throughput quite a bit. In an experiment with one SSD node with 500 GB of data (720 segments), we observed a baseline QPS of 150 without any pruning and pruning on broker side improves QPS to about 1500.

Proposed Solution
-----------------

We propose a solution that would allow the broker to quickly prune segments for a given query, reducing the overheads and improving throughput. The idea is to make information available in the segment metadata for broker to be able to quickly prune a segment for a given query. Once the broker has compiled the query, it has the filter query tree that represents the predicates in the query. If there are predicates on column(s) for which there is partition/range information in the metadata, the broker would be able to quickly prune segments that would not satisfy the predicates.


In our design, we propose two new components within the broker:

  * **Segment ZK Metadata Manager**: This component will be responsible for caching the segment ZK metadata in memory within the broker. Its cache will be kept upto date by listening to external view changes.
  * **Segment Pruner**: This component will be responsible pruning segments for a given query. Segments will be pruned based on the segment metadata and the predicates in the query.

Segment ZK Metadata Manager
---------------------------

While the broker does not have access to the segments themselves, it does have access to the ZK metadata for segments. The SegmentZKMetadataManager will be responsible for fetching and caching this metadata for each segment.
Upon transition to online state, the SegmentZKMetadataManager will go over the segments of the table(s) it hosts and build its cache.
It will use the existing External View change listener to update its cache. Since External View does not change when segments are refreshed, and setting watches for each segment in the table is too expensive, we are choosing to live with a limitation where this feature does not work for refresh usecase. This limitation can be revisited at a later time, when inexpensive solutions are available to detect segment level changes for refresh usecases.

Table Level Partition Metadata
------------------------------

We will introduce a table level config to control enabling/disabling this feature for a table. This config can actually be the pruner class name, so that realtime segment generation can pick it up. Absence or incorrect specification of this table config would imply the feature is disabled for the table.

Segment Level Partition Metadata
--------------------------------

The partition metadata would be a list of tuples, one for each partition column, where each tuple contains:
Partition column: Column on which the data is partitioned.
Partition value: A min-max range with [start, end]. For single value start == end.
Prune function: Name of the class that will be used by the broker to prune a segment based on the predicate(s) in the query. It will take as argument the predicate value and the partition value, and return true if segment can be pruned, and false otherwise.

For example, let us consider a case where the data is naturally partitioned on time column ‘daysSinceEpoch’. The segment zk metadata will have information like below:

::

  {
    “partitionColumn”	: “daysSinceEpoch”,
  	“partitionStart”	: “17200”,
     “partitionEnd”		: “17220”,
	 “pruneFunction”		: “TimePruner”
  }

Now consider the following query comes in. 

::

  Select count(*) from myTable where daysSinceEpoch between 17100 and 17110

The broker will recognize the range predicate on the partition column, and call the TimePruner which will identify that the predicate cannot be satisfied and hence and return true, thus pruning the segment. If there is no segment metadata or there is no range predicate on partition column, the segment will not be pruned (return false).

Let’s consider another example where the data is partitioned by memberId, where a hash function was applied on the memberId to compute a partition number.

::

  {
    “partitionColumn”	: “memberId”,
  	“partitionStart”	: “10”,
    “partitionEnd”		: “10”,
	“pruneFunction”		: “HashPartitionPruner”
  }

  Select count(*) from myTable where memberId = 1000`

In this case, the HashPartitionPruner will compute the partition id for the  memberId (1000) in the query. And if it turns out to anything other than 10, the segment would be pruned out. The prune function would contain the complete logic (and information) to be able to compute partition id’s from meberId’s.

Segment Pruner
--------------

The broker will perform segment pruning as follows. This is not an exact algorithm but meant for conveying the idea. Actual implementation might differ slightly (if needed).

#. Broker will compile the query and generate filter query tree as it does currently.
#. The broker will perform a DFS on the filter query tree  and prune the segment as follows:

	* If the current node is leaf and is not the partition column, return false (not prune).
	* If the current node is leaf and is the partition column, return the result of calling prune function with predicate value from leaf node, and partition value from metadata. 
	* If the current node is AND, return true as long as one of its children returned true (prune).
	* If the current node is OR, return true if all of its children returned true (prune).

Segment Generation
------------------

The segment generation code would be enhanced as follows:
It already auto-detects sorted columns, but only writes out the min-max range for time column to metadata. It will be enhanced to write out the min-max range for all sorted columns in the metadata.

For columns with custom partitioning schemes, the name of partitioning (pruner) class will be specified in the segment generation config. Segment generator will ensure that the column values adhere to partitioning scheme and then will write out the partition information into the metadata. In case column values do not adhere to partition scheme, it will log a warning and will not write partition information in the metadata. The impact of this will be that broker would not able to perform any pruning on this segment.

This will apply to both offline as well as realtime segment generation, except that for realtime the pruner class name is specified in the table config. 
When the creation/annotation of segment ZK metadata from segment metadata happens in controller (when adding a new segment) the partition info will also be copied over.

Backward compatibility
----------------------

This feature will be ensured to not create any  backward compatibility issues.
New code with old segments will not find any partition information and pruning will be skipped.
Old code will not look for the new information in segment Zk metadata and will work as expected.

Query response impact
---------------------

The total number of documents in the table is returned as part of the query response. This is computed by servers when they process segments. If some segments are pruned, their documents will not be accounted for in the query response.

To address this, we will enhance the Segment ZK metadata to also store the document count of the segment, which the broker has access to. The total document count will then be computed on the broker side instead.

Partitioning of existing data
-----------------------------

In the scope of this project, existing segments would not be partitioned. This simply means that pruning would not apply to those segments. If there’s a existing usecase that would benefit from this, then there are a few possibilities that can be explored (outside the scope of this project):

Data can be re-bootstrapped after partitioning
----------------------------------------------

If the existing segments already comply to some partitioning, a utility can be created to to update the segment metadata and re-push the segments.

Results
-------

With Partition aware segment assignment and query routing, we were able to demonstrate 6k qps with 99th %ile latency under 100ms, on a data size of 3TB, in production.

Limitations
-----------

The initial implementation will have the following limitations:
It will not work for refresh usecases because currently there’s no cheap way to detect segment ZK metadata change for segment refresh. The only available way is to set watches at segment level that will be too expensive.
Segment generation will not partition the data itself, but assume and assert that input data is partitioned as specified in the config.
