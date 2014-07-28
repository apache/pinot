namespace java com.linkedin.pinot.request

/**
 * Filter Operator
 **/
enum FilterOperator {
  AND,
  OR,
  EQUALITY,
  NOT,
  RANGE,
  REGEX
}

/**
 *  Query type
 **/
struct QueryType {
  1: optional bool hasSelection;
  2: optional bool hasFilter;
  3: optional bool hasAggregation;
  4: optional bool hasGroup_by;
}

/**
 * Query source
 **/
struct QuerySource {
  1: optional string resourceName;
  2: optional string tableName;
}

/**
 * Filter query
 **/
struct FilterQuery {
  1: optional string column;
  2: list<string> value;
  3: optional FilterOperator operator;
  4: list<i32> nestedFilterQueryIds;
}

/**
 * Filter Query is nested but thrift stable version does not support yet (The support is there in top of the trunk but no released jars. Two concerns : stability and onus of maintaining a stable point. Also, its pretty difficult to compile thrift in Linkedin software development environment which is not geared towards c++ dev. Hence, the )
 **/
struct FilterQueryMap {
 1: optional map<i32,FilterQuery> filterQueryMap;
}

/**
 *  Aggregation
 **/
struct AggregationInfo {
  1: optional string aggregationType;
  2: optional map<string,string> aggregationParams;
}

/**
 * GroupBy
 **/
struct GroupBy {
  1: optional list<string> columns;
  2: optional i64 topN;
}

/**
 * selection-sort : specifies how the search results should be sorted.
 * The results can be sorted based on one or multiple columns
 **/
struct SelectionSort {
  1: optional string column;
  2: optional bool isAsc;
}

/**
 * Selection
 **/
struct Selection {
  1: optional list<string> selectionColumns;
  2: optional list<SelectionSort> selectionSortSequence;
  3: optional i32 offset;
  4: optional i32 size;
}

/**
 * Broker Query
 **/
struct BrokerRequest {
  1: optional QueryType queryType;
  2: optional QuerySource querySource;
  3: optional string timeInterval;
  4: optional string duration;
  5: optional FilterQuery filterQuery;
  6: optional list<AggregationInfo> aggregationsInfo;
  7: optional GroupBy groupBy;
  8: optional Selection selections;
  9: optional FilterQueryMap filterSubQueryMap;
}

/**
 * Request
 **/
struct InstanceRequest {
  1: required i64 requestId;
  2: required BrokerRequest query;
  3: optional list<i64> searchPartitions;
}
