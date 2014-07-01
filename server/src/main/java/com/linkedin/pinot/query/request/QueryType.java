package com.linkedin.pinot.query.request;

/**
 * Query Type, can be either given by query or derived from query.
 * 
 * @author Xiang Fu <xiafu@linkedin.com>
 * 
 */

public enum QueryType {
  SelectionOnly,
  AggregationOnly,
  TimeSeries,
  GroupBy,
  Complex
}
