package com.linkedin.pinot.core.query.aggregation;

/**
 * The level of different combine call.
 * 
 * @author Xiang Fu <xiafu@linkedin.com>
 *
 */
public enum CombineLevel {
  SEGMENT,
  PARTITION,
  INSTANCE,
  ROUTER,
}
