package org.apache.pinot.query.planner.partitioning;

/**
 * Key Selector provides a partitioning function to encode a specific row into a hash key.
 * This is used for both the
 */
public interface KeySelector<IN, OUT> {

  OUT getKey(IN input);
}
