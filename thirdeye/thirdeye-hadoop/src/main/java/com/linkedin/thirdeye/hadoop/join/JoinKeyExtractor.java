package com.linkedin.thirdeye.hadoop.join;

import org.apache.avro.generic.GenericRecord;

/**
 * Simple interface to extract the joinKey from a Generic Record
 */
public interface JoinKeyExtractor {
  /**
   * @param sourceName name of the source
   * @param record record from which the join Key is extracted. join key value is expected to be a
   *          string.
   * @return
   */
  String extractJoinKey(String sourceName, GenericRecord record);
}
