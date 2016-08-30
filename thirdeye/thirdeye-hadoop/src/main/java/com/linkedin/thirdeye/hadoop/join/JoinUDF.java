package com.linkedin.thirdeye.hadoop.join;

import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public interface JoinUDF {
  /**
   * Initializes by providing the output schema.
   * @param outputSchema
   */
  void init(Schema outputSchema);

  /**
   * @param joinKey common key used to join all the sources
   * @param joinInput Mapping from sourceName to GenericRecord(s)
   * @return
   */
  List<GenericRecord> performJoin(Object joinKeyVal, Map<String, List<GenericRecord>> joinInput);

}
