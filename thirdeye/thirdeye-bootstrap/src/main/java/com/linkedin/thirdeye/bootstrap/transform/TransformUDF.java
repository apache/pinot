package com.linkedin.thirdeye.bootstrap.transform;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/**
 *
 * Simple interface to transform a Generic Record
 */
public interface TransformUDF {

  /**
   * Initializes by providing the output schema.
   * @param outputSchema
   */
  void init(Schema outputSchema);

  /**
   *
   * @param record
   * @return
   */
  GenericRecord transformRecord(GenericRecord record);
}
