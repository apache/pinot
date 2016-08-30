package com.linkedin.thirdeye.hadoop.transform;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultTransformUDF implements TransformUDF {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultTransformUDF.class);

  private Schema outputSchema;

  public DefaultTransformUDF() {

  }

  @Override
  public void init(Schema outputSchema) {
    this.outputSchema = outputSchema;
  }

  @Override
  public GenericRecord transformRecord(String sourceName, GenericRecord record) {
    // Default implementation returns input record as is
    return record;
  }

}
