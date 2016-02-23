package com.linkedin.thirdeye.bootstrap;

import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.SchemaBuilder.RecordBuilder;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.GenericData.Record;

public class AvroTestUtil {
  /**
   * Generate a schema with given schema name and has one field of STRING type
   * for each dimension and one field of LONG type for each metric
   * @param schemaName
   * @param dimensions
   * @param metrics
   * @return
   */
  public static Schema createSchemaFor(String schemaName, String[] dimensions, String[] metrics) {
    FieldAssembler<Schema> fields;
    RecordBuilder<Schema> record = SchemaBuilder.record(schemaName);
    fields = record.namespace("com.linkedin.thirdeye.join").fields();
    for (String dimension : dimensions) {
      fields = fields.name(dimension).type().nullable().stringType().noDefault();
    }
    for (String metric : metrics) {
      fields = fields.name(metric).type().nullable().longType().longDefault(0);
    }
    Schema schema = fields.endRecord();
    return schema;
  }

  /**
   * Generates a generic record with random values. Assumes all dimensions are
   * string type and metrics are long
   * @param schema
   * @param dimensions
   * @param metrics
   * @return
   */
  public static Record generateDummyRecord(Schema schema, String[] dimensions, String[] metrics) {
    System.out.println(schema);
    Random r = new Random();
    GenericRecordBuilder genericRecordBuilder = new GenericRecordBuilder(schema);
    for (String dim : dimensions) {
      genericRecordBuilder.set(dim, dim + "-val" + r.nextInt());
    }
    for (String met : metrics) {
      genericRecordBuilder.set(met, r.nextLong());
    }
    return genericRecordBuilder.build();
  }
}
