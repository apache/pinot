package com.linkedin.thirdeye.bootstrap.aggregation;

import java.io.DataInput;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;

/**
 * Describes the Avro schema for keys and value for mapper and reducer
 * 
 * @author kgopalak
 * 
 */
public class AggregateJobSchema {

  public static Schema mapOutputKeySchema(AggregationJobConfig config) {
    Schema schema;
    FieldAssembler<Schema> fields = SchemaBuilder.record("AggregateMapOutputKey")//
        .namespace("org.apache.avro.ipc")//
         .fields();
    //start
    for(String dimension:config.getDimensionNames()){
      fields.name(dimension).type().stringType().stringDefault("");
    }
    schema = fields.endRecord();
    return schema;
  }
  public static Schema mapOutputValueSchema(AggregationJobConfig config) {
    Schema schema;
    FieldAssembler<Schema> fields = SchemaBuilder.record("AggregateMapOutputKey")//
        .namespace("org.apache.avro.ipc")//
         .fields();
    //start
    for(String dimension:config.getDimensionNames()){
      fields.name(dimension).type().stringType().stringDefault("");
    }
    schema = fields.endRecord();
    
    return schema;
  }
}
