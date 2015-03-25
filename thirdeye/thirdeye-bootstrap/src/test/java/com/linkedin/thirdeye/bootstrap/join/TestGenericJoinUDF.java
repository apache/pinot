package com.linkedin.thirdeye.bootstrap.join;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.SchemaBuilder.RecordBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.BeforeClass;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

public class TestGenericJoinUDF {
  private static Schema OUTPUT_SCHEMA = null;
  private static Schema INPUT_SCHEMA_1 = null;
  private static Schema INPUT_SCHEMA_2 = null;

  String input1SourceName = "input1";
  String[] input1Dimensions = new String[] { "joinKey", "d1", "d2", "d3" };
  String[] input1Metrics = new String[] { "m1" };

  String input2SourceName = "input2";
  String[] input2Dimensions = new String[] { "joinKey", "d4", "d5", "d6" };
  String[] input2Metrics = new String[] { "m2" };

  String[] outputDimensions = new String[] { "d1", "d2", "d3", "d4", "d5", "d6" };
  String[] outputMetrics = new String[] { "m1", "m2" };

  String outputSourceName = "output";

  @BeforeTest
  public void setup() {
    INPUT_SCHEMA_1 = createSchemaFor(input1SourceName, input1Dimensions,
        input1Metrics);

    INPUT_SCHEMA_2 = createSchemaFor(input1SourceName, input2Dimensions,
        input2Metrics);

    OUTPUT_SCHEMA = createSchemaFor(outputSourceName, outputDimensions,
        outputMetrics);

  }

  private Schema createSchemaFor(String schemaName, String[] dimensions,
      String[] metrics) {
    FieldAssembler<Schema> fields;
    RecordBuilder<Schema> record = SchemaBuilder.record(schemaName);
    fields = record.namespace("com.linkedin.thirdeye.join").fields();
    for (String dimension : dimensions) {
      fields = fields.name(dimension).type().nullable().stringType()
          .noDefault();
    }
    for (String metric : metrics) {
      fields = fields.name(metric).type().nullable().longType().longDefault(0);
    }
    Schema schema = fields.endRecord();
    return schema;
  }

  @Test
  public void testSimpleJoin() throws IOException {
    GenericRecord record1 = populate(INPUT_SCHEMA_1, input1Dimensions,
        input1Metrics);
    GenericRecord record2 = populate(INPUT_SCHEMA_2, input2Dimensions,
        input2Metrics);

    // make them have the same join key
    String joinKeyVal = "1234";
    record1.put("joinKey", joinKeyVal);
    record2.put("joinKey", joinKeyVal);

    Map<String, String> params = new HashMap<String, String>();
    StringBuilder sb = new StringBuilder();
    String delim = "";
    for (String dim : outputDimensions) {
      sb.append(delim).append(dim);
      if (Arrays.binarySearch(input1Dimensions, dim) > -1) {
        params.put(dim + ".sources", input1SourceName);
      } else if (Arrays.binarySearch(input2Dimensions, dim) > -1) {
        params.put(dim + ".sources", input2SourceName);
      } else {
        System.err.println("dim:" + dim + " not present in the input events");
      }
      delim = ",";
    }
    for (String met : outputMetrics) {
      sb.append(delim).append(met);
      if (Arrays.binarySearch(input1Metrics, met) > -1) {
        params.put(met + ".sources", input1SourceName);
      } else if (Arrays.binarySearch(input2Metrics, met) > -1) {
        params.put(met + ".sources", input2SourceName);
      } else {
        System.err.println("met:" + met + " not present in the input events");
      }
      delim = ",";
    }
    params.put("field.names", sb.toString());

    GenericJoinUDF udf = new GenericJoinUDF(params);
    udf.init(OUTPUT_SCHEMA);

    Map<String, List<GenericRecord>> joinInput = new HashMap<String, List<GenericRecord>>();
    joinInput.put(input1SourceName, Lists.newArrayList(record1));
    joinInput.put(input2SourceName, Lists.newArrayList(record2));
    GenericRecord joinOutputRecord = udf.performJoin(joinKeyVal, joinInput);
    System.out.println(joinOutputRecord);

    for (String dim : outputDimensions) {
      sb.append(delim).append(dim);
      if (Arrays.binarySearch(input1Dimensions, dim) > -1) {
        Assert.assertEquals(joinOutputRecord.get(dim), record1.get(dim));
      } else if (Arrays.binarySearch(input2Dimensions, dim) > -1) {
        Assert.assertEquals(joinOutputRecord.get(dim), record2.get(dim));
      }
    }
    for (String met : outputMetrics) {
      if (Arrays.binarySearch(input1Metrics, met) > -1) {
        Assert.assertEquals(joinOutputRecord.get(met), record1.get(met));
      } else if (Arrays.binarySearch(input2Metrics, met) > -1) {
        Assert.assertEquals(joinOutputRecord.get(met), record2.get(met));
      }
    }

  }

  private Record populate(Schema schema, String[] dimensions, String[] metrics) {
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
