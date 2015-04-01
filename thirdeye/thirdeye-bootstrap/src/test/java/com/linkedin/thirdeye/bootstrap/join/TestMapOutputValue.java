package com.linkedin.thirdeye.bootstrap.join;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.bootstrap.AvroTestUtil;

public class TestMapOutputValue {

  String input1SourceName = "input1";
  String[] input1Dimensions = new String[] { "joinKey", "d1", "d2", "d3" };
  String[] input1Metrics = new String[] { "m1" };

  String input2SourceName = "input2";
  String[] input2Dimensions = new String[] { "joinKey", "d4", "d5", "d6" };
  String[] input2Metrics = new String[] { "m2" };

  @Test
  public void testSerDeser() throws Exception {
    Schema schema1 = AvroTestUtil.createSchemaFor(input1SourceName,
        input1Dimensions, input1Metrics);

    Schema schema2 = AvroTestUtil.createSchemaFor(input1SourceName,
        input2Dimensions, input2Metrics);

    GenericRecord record1 = AvroTestUtil.generateDummyRecord(schema1,
        input1Dimensions, input1Metrics);

    // construct map output value and serialize
    MapOutputValue valueOrig = new MapOutputValue(schema1.getName(), record1);
    byte[] bytes = valueOrig.toBytes();

    // deserialize
    // create schema map
    Map<String, Schema> schemaMap = new HashMap<String, Schema>();
    schemaMap.put(schema1.getName(), schema1);
    schemaMap.put(schema2.getName(), schema2);
    MapOutputValue valueNew = MapOutputValue.fromBytes(bytes, schemaMap);

    Assert.assertEquals(valueOrig.getSchemaName(), valueNew.getSchemaName());
    Assert.assertEquals(valueOrig.getRecord(), valueNew.getRecord());

  }
}
