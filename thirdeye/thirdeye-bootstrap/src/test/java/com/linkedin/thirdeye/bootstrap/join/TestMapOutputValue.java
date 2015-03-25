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

public class TestMapOutputValue {

  @Test
  public void testSerDeser() throws Exception {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

    // event 1 schema and data
    InputStream schemaStream1 = classLoader
        .getResourceAsStream("AbookImportContactsEvent.avsc");
    InputStream event1 = classLoader
        .getResourceAsStream("sample_AbookImportContactsEvent.json");

    Schema schema1 = new Schema.Parser().parse(schemaStream1);
    System.out.println(schema1);
    GenericRecord record1 = new GenericData.Record(schema1);
    JsonDecoder jsonDecoder1 = null;
    jsonDecoder1 = DecoderFactory.get().jsonDecoder(schema1, event1);
    GenericDatumReader<GenericRecord> gdr1 = new GenericDatumReader<GenericRecord>(
        schema1);
    gdr1.read(record1, jsonDecoder1);

    // event 2
    InputStream schemaStream2 = classLoader
        .getResourceAsStream("AbookImportImpressionEvent.avsc");

    InputStream event2 = classLoader
        .getResourceAsStream("sample_AbookImportImpressionEvent.json");
    Schema schema2 = new Schema.Parser().parse(schemaStream2);
    System.out.println(schema2);

    GenericRecord record2 = new GenericData.Record(schema2);
    JsonDecoder jsonDecoder2 = null;
    jsonDecoder2 = DecoderFactory.get().jsonDecoder(schema2, event2);
    GenericDatumReader<GenericRecord> gdr2 = new GenericDatumReader<GenericRecord>(
        schema2);
    gdr2.read(record2, jsonDecoder2);


    // construct map output value and serialize
    MapOutputValue valueOrig = new MapOutputValue(schema1.getName(), record1);
    byte[] bytes = valueOrig.toBytes();

    //deserialize
    // create schema map  
    Map<String, Schema> schemaMap = new HashMap<String, Schema>();
    schemaMap.put(schema1.getName(), schema1);
    schemaMap.put(schema2.getName(), schema2);
    MapOutputValue valueNew = MapOutputValue.fromBytes(bytes, schemaMap);
    
    Assert.assertEquals(valueOrig.getSchemaName(), valueNew.getSchemaName());
    Assert.assertEquals(valueOrig.getRecord(), valueNew.getRecord());
    

  }
}
