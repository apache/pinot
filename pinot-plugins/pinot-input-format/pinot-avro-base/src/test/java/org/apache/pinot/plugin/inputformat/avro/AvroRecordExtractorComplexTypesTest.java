/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.plugin.inputformat.avro;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.pinot.spi.data.readers.AbstractRecordExtractorTest;
import org.apache.pinot.spi.data.readers.RecordReader;

import static org.apache.avro.Schema.*;


/**
 * Tests the {@link AvroRecordExtractor} for Avro Map and Avro Record
 */
public class AvroRecordExtractorComplexTypesTest extends AbstractRecordExtractorTest {

  private final File _dataFile = new File(_tempDir, "complex.avro");
  Schema avroSchema;
  Schema intStringMapAvroSchema;
  Schema stringIntMapAvroSchema;
  Schema simpleRecordSchema;
  Schema complexRecordSchema;
  Schema complexFieldSchema;
  Schema complexListSchema;

  @Override
  protected List<Map<String, Object>> getInputRecords() {

    // map with int keys
    intStringMapAvroSchema = createMap(create(Type.STRING));

    // map with string keys
    stringIntMapAvroSchema = createMap(create(Type.INT));

    // simple record - contains a string, long and double array
    simpleRecordSchema = createRecord("simpleRecord", null, null, false);
    simpleRecordSchema.setFields(Lists.newArrayList(new Field("simpleField1", create(Type.STRING), null, null),
        new Field("simpleField2", create(Type.LONG), null, null),
        new Field("simpleList", createArray(create(Type.DOUBLE)), null, null)));

    // complex record - contains a string, a complex field (contains int and long)
    complexRecordSchema = createRecord("complexRecord", null, null, false);
    complexFieldSchema = createRecord("complexField", null, null, false);
    complexFieldSchema.setFields(Lists.newArrayList(new Field("field1", create(Type.INT), null, null),
        new Field("field2", create(Type.LONG), null, null)));
    complexRecordSchema.setFields(Lists.newArrayList(new Field("simpleField", create(Type.STRING), null, null),
        new Field("complexField", complexFieldSchema, null, null)));

    // complex list element - each element contains a record of int and long
    complexListSchema = createRecord("complexList", null, null, false);
    complexListSchema.setFields(Lists.newArrayList(new Field("field1", create(Type.INT), null, null),
        new Field("field2", create(Type.LONG), null, null)));

    Field map1Field = new Field("map1", intStringMapAvroSchema, null, null);
    Field map2Field = new Field("map2", stringIntMapAvroSchema, null, null);
    Field simpleRecordField = new Field("simpleRecord", simpleRecordSchema, null, null);
    Field complexRecordField = new Field("complexRecord", complexRecordSchema, null, null);
    Field complexListField = new Field("complexList", createArray(complexListSchema), null, null);

    avroSchema = createRecord("manyComplexTypes", null, null, false);
    avroSchema
        .setFields(Lists.newArrayList(map1Field, map2Field, simpleRecordField, complexRecordField, complexListField));

    List<Map<String, Object>> inputRecords = new ArrayList<>(2);
    inputRecords.add(getRecord1());
    inputRecords.add(getRecord2());
    return inputRecords;
  }

  private Map<String, Object> getRecord1() {
    Map<String, Object> record1 = new HashMap<>();

    Map<Integer, String> map1 = new HashMap<>();
    map1.put(30, "foo");
    map1.put(200, "bar");
    record1.put("map1", map1);

    Map<String, Integer> map2 = new HashMap<>();
    map2.put("k1", 10000);
    map2.put("k2", 20000);
    record1.put("map2", map2);

    GenericRecord simpleRecord = new GenericData.Record(simpleRecordSchema);
    simpleRecord.put("simpleField1", "foo");
    simpleRecord.put("simpleField2", 1588469340000L);
    simpleRecord.put("simpleList", Arrays.asList(1.1, 2.2));
    record1.put("simpleRecord", simpleRecord);

    GenericRecord complexRecord = new GenericData.Record(complexRecordSchema);
    GenericRecord subComplexRecord = new GenericData.Record(complexFieldSchema);
    subComplexRecord.put("field1", 100);
    subComplexRecord.put("field2", 1588469340000L);
    complexRecord.put("simpleField", "foo");
    complexRecord.put("complexField", subComplexRecord);
    record1.put("complexRecord", complexRecord);

    GenericRecord listElem1 = new GenericData.Record(complexListSchema);
    listElem1.put("field1", 20);
    listElem1.put("field2", 2000200020002000L);
    GenericRecord listElem2 = new GenericData.Record(complexListSchema);
    listElem2.put("field1", 280);
    listElem2.put("field2", 8000200020002000L);
    record1.put("complexList", Arrays.asList(listElem1, listElem2));

    return record1;
  }

  private Map<String, Object> getRecord2() {
    Map<String, Object> record2 = new HashMap<>();
    Map<Integer, String> map1 = new HashMap<>();
    map1.put(30, "moo");
    map1.put(200, "baz");
    record2.put("map1", map1);

    Map<String, Integer> map2 = new HashMap<>();
    map2.put("k1", 100);
    map2.put("k2", 200);
    record2.put("map2", map2);

    GenericRecord simpleRecord2 = new GenericData.Record(simpleRecordSchema);
    simpleRecord2.put("simpleField1", "foo");
    simpleRecord2.put("simpleField2", 1588469340000L);
    simpleRecord2.put("simpleList", Arrays.asList(1.1, 2.2));
    record2.put("simpleRecord", simpleRecord2);

    GenericRecord complexRecord2 = new GenericData.Record(complexRecordSchema);
    GenericRecord subComplexRecord2 = new GenericData.Record(complexFieldSchema);
    subComplexRecord2.put("field1", 100);
    subComplexRecord2.put("field2", 1588469340000L);
    complexRecord2.put("simpleField", "foo");
    complexRecord2.put("complexField", subComplexRecord2);
    record2.put("complexRecord", complexRecord2);

    GenericRecord listElem12 = new GenericData.Record(complexListSchema);
    listElem12.put("field1", 20);
    listElem12.put("field2", 2000200020002000L);
    GenericRecord listElem22 = new GenericData.Record(complexListSchema);
    listElem22.put("field1", 280);
    listElem22.put("field2", 8000200020002000L);
    record2.put("complexList", Arrays.asList(listElem12, listElem22));

    return record2;
  }

  @Override
  protected Set<String> getSourceFields() {
    return Sets.newHashSet("map1", "map2", "simpleRecord", "complexRecord", "complexList");
  }

  /**
   * Create an AvroRecordReader
   */
  @Override
  protected RecordReader createRecordReader(Set<String> fieldsToRead) throws IOException {
    AvroRecordReader avroRecordReader = new AvroRecordReader();
    avroRecordReader.init(_dataFile, fieldsToRead, null);
    return avroRecordReader;
  }

  /**
   * Create an Avro input file using the input records containing maps and record
   */
  @Override
  protected void createInputFile() throws IOException {

    try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      fileWriter.create(avroSchema, _dataFile);
      for (Map<String, Object> inputRecord : _inputRecords) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        for (String columnName : _sourceFieldNames) {
          record.put(columnName, inputRecord.get(columnName));
        }
        fileWriter.append(record);
      }
    }
  }
}
