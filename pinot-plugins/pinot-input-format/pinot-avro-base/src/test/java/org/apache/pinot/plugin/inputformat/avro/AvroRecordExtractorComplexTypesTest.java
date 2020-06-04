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
import java.io.InputStream;
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

  @Override
  protected List<Map<String, Object>> getInputRecords() {

    Schema complexRecord = createRecord("complexRecord", null, null, false);
    complexRecord.setFields(Lists.newArrayList(new Field("field1", create(Type.STRING), null, null),
        new Field("field2", create(Type.LONG), null, null),
        new Field("list", createArray(create(Type.DOUBLE)), null, null)));
    List<Map<String, Object>> inputRecords = new ArrayList<>(2);
    Map<String, Object> record1 = new HashMap<>();
    Map<Integer, String> map1 = new HashMap<>();
    map1.put(30, "foo");
    map1.put(200, "bar");
    Map<String, Integer> map2 = new HashMap<>();
    map2.put("k1", 10000);
    map2.put("k2", 20000);
    record1.put("map1", map1);
    record1.put("map2", map2);
    GenericRecord genericRecord1 = new GenericData.Record(complexRecord);
    genericRecord1.put("field1", "foo");
    genericRecord1.put("field2", 1588469340000L);
    genericRecord1.put("list", Arrays.asList(1.1, 2.2));
    record1.put("complexField", genericRecord1);
    inputRecords.add(record1);

    Map<String, Object> record2 = new HashMap<>();
    map1 = new HashMap<>();
    map1.put(30, "moo");
    map1.put(200, "baz");
    map2 = new HashMap<>();
    map2.put("k1", 100);
    map2.put("k2", 200);
    record2.put("map1", map1);
    record2.put("map2", map2);
    GenericRecord genericRecord2 = new GenericData.Record(complexRecord);
    genericRecord2.put("field1", "bar");
    genericRecord2.put("field2", 1588450000000L);
    genericRecord2.put("list", Arrays.asList(3.3, 4.4));
    record2.put("complexField", genericRecord2);
    inputRecords.add(record2);

    return inputRecords;
  }

  @Override
  protected Set<String> getSourceFields() {
    return Sets.newHashSet("map1", "map2", "complexField");
  }

  /**
   * Create an AvroRecordReader
   */
  @Override
  protected RecordReader createRecordReader(Set<String> fieldsToRead)
      throws IOException {
    AvroRecordReader avroRecordReader = new AvroRecordReader();
    avroRecordReader.init(_dataFile, fieldsToRead, null);
    return avroRecordReader;
  }

  /**
   * Create an Avro input file using the input records containing maps and record
   */
  @Override
  protected void createInputFile()
      throws IOException {
    Schema avroSchema = createRecord("mapRecord", null, null, false);
    Schema intStringMapAvroSchema = createMap(create(Type.STRING));
    Schema stringIntMapAvroSchema = createMap(create(Type.INT));
    Schema complexRecord = createRecord("complexRecord", null, null, false);
    complexRecord.setFields(Lists.newArrayList(new Field("field1", create(Type.STRING), null, null),
        new Field("field2", create(Type.LONG), null, null),
        new Field("list", createArray(create(Type.DOUBLE)), null, null)));
    List<Field> fields = Arrays.asList(new Field("map1", intStringMapAvroSchema, null, null),
        new Field("map2", stringIntMapAvroSchema, null, null), new Field("complexField", complexRecord, null, null));
    avroSchema.setFields(fields);

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
