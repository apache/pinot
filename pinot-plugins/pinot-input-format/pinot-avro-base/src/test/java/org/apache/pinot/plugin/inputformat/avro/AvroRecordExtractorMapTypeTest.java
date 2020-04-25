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
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.pinot.spi.data.readers.AbstractRecordExtractorTest;
import org.apache.pinot.spi.data.readers.RecordReader;

import static org.apache.avro.Schema.*;


/**
 * Tests the {@link AvroRecordExtractor} using a schema containing groovy transform functions for Avro maps
 */
public class AvroRecordExtractorMapTypeTest extends AbstractRecordExtractorTest {

  private final File _dataFile = new File(_tempDir, "maps.avro");

  @Override
  protected List<Map<String, Object>> getInputRecords() {
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
    inputRecords.add(record2);

    return inputRecords;
  }

  @Override
  protected org.apache.pinot.spi.data.Schema getPinotSchema()
      throws IOException {
    InputStream schemaInputStream = AbstractRecordExtractorTest.class.getClassLoader()
        .getResourceAsStream("groovy_map_transform_functions_schema.json");
    return org.apache.pinot.spi.data.Schema.fromInputSteam(schemaInputStream);
  }

  @Override
  protected Set<String> getSourceFields() {
    return Sets.newHashSet("map1", "map2");
  }

  /**
   * Create an AvroRecordReader
   */
  @Override
  protected RecordReader createRecordReader()
      throws IOException {
    AvroRecordReader avroRecordReader = new AvroRecordReader();
    avroRecordReader.init(_dataFile, _pinotSchema, null, _sourceFieldNames);
    return avroRecordReader;
  }

  /**
   * Create an Avro input file using the input records containing maps
   */
  @Override
  protected void createInputFile()
      throws IOException {
    org.apache.avro.Schema avroSchema = createRecord("mapRecord", null, null, false);
    org.apache.avro.Schema intStringMapAvroSchema = createMap(create(Type.STRING));
    org.apache.avro.Schema stringIntMapAvroSchema = createMap(create(Type.INT));
    List<Field> fields = Arrays
        .asList(new Field("map1", intStringMapAvroSchema, null, null), new Field("map2", stringIntMapAvroSchema, null, null));
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
