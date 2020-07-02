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
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.pinot.spi.data.readers.AbstractRecordExtractorTest;
import org.apache.pinot.spi.data.readers.RecordReader;

import static org.apache.avro.Schema.*;


/**
 * Tests the {@link AvroRecordExtractor} using a schema containing groovy transform functions
 */
public class AvroRecordExtractorTest extends AbstractRecordExtractorTest {

  private final File _dataFile = new File(_tempDir, "events.avro");

  /**
   * Create an AvroRecordReader
   */
  @Override
  protected RecordReader createRecordReader()
      throws IOException {
    AvroRecordReader avroRecordReader = new AvroRecordReader();
    avroRecordReader.init(_dataFile, _sourceFieldNames, null);
    return avroRecordReader;
  }

  /**
   * Create an Avro input file using the input records
   */
  @Override
  protected void createInputFile()
      throws IOException {

    Schema avroSchema = createRecord("eventsRecord", null, null, false);
    List<Field> fields = Arrays
        .asList(new Field("user_id", createUnion(Lists.newArrayList(create(Type.INT), create(Type.NULL))), null, null),
            new Field("firstName", createUnion(Lists.newArrayList(create(Type.STRING), create(Type.NULL))), null, null),
            new Field("lastName", createUnion(Lists.newArrayList(create(Type.STRING), create(Type.NULL))), null, null),
            new Field("bids", createUnion(Lists.newArrayList(createArray(create(Type.INT)), create(Type.NULL))), null,
                null), new Field("campaignInfo", create(Type.STRING), null, null),
            new Field("cost", create(Type.DOUBLE), null, null), new Field("timestamp", create(Type.LONG), null, null));

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
