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
package org.apache.pinot.plugin.inputformat.parquet;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.AbstractRecordExtractorTest;
import org.apache.pinot.spi.data.readers.AbstractRecordReaderTest;
import org.apache.pinot.spi.data.readers.RecordExtractor;
import org.apache.pinot.spi.data.readers.RecordReader;

import static org.apache.avro.Schema.create;
import static org.apache.avro.Schema.createArray;
import static org.apache.avro.Schema.createRecord;
import static org.apache.avro.Schema.createUnion;

/**
 * Tests the {@link ParquetRecordExtractor} using a schema containing groovy transform functions
 */
public class ParquetRecordExtractorTest extends AbstractRecordExtractorTest {
  private final File _dataFile = new File(_tempDir, "events.parquet");

  /**
   * Create a ParquetRecordReader
   */
  @Override
  protected RecordReader createRecordReader()
      throws IOException {
    ParquetRecordReader recordReader = new ParquetRecordReader();
    recordReader.init(_dataFile, _pinotSchema, null);
    return recordReader;
  }

  @Override
  protected RecordExtractor createRecordExtractor(Set<String> sourceFields) {
    ParquetRecordExtractor recordExtractor = new ParquetRecordExtractor();
    recordExtractor.init(sourceFields, null);
    return recordExtractor;
  }

  /**
   * Create a Parquet input file using the input records
   */
  @Override
  protected void createInputFile()
      throws IOException {
    Schema avroSchema = createRecord("eventsRecord", null, null, false);
    List<Schema.Field> fields = Arrays
        .asList(new Schema.Field("user_id", createUnion(Lists.newArrayList(create(Schema.Type.INT), create(Schema.Type.NULL))), null, null),
            new Schema.Field("firstName", createUnion(Lists.newArrayList(create(Schema.Type.STRING), create(Schema.Type.NULL))), null, null),
            new Schema.Field("lastName", createUnion(Lists.newArrayList(create(Schema.Type.STRING), create(Schema.Type.NULL))), null, null),
            new Schema.Field("bids", createUnion(Lists.newArrayList(createArray(create(Schema.Type.INT)), create(
                Schema.Type.NULL))), null, null),
            new Schema.Field("campaignInfo", create(Schema.Type.STRING), null, null),
            new Schema.Field("cost", create(Schema.Type.DOUBLE), null, null),
            new Schema.Field("timestamp", create(Schema.Type.LONG), null, null));
    avroSchema.setFields(fields);

    try (ParquetWriter<GenericRecord> writer = ParquetUtils
        .getParquetWriter(new Path(_dataFile.getAbsolutePath()), avroSchema)) {
      for (Map<String, Object> inputRecord : _inputRecords) {
        GenericRecord record = new GenericData.Record(avroSchema);
        for (String columnName : _sourceFieldNames) {
          record.put(columnName, inputRecord.get(columnName));
        }
        writer.write(record);
      }
    }
  }
}
