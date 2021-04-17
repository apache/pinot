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

import java.io.File;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.AbstractRecordReaderTest;
import org.apache.pinot.spi.data.readers.RecordReader;


public class AvroRecordReaderTest extends AbstractRecordReaderTest {
  private final File _dataFile = new File(_tempDir, "data.avro");

  @Override
  protected RecordReader createRecordReader() throws Exception {
    AvroRecordReader avroRecordReader = new AvroRecordReader();
    avroRecordReader.init(_dataFile, _sourceFields, null);
    return avroRecordReader;
  }

  @Override
  protected void writeRecordsToFile(List<Map<String, Object>> recordsToWrite) throws Exception {
    Schema schema = AvroUtils.getAvroSchemaFromPinotSchema(getPinotSchema());
    final DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
    try (DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<>(writer)) {
      fileWriter.create(schema, _dataFile);
      for (Map<String, Object> r : recordsToWrite) {
        GenericRecord record = new GenericData.Record(schema);
        for (FieldSpec fieldSpec : getPinotSchema().getAllFieldSpecs()) {
          record.put(fieldSpec.getName(), r.get(fieldSpec.getName()));
        }
        fileWriter.append(record);
      }
    }
  }
}
