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
package org.apache.pinot.controller.recommender.data.writer;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;


public class AvroRecordAppender implements Closeable {

  private final DataFileWriter<GenericData.Record> _recordWriter;
  private final org.apache.avro.Schema _avroSchema;

  public AvroRecordAppender(File file, org.apache.avro.Schema avroSchema)
      throws IOException {
    _avroSchema = avroSchema;
    _recordWriter = new DataFileWriter<>(new GenericDatumWriter<GenericData.Record>(_avroSchema));
    _recordWriter.create(_avroSchema, file);
  }

  public void append(Map<String, Object> record)
      throws IOException {
    GenericData.Record nextRecord = new GenericData.Record(_avroSchema);
    record.forEach((column, value) -> {
      nextRecord.put(column, record.get(column));
    });
    _recordWriter.append(nextRecord);
  }

  @Override
  public void close()
      throws IOException {
    _recordWriter.close();
  }
}
