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
import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;


/**
 * Record reader for AVRO file.
 */
public class AvroRecordReader implements RecordReader<GenericRecord> {
  private File _dataFile;
  private Schema _schema;
  private DataFileStream<GenericRecord> _avroReader;
  private GenericRecord _reusableAvroRecord = null;

  public AvroRecordReader() {
  }

  @Override
  public void init(File dataFile, Schema schema, @Nullable RecordReaderConfig recordReaderConfig)
      throws IOException {
    _dataFile = dataFile;
    _schema = schema;
    _avroReader = AvroUtils.getAvroReader(dataFile);
    try {
      AvroUtils.validateSchema(_schema, _avroReader.getSchema());
    } catch (Exception e) {
      _avroReader.close();
      throw e;
    }
  }

  @Override
  public boolean hasNext() {
    return _avroReader.hasNext();
  }

  @Override
  public GenericRecord next(GenericRecord reuse)
      throws IOException {
    return _avroReader.next(reuse);
  }

  // NOTE: hard to extract common code further
  @SuppressWarnings("Duplicates")
  @Override
  public GenericRecord next()
      throws IOException {
    return next(_reusableAvroRecord);
  }

  @Override
  public void rewind()
      throws IOException {
    _avroReader.close();
    _avroReader = AvroUtils.getAvroReader(_dataFile);
  }

  @Override
  public Schema getSchema() {
    return _schema;
  }

  @Override
  public String getRecordExtractorClassName() {
    return AvroRecordExtractor.class.getName();
  }

  @Override
  public void close()
      throws IOException {
    _avroReader.close();
  }
}
