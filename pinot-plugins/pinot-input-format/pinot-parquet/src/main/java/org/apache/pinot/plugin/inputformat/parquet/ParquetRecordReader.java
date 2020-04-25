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

import java.io.File;
import java.io.IOException;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;


/**
 * Record reader for Parquet file.
 */
public class ParquetRecordReader implements RecordReader {
  private Path _dataFilePath;
  private Schema _schema;
  private ParquetRecordExtractor _recordExtractor;
  private ParquetReader<GenericRecord> _reader;
  private GenericRecord _nextRecord;

  @Override
  public void init(File dataFile, Schema schema, @Nullable RecordReaderConfig recordReaderConfig, Set<String> fields)
      throws IOException {
    _dataFilePath = new Path(dataFile.getAbsolutePath());
    _schema = schema;
    ParquetUtils.validateSchema(_schema, ParquetUtils.getParquetSchema(_dataFilePath));
    _recordExtractor = new ParquetRecordExtractor();
    _recordExtractor.init(fields, null);

    _reader = ParquetUtils.getParquetReader(_dataFilePath);
    _nextRecord = _reader.read();
  }

  @Override
  public boolean hasNext() {
    return _nextRecord != null;
  }

  @Override
  public GenericRow next()
      throws IOException {
    return next(new GenericRow());
  }

  // NOTE: hard to extract common code further
  @SuppressWarnings("Duplicates")
  @Override
  public GenericRow next(GenericRow reuse)
      throws IOException {
   _recordExtractor.extract(_nextRecord, reuse);
    _nextRecord = _reader.read();
    return reuse;
  }

  @Override
  public void rewind()
      throws IOException {
    _reader = ParquetUtils.getParquetReader(_dataFilePath);
    _nextRecord = _reader.read();
  }

  @Override
  public Schema getSchema() {
    return _schema;
  }

  @Override
  public void close()
      throws IOException {
    _reader.close();
  }

}
