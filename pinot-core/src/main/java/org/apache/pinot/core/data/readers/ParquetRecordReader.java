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
package org.apache.pinot.core.data.readers;

import org.apache.avro.generic.GenericRecord;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.utils.ParquetUtils;
import org.apache.pinot.core.data.GenericRow;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.util.AvroUtils;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;


/**
 * Record reader for Parquet file.
 */
public class ParquetRecordReader implements RecordReader {
  private final String _dataFilePath;
  private final Schema _schema;

  private Iterator<GenericRecord> _reader;

  public ParquetRecordReader(File dataFile, Schema schema)
      throws IOException {
    _dataFilePath = "file://" + dataFile.getAbsolutePath();
    _schema = schema;
    _reader = ParquetUtils.getParquetReader(_dataFilePath);
    AvroUtils.validateSchema(_schema, ParquetUtils.getParquetSchema(_dataFilePath));
  }

  @Override
  public void init(SegmentGeneratorConfig segmentGeneratorConfig) {

  }

  @Override
  public boolean hasNext() {
    return _reader.hasNext();
  }

  @Override
  public GenericRow next()
      throws IOException {
    return next(new GenericRow());
  }

  @Override
  public GenericRow next(GenericRow reuse)
      throws IOException {
    GenericRecord next = _reader.next();
    AvroUtils.fillGenericRow(next, reuse, _schema);
    return reuse;
  }

  @Override
  public void rewind()
      throws IOException {
    _reader = ParquetUtils.getParquetReader(_dataFilePath);
  }

  @Override
  public Schema getSchema() {
    return _schema;
  }

  @Override
  public void close()
      throws IOException {
  }
}
