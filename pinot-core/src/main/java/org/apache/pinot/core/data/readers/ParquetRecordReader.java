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

import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.utils.ParquetUtils;
import org.apache.pinot.core.data.GenericRow;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.util.AvroUtils;

import java.io.File;
import java.io.IOException;


/**
 * Record reader for Parquet file.
 */
public class ParquetRecordReader implements RecordReader {
  private final String _dataFilePath;
  private final Schema _schema;
  private final List<FieldSpec> _fieldSpecs;

  private ParquetReader<GenericRecord> _reader;
  private GenericRecord _next;
  private boolean _hasNext;

  public ParquetRecordReader(File dataFile, Schema schema)
      throws IOException {
    _dataFilePath = dataFile.getAbsolutePath();
    _schema = schema;
    _fieldSpecs = RecordReaderUtils.extractFieldSpecs(schema);

    _reader = ParquetUtils.getParquetReader(_dataFilePath);
    advanceToNext();

    AvroUtils.validateSchema(_schema, ParquetUtils.getParquetSchema(_dataFilePath));
  }

  @Override
  public void init(SegmentGeneratorConfig segmentGeneratorConfig) {

  }

  @Override
  public boolean hasNext() {
    return _hasNext;
  }

  @Override
  public GenericRow next()
      throws IOException {
    return next(new GenericRow());
  }

  @Override
  public GenericRow next(GenericRow reuse)
      throws IOException {
    for (FieldSpec fieldSpec : _fieldSpecs) {
      String fieldName = fieldSpec.getName();
      Object value = _next.get(fieldName);
      // Allow default value for non-time columns
      if (value != null || fieldSpec.getFieldType() != FieldSpec.FieldType.TIME) {
        reuse.putField(fieldName, RecordReaderUtils.convert(fieldSpec, value));
      }
    }
    advanceToNext();
    return reuse;
  }

  @Override
  public void rewind()
      throws IOException {
    _reader = ParquetUtils.getParquetReader(_dataFilePath);
    advanceToNext();
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

  private void advanceToNext() {
    try {
      _next = _reader.read();
      _hasNext = (_next != null);
    } catch (IOException e) {
      throw new RuntimeException("Failed while reading parquet file: " + _dataFilePath, e);
    }
  }
}
