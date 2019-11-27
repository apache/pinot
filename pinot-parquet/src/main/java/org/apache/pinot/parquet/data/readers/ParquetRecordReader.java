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
package org.apache.pinot.parquet.data.readers;

import java.io.IOException;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.core.data.GenericRow;
import org.apache.pinot.core.data.readers.RecordReader;
import org.apache.pinot.core.data.readers.RecordReaderConfig;
import org.apache.pinot.core.data.readers.RecordReaderUtils;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.util.AvroUtils;


/**
 * Record reader for Parquet file.
 */
public class ParquetRecordReader implements RecordReader {
  private Path _dataFilePath;
  private Schema _schema;
  private List<FieldSpec> _fieldSpecs;
  private ParquetReader<GenericRecord> _reader;
  private GenericRecord _nextRecord;

  @Override
  public void init(String inputPath, Schema schema, RecordReaderConfig recordReaderConfig)
      throws IOException {
    _dataFilePath = new Path(inputPath);
    _schema = schema;
    AvroUtils.validateSchema(_schema, ParquetUtils.getParquetSchema(_dataFilePath));

    _fieldSpecs = RecordReaderUtils.extractFieldSpecs(_schema);
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
    for (FieldSpec fieldSpec : _fieldSpecs) {
      String fieldName = fieldSpec.getName();
      Object value = _nextRecord.get(fieldName);
      // Allow default value for non-time columns
      if (value != null || fieldSpec.getFieldType() != FieldSpec.FieldType.TIME) {
        reuse.putField(fieldName, RecordReaderUtils.convert(fieldSpec, value));
      }
    }
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
