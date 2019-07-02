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

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.core.data.GenericRow;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.util.AvroUtils;


/**
 * Record reader for AVRO file.
 */
public class AvroRecordReader implements RecordReader {
  private final File _dataFile;
  private final Schema _schema;
  private final List<FieldSpec> _fieldSpecs;

  private DataFileStream<GenericRecord> _avroReader;
  private GenericRecord _reusableAvroRecord = null;

  public AvroRecordReader(File dataFile, Schema schema)
      throws IOException {
    _dataFile = dataFile;
    _schema = schema;
    _fieldSpecs = RecordReaderUtils.extractFieldSpecs(schema);
    _avroReader = AvroUtils.getAvroReader(dataFile);
    try {
      AvroUtils.validateSchema(_schema, _avroReader.getSchema());
    } catch (Exception e) {
      _avroReader.close();
      throw e;
    }
  }

  @Override
  public void init(SegmentGeneratorConfig segmentGeneratorConfig) {

  }

  @Override
  public boolean hasNext() {
    return _avroReader.hasNext();
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
    _reusableAvroRecord = _avroReader.next(_reusableAvroRecord);
    for (FieldSpec fieldSpec : _fieldSpecs) {
      String fieldName = fieldSpec.getName();
      Object value = _reusableAvroRecord.get(fieldName);
      // Allow default value for non-time columns
      //Why special treatment for Time Column?
      if (value != null || fieldSpec.getFieldType() != FieldSpec.FieldType.TIME) {
        AvroUtils.extractField(fieldSpec, _reusableAvroRecord, reuse);
      }
    }
    return reuse;
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
  public void close()
      throws IOException {
    _avroReader.close();
  }
}
