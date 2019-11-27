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
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.core.data.GenericRow;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;


/**
 * Record reader for Thrift file.
 */
public class ThriftRecordReader implements RecordReader {
  private final File _dataFile;
  private final Schema _schema;
  private final List<FieldSpec> _fieldSpecs;
  private final Class<?> _thriftClass;
  private final Map<String, Integer> _fieldIds = new HashMap<>();

  private InputStream _inputStream;
  private TProtocol _tProtocol;
  private boolean _hasNext;

  public ThriftRecordReader(File dataFile, Schema schema, ThriftRecordReaderConfig recordReaderConfig)
      throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException {
    _dataFile = dataFile;
    _schema = schema;
    _fieldSpecs = RecordReaderUtils.extractFieldSpecs(schema);
    _thriftClass = Class.forName(recordReaderConfig.getThriftClass());
    TBase tObject = (TBase) _thriftClass.newInstance();
    int index = 1;
    TFieldIdEnum tFieldIdEnum;
    while ((tFieldIdEnum = tObject.fieldForId(index)) != null) {
      _fieldIds.put(tFieldIdEnum.getFieldName(), index);
      index++;
    }

    init();
  }

  @Override
  public void init(String inputPath, Schema schema, RecordReaderConfig recordReaderConfig)
      throws Exception {

  }

  private void init()
      throws IOException {
    _inputStream = RecordReaderUtils.getBufferedInputStream(_dataFile);
    try {
      _tProtocol = new TBinaryProtocol(new TIOStreamTransport(_inputStream));
      _hasNext = hasMoreToRead();
    } catch (Exception e) {
      _inputStream.close();
      throw e;
    }
  }

  private boolean hasMoreToRead()
      throws IOException {
    _inputStream.mark(1);
    int nextByte = _inputStream.read();
    _inputStream.reset();
    return nextByte != -1;
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
    TBase tObject;
    try {
      tObject = (TBase) _thriftClass.newInstance();
      tObject.read(_tProtocol);
    } catch (Exception e) {
      throw new IOException("Caught exception while reading thrift object", e);
    }
    for (FieldSpec fieldSpec : _fieldSpecs) {
      String fieldName = fieldSpec.getName();
      Object value = null;
      Integer fieldId = _fieldIds.get(fieldName);
      if (fieldId != null) {
        //noinspection unchecked
        value = tObject.getFieldValue(tObject.fieldForId(fieldId));
      }
      // Allow default value for non-time columns
      if (value != null || fieldSpec.getFieldType() != FieldSpec.FieldType.TIME) {
        reuse.putField(fieldName, RecordReaderUtils.convert(fieldSpec, value));
      }
    }
    _hasNext = hasMoreToRead();
    return reuse;
  }

  @Override
  public void rewind()
      throws IOException {
    _inputStream.close();
    init();
  }

  @Override
  public Schema getSchema() {
    return _schema;
  }

  @Override
  public void close()
      throws IOException {
    _inputStream.close();
  }
}
