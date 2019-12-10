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
package org.apache.pinot.thrift.data.readers;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.apache.pinot.spi.data.readers.RecordReaderUtils;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;


/**
 * Record reader for Thrift file.
 */
public class ThriftRecordReader implements RecordReader {
  private File _dataFile;
  private Schema _schema;
  private List<FieldSpec> _fieldSpecs;
  private Class<?> _thriftClass;
  private Map<String, Integer> _fieldIds = new HashMap<>();

  private InputStream _inputStream;
  private TProtocol _tProtocol;
  private boolean _hasNext;

  public ThriftRecordReader() {
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
  public void init(File dataFile, Schema schema, @Nullable RecordReaderConfig config)
      throws IOException {
    ThriftRecordReaderConfig recordReaderConfig = (ThriftRecordReaderConfig) config;
    _dataFile = dataFile;
    _schema = schema;
    _fieldSpecs = RecordReaderUtils.extractFieldSpecs(schema);
    TBase tObject;
    try {
      _thriftClass = this.getClass().getClassLoader().loadClass(recordReaderConfig.getThriftClass());
      tObject = (TBase) _thriftClass.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    int index = 1;
    TFieldIdEnum tFieldIdEnum;
    while ((tFieldIdEnum = tObject.fieldForId(index)) != null) {
      _fieldIds.put(tFieldIdEnum.getFieldName(), index);
      index++;
    }

    init();
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
