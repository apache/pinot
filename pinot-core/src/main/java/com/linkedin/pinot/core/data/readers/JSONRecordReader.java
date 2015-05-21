/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.data.readers;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;

public class JSONRecordReader implements RecordReader {
  private final String _dataFile;
  private final Schema _schema;

  JsonParser _parser;
  Iterator<Map> _iterator;

  public JSONRecordReader(String dataFile, Schema schema) {
    _dataFile = dataFile;
    _schema = schema;
  }

  @Override
  public void init() throws Exception {
    final Reader reader = new FileReader(_dataFile);
    _parser = new JsonFactory().createJsonParser(reader);
    _iterator = new ObjectMapper().readValues(_parser, Map.class);

  }

  @Override
  public void rewind() throws Exception {
    _parser.close();
    init();
  }

  @Override
  public boolean hasNext() {
    return _iterator.hasNext();
  }

  @Override
  public Schema getSchema() {
    return _schema;
  }

  @Override
  public GenericRow next() {
    Map<String, Object> record = _iterator.next();
    Map<String, Object> fieldMap = new HashMap<String, Object>();

    for (final FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
      String column = fieldSpec.getName();
      Object data = record.get(column);

      Object value=null;
      if (fieldSpec.isSingleValueField()) {
        String token = (data != null) ? data.toString() : null;
        value = RecordReaderUtils.convertToDataType(token, fieldSpec.getDataType());
      } else {
        value = convertToDataTypeArray(data, fieldSpec.getDataType());
      }

      fieldMap.put(column, value);
    }

    GenericRow genericRow = new GenericRow();
    genericRow.init(fieldMap);
    return genericRow;
  }

  @Override
  public void close() throws Exception {
    _parser.close();
  }

  private Object [] convertToDataTypeArray(Object data, DataType dataType) {
    Object [] value;

    if ((data == null)) {
      value = new Object[1];
      value[0] = RecordReaderUtils.getDefaultNullValue(dataType);

    } else {
      ArrayList objs = (ArrayList) data;
      value = new Object[objs.size()];

      for (int i = 0; i < objs.size(); ++i) {
        String token = (objs.get(i) != null) ? objs.get(i).toString() : null;
        value[i] = RecordReaderUtils.convertToDataType(token, dataType);
      }
    }

    return value;
  }
}
