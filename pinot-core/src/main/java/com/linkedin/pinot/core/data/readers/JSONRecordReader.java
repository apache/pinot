/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import java.io.FileReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;


public class JSONRecordReader extends BaseRecordReader {
  private final String _dataFile;
  private final Schema _schema;

  JsonParser _parser;
  Iterator<Map> _iterator;

  public JSONRecordReader(String dataFile, Schema schema) {
    super();
    super.initNullCounters(schema);
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
    return next(new GenericRow());
  }

  @Override
  public GenericRow next(GenericRow row) {
    Map<String, Object> record = _iterator.next();

    for (final FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
      String column = fieldSpec.getName();
      Object data = record.get(column);

      Object value;
      if (fieldSpec.isSingleValueField()) {
        String token = (data != null) ? data.toString() : null;
        if (token == null || token.isEmpty()) {
          incrementNullCountFor(fieldSpec.getName());
        }
        value = RecordReaderUtils.convertToDataType(token, fieldSpec);
      } else {
        value = RecordReaderUtils.convertToDataTypeArray((ArrayList) data, fieldSpec);
      }

      row.putField(column, value);
    }

    return row;
  }

  @Override
  public void close() throws Exception {
    _parser.close();
  }
}
